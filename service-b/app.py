import os, time, sqlite3, threading, logging
import pika
from flask import Flask, request, jsonify
from pydantic import ValidationError
from schemas import OrderEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [B] %(message)s")
log = logging.getLogger("B")
app = Flask(__name__)

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
QUEUE, DLX, DLQ = "order.events", "order.dlx", "order.events.dlq"
DB_PATH = os.getenv("DB_PATH", "/data/b.db")
STOCK = {"SKU-001": 100, "SKU-002": 5, "SKU-HABIS": 0}
AUTH_DONE = {}   # order_id -> hasil (otorisasi idempoten)
lock = threading.Lock()


# ---------- API sinkron ----------
def maybe_delay():
    d = int(request.headers.get("X-Simulate-Delay-Ms", "0") or 0)
    if d:
        time.sleep(d / 1000)

@app.post("/stock/check")
def stock_check():
    maybe_delay()
    b = request.get_json(force=True)
    avail = STOCK.get(b.get("sku"))
    if avail is None:
        return jsonify(error="SKU_NOT_FOUND"), 404
    if avail < int(b.get("qty", 0)):
        return jsonify(error="INSUFFICIENT_STOCK", available=avail), 409
    return jsonify(ok=True, available=avail)

@app.post("/payments/authorize")
def authorize():
    maybe_delay()
    b = request.get_json(force=True)
    oid = b["order_id"]
    with lock:
        if oid in AUTH_DONE:                       # idempoten
            return jsonify(AUTH_DONE[oid])
        if b.get("card_token") == "tok_declined":
            return jsonify(error="DECLINED"), 402
        AUTH_DONE[oid] = {"ok": True, "auth_code": f"AUTH-{oid[:8]}"}
        return jsonify(AUTH_DONE[oid])

@app.get("/health")
def health():
    return jsonify(status="ok", service="B")


# ---------- Consumer asinkron (ack manual + idempotensi) ----------
def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.execute("CREATE TABLE IF NOT EXISTS processed(message_id TEXT PRIMARY KEY, at REAL)")
    return c

def process(event: OrderEvent):
    log.info("EMAIL   -> %s (order %s)", event.email, event.order_id[:8])
    log.info("REPORT  -> catat penjualan %s x%d Rp%.0f", event.sku, event.qty, event.amount)
    log.info("GUDANG  -> kurangi stok %s x%d", event.sku, event.qty)
    with lock:
        if event.sku in STOCK:
            STOCK[event.sku] = max(0, STOCK[event.sku] - event.qty)

def on_message(ch, method, props, body):
    try:
        event = OrderEvent.model_validate_json(body)
    except ValidationError as e:
        log.error("PESAN INVALID -> DLQ: %s", e.errors(include_url=False)[:1])
        ch.basic_nack(method.delivery_tag, requeue=False)      # ke dead-letter queue
        return
    conn = db()
    try:
        # Idempotensi: message_id sudah diproses -> ack tanpa efek ganda
        if conn.execute("SELECT 1 FROM processed WHERE message_id=?", (event.message_id,)).fetchone():
            log.warning("DUPLIKAT message_id=%s -> di-skip, ack", event.message_id)
            ch.basic_ack(method.delivery_tag)
            return
        process(event)
        conn.execute("INSERT INTO processed VALUES(?,?)", (event.message_id, time.time()))
        conn.commit()
        ch.basic_ack(method.delivery_tag)                      # ack SETELAH sukses
        log.info("ACK message_id=%s", event.message_id)
    except Exception as e:
        log.exception("GAGAL proses, requeue: %s", e)
        ch.basic_nack(method.delivery_tag, requeue=True)
        time.sleep(1)
    finally:
        conn.close()

def consume_forever():
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST, heartbeat=30))
            ch = conn.channel()
            ch.exchange_declare(DLX, "fanout", durable=True)
            ch.queue_declare(DLQ, durable=True)
            ch.queue_bind(DLQ, DLX)
            ch.queue_declare(QUEUE, durable=True, arguments={"x-dead-letter-exchange": DLX})
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(QUEUE, on_message, auto_ack=False)
            log.info("Consumer siap menunggu pesan di '%s'", QUEUE)
            ch.start_consuming()
        except Exception as e:
            log.warning("Consumer reconnect (%s)", e)
            time.sleep(3)


if __name__ == "__main__":
    threading.Thread(target=consume_forever, daemon=True).start()
    app.run(host="0.0.0.0", port=8001, threaded=True)
