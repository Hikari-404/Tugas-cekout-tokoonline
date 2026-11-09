import os, time, uuid, logging
import pika, requests
from flask import Flask, request, jsonify
from pydantic import ValidationError
from schemas import CheckoutRequest, OrderEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [A] %(message)s")
log = logging.getLogger("A")
app = Flask(__name__)

B_URL = os.getenv("SERVICE_B_URL", "http://service-b:8001")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
QUEUE, DLX, DLQ = "order.events", "order.dlx", "order.events.dlq"
CONNECT_TIMEOUT, READ_TIMEOUT = 1.0, 2.0     # timeout sinkron (detik)


# ---------- Broker ----------
def publish(event: OrderEvent):
    """Publish persistent + publisher confirm (broker meng-ack penerimaan pesan)."""
    conn = pika.BlockingConnection(pika.ConnectionParameters(
        RABBIT_HOST, heartbeat=30, blocked_connection_timeout=5, socket_timeout=5))
    try:
        ch = conn.channel()
        ch.exchange_declare(DLX, "fanout", durable=True)
        ch.queue_declare(DLQ, durable=True)
        ch.queue_bind(DLQ, DLX)
        ch.queue_declare(QUEUE, durable=True, arguments={"x-dead-letter-exchange": DLX})
        ch.confirm_delivery()
        ch.basic_publish(
            exchange="", routing_key=QUEUE, body=event.model_dump_json().encode(),
            properties=pika.BasicProperties(
                delivery_mode=2, content_type="application/json",
                message_id=event.message_id),
            mandatory=True)   # raise jika tidak ter-confirm
        log.info("PUBLISHED & CONFIRMED message_id=%s", event.message_id)
    finally:
        conn.close()


# ---------- Panggilan sinkron ke B ----------
class UpstreamError(Exception):
    def __init__(self, status, code, detail):
        self.status, self.code, self.detail = status, code, detail

def call_b(path, payload, headers, retries=0):
    for attempt in range(retries + 1):
        try:
            r = requests.post(f"{B_URL}{path}", json=payload, headers=headers,
                              timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if r.status_code >= 500 and attempt < retries:
                continue
            return r
        except requests.exceptions.Timeout:
            log.warning("TIMEOUT %s (attempt %d)", path, attempt + 1)
            if attempt == retries:
                raise UpstreamError(504, "UPSTREAM_TIMEOUT", f"Service B timeout pada {path}")
        except requests.exceptions.ConnectionError:
            log.warning("CONNECTION ERROR %s (attempt %d)", path, attempt + 1)
            if attempt == retries:
                raise UpstreamError(503, "UPSTREAM_UNAVAILABLE", "Service B tidak tersedia")


@app.post("/checkout")
def checkout():
    try:
        req = CheckoutRequest.model_validate(request.get_json(force=True, silent=True) or {})
    except ValidationError as e:
        return jsonify(error="VALIDATION_ERROR", detail=e.errors(include_url=False, include_context=False, include_input=False)), 400

    order_id = str(uuid.uuid4())
    hdr = {"X-Simulate-Delay-Ms": str(req.simulate_delay_ms)}
    try:
        # 1) SINKRON: cek stok (aman di-retry 1x karena read-only)
        r = call_b("/stock/check", {"sku": req.sku, "qty": req.qty}, hdr, retries=1)
        if r.status_code == 409:
            return jsonify(error="OUT_OF_STOCK", order_id=order_id, detail=r.json()), 409
        if r.status_code != 200:
            return jsonify(error="STOCK_CHECK_FAILED", status=r.status_code), 502

        # 2) SINKRON: otorisasi pembayaran (tidak di-retry; idempoten lewat order_id)
        r = call_b("/payments/authorize",
                   {"order_id": order_id, "amount": req.amount, "card_token": req.card_token}, hdr)
        if r.status_code == 402:
            return jsonify(error="PAYMENT_DECLINED", order_id=order_id), 402
        if r.status_code != 200:
            return jsonify(error="PAYMENT_FAILED", status=r.status_code), 502
    except UpstreamError as e:
        return jsonify(error=e.code, order_id=order_id, detail=e.detail), e.status

    # 3) ASINKRON: email, update laporan, info gudang -> lewat broker
    event = OrderEvent(message_id=str(uuid.uuid4()), order_id=order_id, sku=req.sku,
                       qty=req.qty, amount=req.amount, email=req.email, created_at=time.time())
    try:
        publish(event)
    except Exception as e:  # pesanan tetap sah; di produksi gunakan outbox pattern
        log.error("PUBLISH FAILED: %s", e)
        return jsonify(status="CONFIRMED_NOTIFICATION_PENDING", order_id=order_id), 202
    return jsonify(status="CONFIRMED", order_id=order_id, message_id=event.message_id), 201


@app.post("/notify")
def notify():
    """Publish event langsung (dipakai demo: B mati -> pesan menumpuk di broker).
    Body opsional: {"message_id": "..."} -> message_id sama = pesan duplikat."""
    body = request.get_json(silent=True) or {}
    event = OrderEvent(message_id=body.get("message_id", str(uuid.uuid4())),
                       order_id=str(uuid.uuid4()), sku="SKU-001", qty=1, amount=1000,
                       email=body.get("email", "demo@example.com"), created_at=time.time())
    try:
        publish(event)
    except Exception as e:
        return jsonify(error="BROKER_UNAVAILABLE", detail=str(e)), 503
    return jsonify(status="QUEUED", message_id=event.message_id), 202


@app.get("/health")
def health():
    return jsonify(status="ok", service="A")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
