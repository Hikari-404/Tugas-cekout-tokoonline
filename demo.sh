#!/usr/bin/env bash
# Jalankan setelah: docker compose up -d --build
A=http://localhost:8000
J='Content-Type: application/json'
ok='{"sku":"SKU-001","qty":2,"amount":150000,"card_token":"tok_ok","email":"budi@example.com"}'
step(){ echo; echo "=================== $* ==================="; }

step "1. Checkout normal (sinkron OK + event asinkron)"
curl -s -w "\nHTTP %{http_code}\n" -X POST $A/checkout -H "$J" -d "$ok"; sleep 1

step "2. Stok habis -> 409"
curl -s -w "\nHTTP %{http_code}\n" -X POST $A/checkout -H "$J" -d '{"sku":"SKU-HABIS","qty":1,"amount":1000,"card_token":"tok_ok","email":"a@b.co"}'

step "3. Pembayaran ditolak -> 402"
curl -s -w "\nHTTP %{http_code}\n" -X POST $A/checkout -H "$J" -d '{"sku":"SKU-001","qty":1,"amount":1000,"card_token":"tok_declined","email":"a@b.co"}'

step "4. Validasi gagal (serialisasi) -> 400"
curl -s -w "\nHTTP %{http_code}\n" -X POST $A/checkout -H "$J" -d '{"sku":"SKU-001","qty":-1,"amount":"abc","email":"bukan-email"}'

step "5. TIMEOUT: B lambat 5 detik (timeout A = 2 detik) -> 504"
time curl -s -w "\nHTTP %{http_code}\n" -X POST $A/checkout -H "$J" \
  -d '{"sku":"SKU-001","qty":1,"amount":1000,"card_token":"tok_ok","email":"a@b.co","simulate_delay_ms":5000}'

step "6. KEGAGALAN: Layanan B DIMATIKAN"
docker compose stop service-b
echo "-- 6a. Sinkron: checkout gagal cepat & terkontrol (503)"
curl -s -w "\nHTTP %{http_code}\n" -X POST $A/checkout -H "$J" -d "$ok"
echo "-- 6b. Asinkron: 3 pesan tetap diterima broker (202), menumpuk di antrean"
for i in 1 2 3; do curl -s -w " HTTP %{http_code}\n" -X POST $A/notify; done
docker compose exec rabbitmq rabbitmqctl list_queues name messages | grep order

step "7. Layanan B DIHIDUPKAN KEMBALI -> pesan terkirim (ack)"
docker compose start service-b; sleep 8
docker compose logs --tail=20 service-b
docker compose exec rabbitmq rabbitmqctl list_queues name messages | grep order

step "8. IDEMPOTENSI: pesan duplikat (message_id sama dikirim 2x)"
for i in 1 2; do curl -s -w " HTTP %{http_code}\n" -X POST $A/notify -H "$J" -d '{"message_id":"dup-123"}'; done
sleep 2; docker compose logs --tail=8 service-b
