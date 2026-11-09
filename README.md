# Studi Kasus Checkout Toko Online — Komunikasi Sinkron & Asinkron

Dua layanan terpisah (Docker Compose) + RabbitMQ.

```
Client ──POST /checkout──▶ [Service A: Order]
                              │ SINKRON (REST, timeout 1s connect / 2s read)
                              ├──▶ [Service B] /stock/check        (retry 1x, read-only)
                              ├──▶ [Service B] /payments/authorize (tanpa retry, idempoten by order_id)
                              │ ASINKRON (RabbitMQ, persistent + publisher confirm)
                              └──▶ queue order.events ──▶ [Service B consumer]
                                        email · update laporan · info gudang (ack manual, idempoten)
                                        pesan invalid ──▶ order.events.dlq
```

## Menjalankan
```bash
docker compose up -d --build
./demo.sh            # skenario demo lengkap
# RabbitMQ UI: http://localhost:15672 (guest/guest)
docker compose logs -f service-a service-b
docker compose down -v
```

## Pemetaan Rubrik
| Poin | Bukti di repo |
|---|---|
| 25 — Dua layanan terpisah berjalan | `service-a/`, `service-b/`, `docker-compose.yml`, `GET /health` kedua layanan |
| 20 — Sinkron (status HTTP, timeout, galat) | `call_b()` di `service-a/app.py`: 200/201, 400, 402, 409, 503 (B mati), 504 (timeout) |
| 20 — Asinkron queue (ack & idempoten) | Publisher confirm + persistent (A); `basic_ack` manual setelah sukses, tabel `processed(message_id)` SQLite, DLQ (B) |
| 15 — Demo kegagalan & analisis | `demo.sh` langkah 5–8 + tabel analisis di bawah |
| 10 — Serialisasi | `schemas.py`: JSON + Pydantic, `schema_version`, validasi di kedua sisi |
| 10 — Kerapian repo & laporan | README ini, struktur folder, commit bertahap |

## Hasil yang Diharapkan Saat Demo
| Skenario | Hasil |
|---|---|
| Checkout normal | `201 CONFIRMED`; log B: EMAIL, REPORT, GUDANG, ACK |
| Stok habis / kartu ditolak / input salah | `409` / `402` / `400` |
| B lambat (5 dtk) | `504 UPSTREAM_TIMEOUT` setelah ±2–4 dtk (retry 1x pada cek stok) |
| B mati (sinkron) | `503 UPSTREAM_UNAVAILABLE` cepat, tidak menggantung |
| B mati (asinkron) | `/notify` tetap `202`; pesan menumpuk di antrean (`messages: 3`) |
| B hidup lagi | Semua pesan diproses + di-ack, antrean kembali 0 |
| Pesan duplikat | Log `DUPLIKAT ... di-skip`, efek tidak ganda |

## Analisis Perbandingan Sinkron vs Asinkron
| Aspek | Sinkron (REST) | Asinkron (Broker) |
|---|---|---|
| Cocok untuk | Cek stok, otorisasi bayar (jawaban dibutuhkan sekarang) | Email, laporan, info gudang (boleh tertunda) |
| Saat B mati | Request gagal (503/504) — ketergantungan langsung | Pesan aman di antrean, diproses saat B pulih |
| Latensi ke user | Menambah waktu respons | Hampir tidak menambah |
| Konsistensi | Kuat/langsung | Eventual |
| Risiko | Cascading failure, perlu timeout & retry | Duplikat/urutan → perlu ack + idempotensi |
| Mitigasi di tugas ini | Timeout, retry terbatas, pemetaan galat | Persistent msg, confirm, ack manual, dedupe `message_id`, DLQ |

**Kesimpulan:** gunakan sinkron hanya untuk langkah yang menentukan keputusan checkout; semua efek samping
dipindah ke asinkron agar kegagalan layanan pendukung tidak menggagalkan transaksi.

## Keterbatasan & Pengembangan
- Stok & otorisasi bayar disimpan in-memory (simulasi); gunakan database sungguhan di produksi.
- Jika publish gagal setelah pembayaran sukses → gunakan **Outbox Pattern**.
- Bisa ditambah gRPC (`.proto`) sebagai alternatif REST.
