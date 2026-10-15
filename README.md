# PRISM RetailMind Sync Layer

Enterprise-grade, asynchronous integration engine connecting **Odoo 17 CE** (ERP/WMS Master) and **Saleor 3.23** (Headless E-commerce) via the **Transactional Outbox Pattern**, **FastAPI Ingress API**, and **RabbitMQ**.

---

## 1. Multi-Machine Topology

```
 [ Machine 2: 172.27.133.76 (Remote Host) ]
   • Odoo 17 CE (:8069) + Custom Addon (prism_saleor_connector)
   • PostgreSQL (:5432) -> Databases: odoo, saleor, prism_sync
   • RabbitMQ Broker (:5672) + Management UI (:15672)
   • Saleor 3.23 GraphQL API (:8000)

                    ▲
                    │ Remote Network Access
                    ▼

 [ Machine 1: 172.27.133.26 (Sync Engine - This Machine) ]
   • prism-sync-ingress (:8095) ──► Single entrance to RabbitMQ; confirms & disk-spools
   • prism-sync-relay           ──► Polls odoo.prism_sync_outbox remotely ➔ POSTs to Ingress
   • prism-sync-worker          ──► Consumes from RabbitMQ ➔ executes Saleor GraphQL mutations
   • Virtualenv                 ──► sync-layer/venv/
   • Logs & PIDs                ──► sync-layer/logs/
```

---

## 2. Product Sync Flow (End-to-End)

```mermaid
flowchart LR
    Odoo["Odoo 17 CE<br/>(Product Edit)"] -->|Watermark Cron<br/>lookback -2m| Outbox[("prism_sync_outbox<br/>(Odoo PostgreSQL)")]
    Outbox -->|Poll 5s<br/>SKIP LOCKED| Relay["prism-sync-relay<br/>(This Machine)"]
    Relay -->|HTTP POST /ingest/odoo/product<br/>X-API-Key| Ingress["prism-sync-ingress<br/>(FastAPI :8095)"]
    Ingress -->|Publisher Confirms| RabbitMQ["RabbitMQ Broker<br/>(Queue: to_saleor.products)"]
    RabbitMQ -->|Consume| Worker["prism-sync-worker<br/>(This Machine)"]
    Worker <--> SyncDB[("prism_sync DB<br/>(variant_map & processed_message)")]
    Worker -->|GraphQL Mutations| Saleor["Saleor 3.23<br/>(:8000/graphql/)"]
```

1. **Trigger in Odoo:**
   * An administrator edits a product title, price, or variant attribute in Odoo.
   * Scheduled cron `PRISM Sync: products` wakes up every 10 minutes (or on demand), queries products changed within `watermark - 2 minutes`, and inserts them into `prism_sync_outbox` in the **same database transaction** (zero network calls inside Odoo).
2. **Outbox Polling & Ingress Push (`prism-sync-relay`):**
   * The relay polls `prism_sync_outbox` via `SELECT ... FOR UPDATE SKIP LOCKED`.
   * Pre-validates the envelope and payload against `schemas.py`.
   * Sends an HTTP POST to `http://127.0.0.1:8095/ingest/odoo/product` with `X-API-Key`.
   * Upon HTTP 200 from Ingress, updates outbox row to `state = 'sent'`, `sent_at = NOW()`.
   * If Ingress returns 422, marks `state = 'failed'` and stores the exact validation error.
   * If Ingress is unreachable or returns 503, leaves row `pending` to retry next poll.
3. **Ingress Validation & RabbitMQ Delivery (`prism-sync-ingress`):**
   * Validates API key and payload schema (supporting `odoo_template_id`).
   * Publishes message with AMQP Publisher Confirms to `prism.sync` exchange (`odoo.product.*`).
   * Routes message into RabbitMQ queue `to_saleor.products`.
4. **Consumption & Execution (`prism-sync-worker`):**
   * Worker consumes message from `to_saleor.products`.
   * Deduplicates using `processed_message` table in `prism_sync` database.
   * Looks up SKU in `variant_map`:
     * **If SKU exists:** Calls Saleor GraphQL `productUpdate` and `productVariantChannelListingUpdate`.
     * **If SKU is new:** Calls `productCreate`, `productVariantCreate`, channel listings, and updates `variant_map`.
   * Acknowledges message to RabbitMQ.

---

## 3. Daemon Commands & Operations

Management scripts are located in `sync-layer/scripts/`:

### Start Daemons
Starts Ingress API, Relay, and Worker as background processes with output redirected to `logs/`:
```bash
./sync-layer/scripts/start_daemons.sh
```

### Check Status & Logs
Checks if all three processes are running by PID, performs an Ingress `/healthz` check, and shows the last 5 log lines:
```bash
./sync-layer/scripts/status_daemons.sh
```

### Stop Daemons
Gracefully terminates all three daemons (SIGTERM followed by SIGKILL fallback):
```bash
./sync-layer/scripts/stop_daemons.sh
```

### Continuous Foreground Supervision
To start and monitor all daemons together with automatic cleanup on Ctrl+C:
```bash
./sync-layer/scripts/run_all.sh
```

### Live Log Streaming
Follow real-time activity:
```bash
# Stream Ingress API requests and RabbitMQ publish confirms
tail -f sync-layer/logs/ingress.log

# Stream relay activity (polling Odoo outbox and posting to Ingress)
tail -f sync-layer/logs/relay.log

# Stream worker activity (consuming from RabbitMQ and Saleor GraphQL mutations)
tail -f sync-layer/logs/worker.log
```

---

## 4. Ingress API Health & Manual Verification

### Ingress Healthcheck
```bash
curl -s http://127.0.0.1:8095/healthz
# Expected output: {"status":"ok","rabbitmq":true,"spool_pending":0}
```

### Test Product Sync Through Ingress
```bash
curl -X POST http://127.0.0.1:8095/ingest/odoo/product \
  -H "Content-Type: application/json" \
  -H "X-API-Key: prism_secret_2026" \
  -d '{
    "msg_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "idem_key": "product:MANUAL-TEST-SKU:2026-10-15T12:00:00Z",
    "source": "odoo",
    "flow": "product",
    "action": "created",
    "schema": 1,
    "as_of": "2026-10-15T12:00:00Z",
    "routing_key": "odoo.product.created",
    "payload": {
      "sku": "MANUAL-TEST-SKU",
      "odoo_product_id": 9999,
      "odoo_template_id": 8888,
      "name": "Manual Ingress Test",
      "description": "Testing Ingress hop",
      "category_path": "All / Test",
      "banner": "prism",
      "attributes": {},
      "active": true,
      "channels": [{"channel_slug": "online", "price_inr": 299.00}]
    }
  }'
```

---

## 5. Configuration Reference (`prism-sync.env`)

The active configuration file is located at `sync-layer/prism-sync.env`:

```ini
# RabbitMQ AMQP Broker on 172.27.133.76
RABBIT_URL=amqp://prism_sync:cybage%40123@172.27.133.76:5672/prism
RABBIT_EXCHANGE=prism.sync

# Remote PostgreSQL Connections on 172.27.133.76
ODOO_RO_DSN=host=172.27.133.76 port=5432 dbname=odoo user=prism_sync password=cybage@123
ODOO_DB_DSN=host=172.27.133.76 port=5432 dbname=odoo user=prism_sync password=cybage@123
SYNC_DSN=host=172.27.133.76 port=5432 dbname=prism_sync user=prism_sync password=cybage@123
SALEOR_RO_DSN=host=172.27.133.76 port=5432 dbname=saleor user=prism_sync password=cybage@123

# Saleor 3.23 GraphQL API
SALEOR_GRAPHQL=http://172.27.133.76:8000/graphql/
SALEOR_APP_TOKEN=UNThVvs73naqLzx7kMU4xYynShDObZ

# Relay Tuning
RELAY_POLL_INTERVAL=5.0
RELAY_BATCH_SIZE=100

# Ingress API Service (local daemon on machine 1)
INGRESS_URL=http://127.0.0.1:8095
INGEST_API_KEY=prism_secret_2026
```

> **Note on Password Encoding:** In `RABBIT_URL`, the `@` character in password `cybage@123` must be percent-encoded as `%40` (`cybage%40123`) to prevent URL parsing errors.
