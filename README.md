# PRISM RetailMind Sync Layer

Enterprise-grade, asynchronous integration engine connecting **Odoo 17 CE** (ERP/WMS Master) and **Saleor 3.23** (Headless E-commerce) via the **Transactional Outbox Pattern** and **RabbitMQ**.

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
   • prism-sync-relay  ──► Polls odoo.prism_sync_outbox remotely ➔ publishes to RabbitMQ
   • prism-sync-worker ──► Consumes from RabbitMQ ➔ executes Saleor GraphQL mutations
   • Virtualenv        ──► sync-layer/venv/
   • Logs & PIDs       ──► sync-layer/logs/
```

---

## 2. Product Sync Flow (End-to-End)

```mermaid
flowchart LR
    Odoo["Odoo 17 CE\n(Product Edit)"] -->|Watermark Cron\nlookback -2m| Outbox[("prism_sync_outbox\n(Odoo PostgreSQL)")]
    Outbox -->|Poll 5s\nSKIP LOCKED| Relay["prism-sync-relay\n(This Machine)"]
    Relay -->|Publisher Confirms| RabbitMQ["RabbitMQ Broker\n(Queue: to_saleor.products)"]
    RabbitMQ -->|Consume| Worker["prism-sync-worker\n(This Machine)"]
    Worker <--> SyncDB[("prism_sync DB\n(variant_map & processed_message)")]
    Worker -->|GraphQL Mutations| Saleor["Saleor 3.23\n(:8000/graphql/)"]
```

1. **Trigger in Odoo:**
   * An administrator edits a product title, price, or variant attribute in Odoo.
   * Scheduled cron `PRISM Sync: products` wakes up every 10 minutes (or on demand), queries products changed within `watermark - 2 minutes`, and inserts them into `prism_sync_outbox` in the **same database transaction** (zero network calls inside Odoo).
2. **Outbox Polling (`prism-sync-relay`):**
   * The relay polls `prism_sync_outbox` via `SELECT ... FOR UPDATE SKIP LOCKED`.
   * Validates the envelope and payload against `schemas.py`.
   * Publishes persistent message to RabbitMQ exchange `prism.sync` (`routing_key: odoo.product.updated`).
   * Upon broker ACK, updates outbox row to `state = 'sent'`, `sent_at = NOW()`.
3. **Consumption & Execution (`prism-sync-worker`):**
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
Starts both the Relay and Worker as background processes with output redirected to `logs/`:
```bash
./sync-layer/scripts/start_daemons.sh
```

### Check Status & Logs
Checks if both processes are running by PID and shows the last 5 log lines:
```bash
./sync-layer/scripts/status_daemons.sh
```

### Stop Daemons
Gracefully terminates both daemons (SIGTERM followed by SIGKILL fallback):
```bash
./sync-layer/scripts/stop_daemons.sh
```

### Live Log Streaming
Follow real-time activity of the relay and worker:
```bash
# Stream relay activity (polling and publishing)
tail -f sync-layer/logs/relay.log

# Stream worker activity (consuming and GraphQL mutations)
tail -f sync-layer/logs/worker.log
```

---

## 4. Manual Execution & Utility Scripts

### Running Daemons Directly (Foreground / Debug Mode)
To view terminal logs live without backgrounding:
```bash
# Run relay in foreground
./sync-layer/venv/bin/python3 sync-layer/prism_sync/relay.py

# Run worker in foreground
./sync-layer/venv/bin/python3 sync-layer/prism_sync/worker.py
```

### Seeding Existing Product Variants
Populates `variant_map` table in `prism_sync` database with all pre-existing catalog variants from Saleor:
```bash
./sync-layer/venv/bin/python3 sync-layer/scripts/seed_variant_map.py
```

### Running Automated Test Suite
Executes all unit tests verifying schemas, idempotency guards, and mock publisher/worker pipelines:
```bash
./sync-layer/venv/bin/python3 -m unittest discover -s sync-layer/tests
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
```

> **Note on Password Encoding:** In `RABBIT_URL`, the `@` character in password `cybage@123` must be percent-encoded as `%40` (`cybage%40123`) to prevent URL parsing errors.

---

## 6. Directory Structure

```
sync-layer/
├── prism-sync.env               # Active credentials & connection config (git-ignored)
├── .gitignore                   # Ignores venv, logs, secrets, and pycache
├── README.md                    # Operational guide (this file)
│
├── prism_sync/                  # Core Python Sync Engine
│   ├── schemas.py               # PRISM 8-field envelope & payload validators
│   ├── relay.py                 # Outbox poller ➔ RabbitMQ publisher
│   ├── worker.py                # Multi-threaded RabbitMQ consumer
│   ├── handlers/
│   │   └── products.py          # Product catalog sync & GraphQL mutations
│   └── utils/
│       └── saleor_client.py     # Saleor GraphQL API client
│
├── prism_saleor_connector/      # Odoo 17 CE Custom Addon (Deploy to /opt/odoo17/custom-addons/)
│   ├── __manifest__.py
│   ├── models/
│   │   ├── prism_sync_outbox.py # Outbox database model
│   │   └── product_sync.py      # Watermark collector
│   ├── data/ir_cron.xml         # Scheduled cron actions
│   └── security/                # Access rights
│
├── scripts/
│   ├── start_daemons.sh         # Starts relay & worker in background
│   ├── stop_daemons.sh          # Stops both daemons
│   ├── status_daemons.sh        # Displays process health & recent logs
│   ├── seed_variant_map.py      # Seeds variant_map from Saleor GraphQL
│   ├── init_prism_sync_db.sql   # Bookkeeping DB schema initialization
│   └── rabbitmq-definitions.json # RabbitMQ vhost, queues, and bindings
│
├── config/
│   ├── prism-sync.env.example   # Template configuration
│   ├── prism-sync-relay.service # Systemd service unit (relay)
│   └── prism-sync-worker.service# Systemd service unit (worker)
│
├── logs/                        # Runtime daemon log files & PID tracking
│   ├── relay.log
│   └── worker.log
│
└── tests/
    ├── test_schemas.py          # Schema validation tests
    └── test_relay_worker.py     # Relay & worker logic tests
```

