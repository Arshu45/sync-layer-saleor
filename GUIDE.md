# PRISM RetailMind Sync Layer: Engineering & Architecture Guide

> **Target Audience:** AI Coding Assistants (Claude, Antigravity, Cursor) and Backend Engineers working on the Odoo 17 CE ➔ Saleor 3.23 integration.
> **Scope:** Architecture principles, design decisions, end-to-end setup runbook, known bottlenecks, edge cases, and troubleshooting playbooks.

---

## 1. System Architecture & High-Level Flow

```
┌────────────────────────────────────────────────────────────────────────────────┐
│ 172.27.133.76 (Host Machine)                                                   │
│                                                                                │
│   ┌───────────────────────────┐                    ┌─────────────────────────┐ │
│   │        Odoo 17 CE         │                    │       Saleor 3.23       │ │
│   │  (Master of Catalog/Stock)│                    │  (Storefront & Holds)   │ │
│   └─────────────┬─────────────┘                    └────────────▲────────────┘ │
│                 │ (1) Enqueue in DB TX                          │ (6) GraphQL  │
│                 ▼                                               │     Writes   │
│   ┌───────────────────────────┐                    ┌────────────┴────────────┐ │
│   │     prism_sync_outbox     │                    │    prism-sync-worker    │ │
│   │   (PostgreSQL Table)      │                    │   (Python daemon)       │ │
│   └─────────────┬─────────────┘                    └────────────▲────────────┘ │
│                 │ (2) Poll (SKIP LOCKED)                        │ (5) Consume  │
│                 ▼                                               │              │
│   ┌───────────────────────────┐                    ┌────────────┴────────────┐ │
│   │     prism-sync-relay      │  (3) Persistent    │        RabbitMQ         │ │
│   │   (Python daemon)         ├───────────────────►│  (Exchange: prism.sync) │ │
│   └───────────────────────────┘      Pub Confirms  │  (Quorum Queues)        │ │
│                                                    └─────────────────────────┘ │
│                                                                 ▲              │
│                                                                 │ (4) Routes   │
│   ┌───────────────────────────┐                                 │     to       │
│   │    prism_sync Database    │◄────────────────────────────────┘     Queues   │
│   │ (variant_map, idempotency)│                                                │
│   └───────────────────────────┘                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### Core Responsibilities
| System | Ownership & Responsibilities | Boundary Rules |
| :--- | :--- | :--- |
| **Odoo 17 CE** | Master product catalog, categories, physical inventory in warehouses/stores, purchase receipts, POS sales, delivery orders. | **NEVER** makes external HTTP/API calls during user operations or database transactions. Writes only to local PostgreSQL `prism_sync_outbox`. |
| **prism-sync-relay** | Background daemon polling `prism_sync_outbox`, validating schemas, and publishing to RabbitMQ. | Polls via `SELECT ... FOR UPDATE SKIP LOCKED`. Only transitions outbox state to `sent` after RabbitMQ Publisher Confirm. |
| **RabbitMQ** | Message broker providing persistence, backpressure, and fault isolation (`prism.sync` exchange). | Quorum queues with `x-delivery-limit: 5`. Poison messages auto-route to `prism.sync.dead` (DLX). |
| **prism-sync-worker**| Consumer applying changes to Saleor via GraphQL mutations and maintaining bookkeeping state. | One thread per queue. Idempotent execution guarded by `processed_message` table. |
| **prism_sync DB** | Integration bookkeeping store: tracks SKU mappings (`variant_map`), absorbed orders, and audit logs. | Read-only connection to Saleor/Odoo tables for stock calculations; never mutates core tables directly. |
| **Saleor 3.23** | Headless storefront, cart reservations, checkout holds, customer accounts, and order intake. | Master of customer checkout experience; physical quantity driven by Odoo. |

---

## 2. Key Design Decisions (The "Why")

### D1: The Transactional Outbox Pattern (Why not direct Webhooks?)
* **The Trap:** Direct HTTP calls from Odoo models during `create()` or `write()` hold database transactions open, freeze user POS checkouts during network lag, and leak phantom updates if the transaction rolls back.
* **The Decision:** Odoo writes to `prism_sync_outbox` in the **exact same PostgreSQL transaction** as the product update. If the transaction rolls back, the outbox record rolls back. If RabbitMQ is down, rows accumulate safely in Postgres until restored.

### D2: The 2-Minute Lookback Window (`watermark - 2 min`)
* **The Trap:** PostgreSQL stamps `write_date` with the **start time** of the transaction (`CURRENT_TIMESTAMP`), not the commit time. A long transaction started at `13:59:00` and committed at `14:01:00` would be skipped forever by a cron running at `14:00:00` with query `WHERE write_date > '14:00:00'`.
* **The Decision:** Every incremental cron runs:
  $$\text{Query: } \texttt{write\_date} \ge \text{watermark} - \mathbf{2\text{ minutes}}$$
  Because the worker uses deterministic idempotency keys (`product:<SKU>:<change_time>`), re-checking 2 minutes of data is completely safe and guarantees zero lost updates.

### D3: Multi-Variant Modeling (Odoo ➔ Saleor)
* In Odoo:
  * `product.template` = Parent entity (Name, Description, Category).
  * `product.product` = Sellable variant (SKU/Default Code, Price Extra, Attributes like Size/Color).
* In Saleor:
  * Every sellable item must belong to a `ProductVariant` under a `Product`.
* **The Decision:**
  * Odoo `product.template` $\rightarrow$ Saleor `Product`.
  * Odoo `product.product` $\rightarrow$ Saleor `ProductVariant`.
  * The worker stores the correlation in `variant_map (sku, saleor_variant_id, saleor_product_id)`.

### D4: Stock Double-Allocation Formula (For Inventory Flow)
* **The Trap:** When an order is paid in Saleor, Saleor holds 1 unit as an allocation. When Odoo receives the order, Odoo reserves 1 unit in the warehouse. If Odoo pushes its free stock right away, that unit is subtracted twice!
* **The Decision:**
  $$Q_{\text{saleor}} = \text{Odoo Free Stock} + A_{\text{absorbed}}$$
  Where $A_{\text{absorbed}}$ is the sum of Saleor allocations for orders already absorbed into Odoo's database.

---

## 3. End-to-End Setup & Runbook

### Prerequisites
* Ubuntu 24.04 LTS (Host: `172.27.133.76`)
* PostgreSQL 15 running locally (Databases: `odoo`, `saleor`, `prism_sync`)
* RabbitMQ 3.12+ installed and enabled
* Odoo 17 CE installed at `/opt/odoo17/` with `custom-addons` configured

---

### Step 1: Initialize Bookkeeping Database (`prism_sync`)
```bash
# 1. Create DB user and database
sudo -u postgres psql -c "CREATE ROLE prism_sync WITH LOGIN PASSWORD 'YOUR_STRONG_PASSWORD';"
sudo -u postgres createdb -O prism_sync prism_sync

# 2. Apply table schemas
sudo -u postgres psql -d prism_sync -f sync-layer/scripts/init_prism_sync_db.sql

# 3. Grant read/update on Odoo outbox table to prism_sync user
sudo -u postgres psql -d odoo -c "GRANT CONNECT ON DATABASE odoo TO prism_sync;"
sudo -u postgres psql -d odoo -c "GRANT USAGE ON SCHEMA public TO prism_sync;"
sudo -u postgres psql -d odoo -c "GRANT SELECT, UPDATE ON prism_sync_outbox TO prism_sync;"
```

---

### Step 2: Configure RabbitMQ
```bash
# 1. Enable management plugin and start service
sudo rabbitmq-plugins enable rabbitmq_management
sudo systemctl restart rabbitmq-server

# 2. Create virtual host and user
sudo rabbitmqctl add_vhost prism
sudo rabbitmqctl add_user prism_sync 'YOUR_RABBIT_PASSWORD'
sudo rabbitmqctl set_permissions -p prism prism_sync '.*' '.*' '.*'

# 3. Import topology (Exchanges, Quorum Queues, DLX)
sudo rabbitmqctl import_definitions sync-layer/scripts/rabbitmq-definitions.json

# 4. Verify bindings
sudo rabbitmqctl list_queues -p prism name type messages consumers
```

---

### Step 3: Install Odoo Custom Addon (`prism_saleor_connector`)
```bash
# 1. Copy addon to custom-addons path
sudo cp -r sync-layer/prism_saleor_connector /opt/odoo17/custom-addons/
sudo chown -R odoo17:odoo17 /opt/odoo17/custom-addons/prism_saleor_connector

# 2. Restart Odoo service
sudo systemctl restart odoo

# 3. Activate in Odoo Web UI:
#   a. Log in as Administrator -> Settings -> Activate Developer Mode.
#   b. Navigate to Apps -> Click "Update Apps List".
#   c. Search for "PRISM Saleor Connector" -> Click "Activate".
```

---

### Step 4: Setup Standalone Python Environment
```bash
# 1. Create dedicated system user & directory
sudo useradd --system --home /opt/prism-sync --shell /usr/sbin/nologin prism-sync
sudo mkdir -p /opt/prism-sync /etc/prism-sync

# 2. Setup Virtualenv & dependencies
sudo python3 -m venv /opt/prism-sync/venv
sudo /opt/prism-sync/venv/bin/pip install --upgrade pip
sudo /opt/prism-sync/venv/bin/pip install pika==1.3.2 psycopg2-binary requests

# 3. Deploy code to /opt/prism-sync/
sudo cp -r sync-layer/prism_sync/* /opt/prism-sync/
sudo chown -R prism-sync:prism-sync /opt/prism-sync

# 4. Configure environment file
sudo cp sync-layer/config/prism-sync.env.example /etc/prism-sync/prism-sync.env
sudo chown root:prism-sync /etc/prism-sync/prism-sync.env
sudo chmod 640 /etc/prism-sync/prism-sync.env
# Edit /etc/prism-sync/prism-sync.env with real passwords and tokens!
```

---

### Step 5: Seed Pre-Existing Catalog SKUs
Since products already exist in both Odoo and Saleor:
```bash
export SALEOR_GRAPHQL="http://127.0.0.1:8000/graphql/"
export SALEOR_APP_TOKEN="YOUR_APP_TOKEN"
export SYNC_DSN="host=127.0.0.1 dbname=prism_sync user=prism_sync password=YOUR_DB_PASSWORD"

/opt/prism-sync/venv/bin/python3 sync-layer/scripts/seed_variant_map.py
```

---

### Step 6: Enable Systemd Daemons
```bash
sudo cp sync-layer/config/prism-sync-relay.service /etc/systemd/system/
sudo cp sync-layer/config/prism-sync-worker.service /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now prism-sync-relay prism-sync-worker

# Verify status
sudo systemctl status prism-sync-relay
sudo systemctl status prism-sync-worker
```

---

## 4. Known Bottlenecks, Edge Cases & Failure Modes

### Bottleneck 1: Database Locks on the Outbox Table
* **Issue:** High write volume on `prism_sync_outbox` causes row lock contention with the relay poller.
* **Mitigation:** The relay **must** use `SELECT ... FOR UPDATE SKIP LOCKED`. This skips rows currently locked by other transactions without waiting. Ensure an index exists on `state` and `id`.

### Bottleneck 2: Saleor GraphQL Rate Limiting & Timeouts
* **Issue:** Saleor limits concurrent GraphQL operations. Pushing 5,000 products simultaneously can cause HTTP `429 Too Many Requests` or query timeouts.
* **Mitigation:** The worker processes messages with a prefetch limit (`basic_qos(prefetch_count=10)`). For bulk updates, mutations should be chunked.

### Bottleneck 3: Poison Messages & Loop Stalls
* **Issue:** A malformed payload (e.g. invalid attribute format) causes the worker handler to crash repeatedly, blocking the queue.
* **Mitigation:** RabbitMQ quorum queues are configured with `x-delivery-limit: 5`. After 5 failed delivery attempts, RabbitMQ automatically routes the message to `prism.sync.dlx` $\rightarrow$ `prism.sync.dead` queue, unblocking the main pipeline.

### Bottleneck 4: Pre-Existing SKUs Without Map
* **Issue:** An Odoo SKU exists in Saleor, but is not present in `variant_map`.
* **Mitigation:** Run `scripts/seed_variant_map.py` before starting the worker. If an unmapped SKU arrives, the worker creates a new Product/Variant in Saleor and adds it to `variant_map`.

### Bottleneck 5: Stale Watermark on Crash
* **Issue:** An Odoo server reboot during cron execution causes the watermark to roll back.
* **Mitigation:** The watermark update in `ir.config_parameter` is committed **in the exact same database transaction** as the outbox `enqueue()` calls. They succeed or fail together atomically.

---

## 5. Troubleshooting & Diagnostics Playbook

### Diagnostic Commands

| Check | Command / Query | Expected Output |
| :--- | :--- | :--- |
| **Outbox Backlog** | `psql -d odoo -c "SELECT state, count(*) FROM prism_sync_outbox GROUP BY state;"` | Most rows `sent`, few `pending`, 0 `failed`. |
| **Outbox Errors** | `psql -d odoo -c "SELECT id, msg_id, last_error FROM prism_sync_outbox WHERE state='failed' LIMIT 5;"` | Inspect failure reasons if any exist. |
| **RabbitMQ Queues** | `sudo rabbitmqctl list_queues -p prism name messages consumers` | `to_saleor.products` messages close to 0; 1 consumer. |
| **Dead-Letter Queue** | `sudo rabbitmqctl list_queues -p prism | grep dead` | Should show `0` messages. Anything > 0 requires human review. |
| **Relay Logs** | `journalctl -u prism-sync-relay -f --no-tail -n 50` | `Published msg_id=... routing_key=odoo.product.created` |
| **Worker Logs** | `journalctl -u prism-sync-worker -f --no-tail -n 50` | `ACK message msg_id=... Successfully synced SKU ...` |
| **Variant Mapping** | `psql -d prism_sync -c "SELECT count(*) FROM variant_map;"` | Matches the total count of mapped SKUs. |

---

### Common Errors & Remediation

#### 1. Relay Error: `psycopg2.errors.UndefinedTable: relation "prism_sync_outbox" does not exist`
* **Root Cause:** The `prism_saleor_connector` module is not installed in Odoo yet.
* **Fix:** Go to Odoo Apps ➔ Update Apps List ➔ Install **PRISM Saleor Connector**.

#### 2. Worker Error: `Saleor GraphQL HTTP 401: Unauthorized`
* **Root Cause:** `SALEOR_APP_TOKEN` in `/etc/prism-sync/prism-sync.env` is invalid or expired.
* **Fix:** In Saleor Dashboard (:9000) ➔ Apps ➔ `prism-sync` ➔ Generate a new token and update the env file, then restart:
  ```bash
  sudo systemctl restart prism-sync-worker
  ```

#### 3. Messages Stuck in `pending` in Outbox
* **Root Cause:** `prism-sync-relay` is stopped or cannot reach RabbitMQ.
* **Fix:** Check `systemctl status prism-sync-relay`. Verify RabbitMQ listener at `127.0.0.1:5672`.

#### 4. Replaying Messages from Dead-Letter Queue
* When a bug in a handler is fixed:
  1. Open RabbitMQ Management UI (`http://127.0.0.1:15672`).
  2. Navigate to Queues ➔ `prism.sync.dead`.
  3. Use the **Shovel plugin** ("Move messages") to route messages back to `to_saleor.products`.
