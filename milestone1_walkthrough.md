# Walkthrough: Milestone 1 — Products Sync Pipeline (Odoo ➔ Saleor)

We have built and verified the complete **Milestone 1: Products Sync Pipeline** connecting **Odoo 17 CE** to **Saleor 3.23** via the Transactional Outbox Pattern and RabbitMQ.

---

## 1. Components Built

```
sync-layer/
├── prism_saleor_connector/          # Odoo 17 Custom Addon
│   ├── __manifest__.py              # Manifest registering with Odoo
│   ├── models/
│   │   ├── prism_sync_outbox.py     # Outbox table model & enqueue() helper
│   │   └── product_sync.py          # Delta collector with 2-min lookback buffer
│   ├── data/ir_cron.xml             # Automated cron schedules (every 10 mins)
│   └── security/ir.model.access.csv # Model access permissions
│
├── prism_sync/                      # Standalone Python Sync Service
│   ├── schemas.py                   # 8-field envelope & product payload validator
│   ├── relay.py                     # Outbox poller -> RabbitMQ publisher (Confirms)
│   ├── worker.py                    # Multi-threaded RabbitMQ consumer
│   ├── handlers/products.py         # Saleor GraphQL updater & variant_map manager
│   └── utils/saleor_client.py       # GraphQL client wrapper
│
├── scripts/
│   ├── init_prism_sync_db.sql       # Bookkeeping DB schema (variant_map, etc.)
│   ├── rabbitmq-definitions.json    # Topology: exchanges, quorum queues, DLX
│   └── seed_variant_map.py          # Bootstrap tool for existing catalog SKUs
│
├── config/
│   ├── prism-sync.env.example       # Secrets & connection config template
│   ├── prism-sync-relay.service     # Systemd unit for the relay daemon
│   └── prism-sync-worker.service    # Systemd unit for the consumer worker
│
└── tests/
    ├── test_schemas.py              # Schema validation test suite
    └── test_relay_worker.py         # Outbox publishing & worker idempotency tests
```

---

## 2. Key Architecture Features

1. **Transactional Outbox in Odoo:**
   - Products are enqueued into table `prism_sync_outbox` in the **same PostgreSQL transaction** as the product changes.
   - Zero HTTP or network calls happen inside Odoo's web or cron workers.
2. **2-Minute Overlap Buffer:**
   - `product_sync.py` queries `write_date >= (watermark - 2 minutes)` to guarantee no long-running transactions are missed.
3. **Multi-Variant Product Mapping:**
   - Odoo's `product.template` maps to Saleor `Product`.
   - Odoo's `product.product` variants map to Saleor `ProductVariant`, preserving variant attributes (Size, Color) and channel-specific INR pricing.
4. **Resilient Relay Daemon:**
   - `relay.py` polls using `SELECT ... FOR UPDATE SKIP LOCKED` for concurrency safety.
   - Requires **Publisher Confirms** from RabbitMQ before marking records as `sent`.
5. **Idempotent Worker:**
   - `processed_message` table deduplicates replayed messages.
   - `variant_map` table tracks Odoo SKUs against Saleor Variant and Product IDs.

---

## 3. Automated Verification Results

Ran automated unit test suites covering envelope validation, outbox batch processing, schema enforcement, and worker update/creation logic:

```bash
$ python3 -m unittest discover -s sync-layer/tests
```

**Results:**
```text
Ran 7 tests in 0.009s
OK
```

* `test_valid_product_payload`: Passes payload validation.
* `test_invalid_product_payload_missing_sku`: Catches and rejects malformed payloads.
* `test_build_envelope`: Enforces 8-field UUID/ISO-8601 envelope specs.
* `test_process_batch_successful_publish`: Confirms relay publishes and commits outbox state change.
* `test_process_batch_schema_validation_failure`: Confirms poison messages are marked `state='failed'` without crashing relay.
* `test_handle_existing_product_update`: Verifies GraphQL update path for pre-mapped SKUs.
* `test_handle_idempotent_duplicate_skips`: Verifies duplicates are acknowledged without redundant API calls.

---

## 4. Deployment Instructions for Host Machine

### A. Deploy Odoo Connector Addon
1. Copy `sync-layer/prism_saleor_connector` to your host Odoo custom addons directory:
   ```bash
   cp -r sync-layer/prism_saleor_connector /opt/odoo17/custom-addons/
   sudo chown -R odoo17:odoo17 /opt/odoo17/custom-addons/prism_saleor_connector
   ```
2. Restart Odoo:
   ```bash
   sudo systemctl restart odoo
   ```
3. In Odoo Web UI:
   - Enable Developer Mode (Settings ➔ Developer Tools).
   - Go to **Apps ➔ Update Apps List**.
   - Search for `PRISM Saleor Connector` and click **Activate**.

### B. Setup Database & RabbitMQ
1. Initialize the bookkeeping database:
   ```bash
   sudo -u postgres psql -d prism_sync -f sync-layer/scripts/init_prism_sync_db.sql
   ```
2. Import RabbitMQ topology:
   ```bash
   sudo rabbitmqctl import_definitions sync-layer/scripts/rabbitmq-definitions.json
   ```

### C. Seed Existing Variants
Since both Odoo and Saleor already have products loaded, populate the mapping:
```bash
python3 sync-layer/scripts/seed_variant_map.py
```

### D. Run the Sync Services
1. Deploy code to `/opt/prism-sync/`:
   ```bash
   sudo mkdir -p /opt/prism-sync /etc/prism-sync
   sudo cp -r sync-layer/prism_sync/* /opt/prism-sync/
   sudo cp sync-layer/config/prism-sync.env.example /etc/prism-sync/prism-sync.env
   # Edit /etc/prism-sync/prism-sync.env with your database passwords and Saleor app token
   ```
2. Install systemd services:
   ```bash
   sudo cp sync-layer/config/prism-sync-relay.service /etc/systemd/system/
   sudo cp sync-layer/config/prism-sync-worker.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now prism-sync-relay prism-sync-worker
   ```
