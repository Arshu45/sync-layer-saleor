# Implementation Plan: Odoo ➔ Saleor Product Sync (Milestone 1)

## Goal Description
Build and deploy the **Product Synchronization Pipeline** from **Odoo 17 CE** to **Saleor 3.23**. This is Milestone 1 of the PRISM Sync Layer, focusing strictly on the Product Catalog entity before expanding to stock, orders, and customers.

Since both Odoo and Saleor already have catalog data preloaded, the sync will handle:
1. **Initial Variant Mapping:** Correlating existing Odoo SKUs with existing Saleor Product and Variant IDs in `variant_map`.
2. **Incremental Delta Sync:** Automatically capturing new products or edits in Odoo (titles, descriptions, attributes, multi-channel INR pricing) and upserting them into Saleor via GraphQL without full catalog dumps.
3. **Multi-Variant Architecture:** Mapping Odoo's `product.template` $\rightarrow$ Saleor `Product` and Odoo's `product.product` variants $\rightarrow$ Saleor `ProductVariant` with attributes.
4. **Environment Target:** Native Python `venv` on Ubuntu 24.04 (`/opt/prism-sync/venv`) managed via systemd, and Odoo custom addon placed in `/opt/odoo17/custom-addons`.

---

## Architecture Flow (Milestone 1: Products)

```mermaid
flowchart TD
    subgraph Odoo ["Odoo 17 CE (/opt/odoo17/custom-addons)"]
        Tmpl["product.template\n(Base info & attributes)"]
        Var["product.product\n(SKU, price, barcode)"]
        CronProd["Products Sync Cron\n(Watermark - 2 min)"]
        OutboxTable[("prism_sync_outbox\n(PostgreSQL table)")]

        Tmpl --> CronProd
        Var --> CronProd
        CronProd -->|enqueue() in same DB TX| OutboxTable
    end

    subgraph HostSystem ["Host / Remote Ubuntu Server"]
        Relay["prism-sync-relay\n(/opt/prism-sync/venv)"]
        OutboxTable -->|SELECT FOR UPDATE SKIP LOCKED| Relay
        
        Broker["RabbitMQ (:5672)\nQueue: to_saleor.products"]
        Relay -->|Publisher Confirms| Broker

        Worker["prism-sync-worker (Products Thread)\n(/opt/prism-sync/venv)"]
        Broker -->|Consume| Worker
        
        SyncDB[("prism_sync Database\n- variant_map\n- processed_message")]
        Worker <--> SyncDB
    end

    subgraph Saleor ["Saleor 3.23 Backend"]
        GraphQL["Saleor GraphQL API (:8000)\n- productUpdate / Create\n- productVariantUpdate / Create\n- productChannelListingUpdate"]
        Worker -->|GraphQL Mutations| GraphQL
    end
```

---

## Target Host Configuration

* **Odoo Custom Addons Path:** `/opt/odoo17/custom-addons/prism_saleor_connector`
* **Sync Layer Venv:** `/opt/prism-sync/venv`
* **Configuration / Secrets:** `/etc/prism-sync/prism-sync.env`
* **Shared Schemas:** `/opt/prism-sync/schemas.py`
* **Services:**
  * `prism-sync-relay.service` (Outbox poller $\rightarrow$ RabbitMQ)
  * `prism-sync-worker.service` (RabbitMQ consumer $\rightarrow$ Saleor GraphQL)

---

## Proposed Changes & Implementation Steps

### Step 1: Shared Schemas & Bookkeeping DB Setup

#### [NEW] `/opt/prism-sync/schemas.py`
Defines envelope validation and the **Product Payload**:
```python
# 8-field envelope standard
class Envelope:
    msg_id: str          # UUID v4
    idem_key: str        # e.g., "product:SKU123:2026-10-10T00:30:00Z"
    source: str          # "odoo"
    flow: str            # "product"
    action: str          # "created" | "updated"
    schema: int          # 1
    as_of: str           # ISO-8601 UTC timestamp
    payload: ProductPayload

class ChannelPrice:
    channel_slug: str
    price_inr: float

class ProductPayload:
    sku: str
    odoo_product_id: int
    odoo_template_id: int
    name: str
    description: str
    category_path: str
    banner: str
    attributes: dict[str, str]  # e.g. {"Size": "L", "Color": "Blue"}
    active: bool
    channels: list[ChannelPrice]
```

#### [NEW] `scripts/init_prism_sync_db.sql`
Creates the bookkeeping schema:
```sql
CREATE TABLE processed_message (
    idem_key text PRIMARY KEY,
    flow text NOT NULL,
    status text NOT NULL,
    detail text,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE variant_map (
    sku text PRIMARY KEY,
    saleor_variant_id text NOT NULL,
    saleor_product_id text NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sync_event_log (
    id bigserial PRIMARY KEY,
    flow text,
    idem_key text,
    status text,
    detail text,
    created_at timestamptz NOT NULL DEFAULT now()
);
```

---

### Step 2: Seed Tool for Pre-Existing Products (`seed_variant_map.py`)

Since products were loaded manually in both systems prior to this build:
* Script queries all variants from Saleor GraphQL:
  ```graphql
  query {
    products(first: 100) {
      edges {
        node {
          id
          name
          variants {
            id
            sku
          }
        }
      }
    }
  }
  ```
* Inserts existing mappings into `variant_map (sku, saleor_variant_id, saleor_product_id)`.
* **Benefit:** When Odoo publishes product updates, the worker immediately updates the existing Saleor records instead of attempting to create duplicates.

---

### Step 3: Odoo Connector Addon (`prism_saleor_connector`)

Located in `/opt/odoo17/custom-addons/prism_saleor_connector/`:

```
prism_saleor_connector/
├── __init__.py
├── __manifest__.py
├── models/
│   ├── __init__.py
│   ├── prism_sync_outbox.py
│   └── product_sync.py
├── data/
│   └── ir_cron.xml
└── security/
    └── ir.model.access.csv
```

#### Model: `prism.sync.outbox` (`models/prism_sync_outbox.py`)
* Table `prism_sync_outbox` in Odoo's PostgreSQL database.
* Exposes `enqueue(flow, action, idem_key, payload)` helper called by the product cron.

#### Model & Logic: `product_sync.py`
* **Watermark Logic:**
  * Reads `prism_sync.watermark.products` from `ir.config_parameter`.
  * Computes `lookback_time = watermark - timedelta(minutes=2)`.
  * Queries `product.product` joined with `product.template`:
    ```python
    domain = [
        '|',
        ('write_date', '>=', lookback_time),
        ('product_tmpl_id.write_date', '>=', lookback_time)
    ]
    variants = self.env['product.product'].search(domain)
    ```
* **Payload Generation:**
  * Maps variant attributes (e.g. Size, Color from `product.template.attribute.value`).
  * Computes channel prices based on channel split rules (Online, Outlet, Social).
  * Calls `self.env['prism.sync.outbox'].enqueue(...)`.
* **Advances Watermark:**
  * Saves the current execution timestamp back to `ir.config_parameter` within the same transaction.

#### Cron Declaration (`data/ir_cron.xml`):
* Declares `ir.cron` "PRISM Sync: products" configured to run at specified intervals (or nightly).

---

### Step 4: Standalone Relay Daemon (`prism-sync-relay`)

Located at `/opt/prism-sync/relay.py`, running via systemd unit `prism-sync-relay.service`:

* Connects to Odoo PostgreSQL database (`ODOO_RO_DSN` with SELECT/UPDATE on `prism_sync_outbox`).
* Polls `WHERE state = 'pending'` using `SELECT ... FOR UPDATE SKIP LOCKED`.
* Validates schema via `schemas.py`.
* Publishes to RabbitMQ exchange `prism.sync` with routing key `odoo.product.created` or `odoo.product.updated`.
* Uses **Publisher Confirms** (`pika.confirm_delivery`).
* On confirm: Updates `state = 'sent'`, `sent_at = NOW()`.
* On schema error: Updates `state = 'failed'`, `last_error = <details>`.

---

### Step 5: Standalone Worker Consumer (`prism-sync-worker`)

Located at `/opt/prism-sync/worker.py`, running via systemd unit `prism-sync-worker.service`:

* Consumes queue `to_saleor.products`.
* For each product message:
  1. Checks `variant_map` by SKU.
  2. **If SKU exists in `variant_map`:**
     * Executes GraphQL `productVariantUpdate` / `productUpdate` with updated details and INR prices.
  3. **If SKU is new (not in `variant_map`):**
     * Checks if template exists in Saleor or creates new Saleor Product (`productCreate`).
     * Creates variant (`productVariantCreate`).
     * Updates channel listing prices (`productChannelListingUpdate`).
     * Records new IDs in `variant_map`.
  4. Records `idem_key` in `processed_message`.
  5. Acknowledges message to RabbitMQ.

---

## Verification Plan

### 1. Verification of Seed Tool
* Run `python3 seed_variant_map.py`.
* Query `variant_map` in `prism_sync` database to verify all existing Odoo SKUs are linked to their corresponding Saleor Variant IDs.

### 2. Verification of Odoo Outbox Enqueue
* In Odoo, edit a product name or price.
* Run the scheduled action `PRISM Sync: products` manually via Odoo Settings.
* Check database table:
  ```sql
  SELECT id, msg_id, flow, action, state, routing_key, payload 
  FROM prism_sync_outbox 
  WHERE flow = 'product';
  ```
* Expected result: Row is present with `state = 'pending'`.

### 3. Verification of Relay Publishing
* Start `prism-sync-relay.service`.
* Re-check `prism_sync_outbox`: Row state changes from `pending` to `sent`.
* In RabbitMQ UI (:15672), verify queue `to_saleor.products` received the message.

### 4. Verification of Worker & Saleor GraphQL Update
* Start `prism-sync-worker.service`.
* Verify queue `to_saleor.products` count drops to 0 (consumed and ACKed).
* Query Saleor GraphQL API or view the product in Saleor Dashboard (:9000).
* Verify that the edited name/price from Odoo matches in Saleor.
