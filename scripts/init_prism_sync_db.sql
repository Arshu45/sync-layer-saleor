-- PRISM RetailMind: Setup Bookkeeping Database schema
-- Run inside database: prism_sync as user prism_sync (or postgres)

CREATE TABLE IF NOT EXISTS processed_message (
    idem_key text PRIMARY KEY,
    flow text NOT NULL,
    status text NOT NULL,
    detail text,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_absorbed (
    saleor_order_id text PRIMARY KEY,
    saleor_order_uuid uuid NOT NULL UNIQUE,
    odoo_order_id int NOT NULL,
    absorbed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS variant_map (
    sku text PRIMARY KEY,
    saleor_variant_id text NOT NULL,
    saleor_product_id text NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse_map (
    odoo_warehouse_code text PRIMARY KEY,
    odoo_warehouse_id int NOT NULL,
    saleor_warehouse_id text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS sync_event_log (
    id bigserial PRIMARY KEY,
    flow text,
    direction text,
    idem_key text,
    status text,
    detail text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sync_event_log_created_at ON sync_event_log (created_at);
CREATE INDEX IF NOT EXISTS idx_variant_map_sku ON variant_map (sku);

