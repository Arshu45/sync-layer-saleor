#!/usr/bin/env python3
"""
PRISM RetailMind: Seed Variant Map from Saleor
Scans all existing products and variants in Saleor via GraphQL
and populates the variant_map table in the prism_sync bookkeeping database.
"""

import os
import sys
import logging
import requests
import psycopg2
from psycopg2.extras import execute_batch

from pathlib import Path

package_root = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv
    for env_candidate in [
        os.getenv("PRISM_ENV_FILE"),
        package_root / "prism-sync.env",
        Path("/etc/prism-sync/prism-sync.env"),
        Path.cwd() / "prism-sync.env",
    ]:
        if env_candidate and Path(env_candidate).is_file():
            load_dotenv(env_candidate)
            break
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_variant_map")

SALEOR_GRAPHQL = os.getenv("SALEOR_GRAPHQL", "http://127.0.0.1:8000/graphql/")
SALEOR_APP_TOKEN = os.getenv("SALEOR_APP_TOKEN", "")
SYNC_DSN = os.getenv("SYNC_DSN", "host=127.0.0.1 dbname=prism_sync user=prism_sync password=prism_sync")

QUERY_PRODUCTS = """
query GetProducts($after: String) {
  products(first: 100, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
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
"""


def fetch_saleor_variants():
    headers = {"Content-Type": "application/json"}
    if SALEOR_APP_TOKEN:
        headers["Authorization"] = f"Bearer {SALEOR_APP_TOKEN}"

    has_next = True
    cursor = None
    records = []

    logger.info("Connecting to Saleor GraphQL at %s ...", SALEOR_GRAPHQL)

    while has_next:
        variables = {"after": cursor}
        resp = requests.post(
            SALEOR_GRAPHQL,
            json={"query": QUERY_PRODUCTS, "variables": variables},
            headers=headers,
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"GraphQL request failed HTTP {resp.status_code}: {resp.text}")

        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL returned errors: {data['errors']}")

        products_data = data.get("data", {}).get("products", {})
        edges = products_data.get("edges", [])
        page_info = products_data.get("pageInfo", {})

        for edge in edges:
            prod_node = edge.get("node", {})
            prod_id = prod_node.get("id")
            for var in prod_node.get("variants", []):
                sku = (var.get("sku") or "").strip()
                var_id = var.get("id")
                if sku and var_id and prod_id:
                    records.append((sku, var_id, prod_id))

        has_next = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")

    logger.info("Discovered %d variants with SKUs in Saleor.", len(records))
    return records


def save_to_variant_map(records):
    if not records:
        logger.warning("No records to insert into variant_map.")
        return

    logger.info("Connecting to prism_sync DB ...")
    conn = psycopg2.connect(SYNC_DSN)
    cur = conn.cursor()

    upsert_sql = """
        INSERT INTO variant_map (sku, saleor_variant_id, saleor_product_id, refreshed_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (sku) DO UPDATE SET
            saleor_variant_id = EXCLUDED.saleor_variant_id,
            saleor_product_id = EXCLUDED.saleor_product_id,
            refreshed_at = NOW();
    """

    execute_batch(cur, upsert_sql, records, page_size=200)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Successfully seeded %d variants into variant_map table.", len(records))


def main():
    try:
        records = fetch_saleor_variants()
        save_to_variant_map(records)
    except Exception as exc:
        logger.error("Failed to seed variant map: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

