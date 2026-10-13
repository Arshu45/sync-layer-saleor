"""
PRISM RetailMind: Product Handler (to_saleor.products consumer)
Applies Odoo product catalog changes to Saleor 3.23 via GraphQL.
"""

import logging
from typing import Any, Dict
import psycopg2

from prism_sync.utils.saleor_client import SaleorClient

logger = logging.getLogger("handler.products")

MUTATION_VARIANT_CHANNEL_UPDATE = """
mutation UpdateVariantChannels($id: ID!, $input: [ProductVariantChannelListingAddInput!]!) {
  productVariantChannelListingUpdate(id: $id, input: $input) {
    errors {
      field
      message
    }
  }
}
"""

MUTATION_PRODUCT_UPDATE = """
mutation UpdateProduct($id: ID!, $input: ProductInput!) {
  productUpdate(id: $id, input: $input) {
    product {
      id
    }
    errors {
      field
      message
    }
  }
}
"""

MUTATION_PRODUCT_CREATE = """
mutation CreateProduct($input: ProductCreateInput!) {
  productCreate(input: $input) {
    product {
      id
    }
    errors {
      field
      message
    }
  }
}
"""

MUTATION_VARIANT_CREATE = """
mutation CreateVariant($input: ProductVariantCreateInput!) {
  productVariantCreate(input: $input) {
    productVariant {
      id
    }
    errors {
      field
      message
    }
  }
}
"""

MUTATION_PRODUCT_CHANNEL_UPDATE = """
mutation UpdateProductChannels($id: ID!, $input: ProductChannelListingUpdateInput!) {
  productChannelListingUpdate(id: $id, input: $input) {
    product {
      id
    }
    errors {
      field
      message
    }
  }
}
"""


class ProductHandler:
    def __init__(self, sync_db_conn, saleor_client: SaleorClient):
        self.db = sync_db_conn
        self.saleor = saleor_client
        self._channel_cache: Dict[str, str] = {}
        self._default_product_type_id: str = ""

    def _get_channel_map(self) -> Dict[str, str]:
        if not self._channel_cache:
            self._channel_cache = self.saleor.get_channel_ids()
        return self._channel_cache

    def _get_default_product_type(self) -> str:
        if not self._default_product_type_id:
            pt_id = self.saleor.get_first_product_type_id()
            if not pt_id:
                raise RuntimeError("No ProductType found in Saleor to create products under.")
            self._default_product_type_id = pt_id
        return self._default_product_type_id

    def handle(self, envelope: Dict[str, Any]) -> bool:
        idem_key = envelope["idem_key"]
        payload = envelope["payload"]
        sku = payload["sku"]
        name = payload["name"]
        description = payload.get("description", "")
        channels_data = payload.get("channels", [])

        cur = self.db.cursor()

        # 1. Idempotency Check
        cur.execute("SELECT status FROM processed_message WHERE idem_key = %s;", (idem_key,))
        existing = cur.fetchone()
        if existing:
            logger.info("Message %s already processed (status=%s). Skipping.", idem_key, existing[0])
            cur.close()
            return True

        # 2. Check variant_map
        cur.execute("SELECT saleor_variant_id, saleor_product_id FROM variant_map WHERE sku = %s;", (sku,))
        map_row = cur.fetchone()

        channel_map = self._get_channel_map()

        if map_row:
            # SKU already mapped in Saleor -> Update
            variant_id, product_id = map_row[0], map_row[1]
            logger.info("SKU %s exists in variant_map (variant_id=%s). Updating in Saleor...", sku, variant_id)

            # Update Product details
            if name or description:
                self.saleor.execute(
                    MUTATION_PRODUCT_UPDATE,
                    {"id": product_id, "input": {"name": name, "description": description}}
                )

            # Update Variant pricing per channel
            variant_channel_inputs = []
            for ch in channels_data:
                ch_slug = ch["channel_slug"]
                if ch_slug in channel_map:
                    variant_channel_inputs.append({
                        "channelId": channel_map[ch_slug],
                        "price": ch["price_inr"]
                    })

            if variant_channel_inputs:
                self.saleor.execute(
                    MUTATION_VARIANT_CHANNEL_UPDATE,
                    {"id": variant_id, "input": variant_channel_inputs}
                )

        else:
            # SKU not yet in Saleor -> Create
            logger.info("SKU %s not found in variant_map. Creating new product in Saleor...", sku)
            pt_id = self._get_default_product_type()

            # 1. Create Product
            prod_res = self.saleor.execute(
                MUTATION_PRODUCT_CREATE,
                {"input": {"name": name, "description": description, "productType": pt_id}}
            )
            created_prod = prod_res.get("productCreate", {}).get("product", {})
            product_id = created_prod.get("id")
            if not product_id:
                errors = prod_res.get("productCreate", {}).get("errors", [])
                raise RuntimeError(f"Failed to create product for SKU {sku}: {errors}")

            # 2. Enable Product in Channels
            update_channels_input = []
            for ch in channels_data:
                ch_slug = ch["channel_slug"]
                if ch_slug in channel_map:
                    update_channels_input.append({
                        "channelId": channel_map[ch_slug],
                        "isPublished": True,
                        "visibleInListings": True
                    })
            if update_channels_input:
                self.saleor.execute(
                    MUTATION_PRODUCT_CHANNEL_UPDATE,
                    {"id": product_id, "input": {"updateChannels": update_channels_input}}
                )

            # 3. Create Variant
            var_res = self.saleor.execute(
                MUTATION_VARIANT_CREATE,
                {"input": {"product": product_id, "sku": sku, "attributes": []}}
            )
            created_var = var_res.get("productVariantCreate", {}).get("productVariant", {})
            variant_id = created_var.get("id")
            if not variant_id:
                errors = var_res.get("productVariantCreate", {}).get("errors", [])
                raise RuntimeError(f"Failed to create variant for SKU {sku}: {errors}")

            # 4. Set Variant channel prices
            var_channel_inputs = []
            for ch in channels_data:
                ch_slug = ch["channel_slug"]
                if ch_slug in channel_map:
                    var_channel_inputs.append({
                        "channelId": channel_map[ch_slug],
                        "price": ch["price_inr"]
                    })
            if var_channel_inputs:
                self.saleor.execute(
                    MUTATION_VARIANT_CHANNEL_UPDATE,
                    {"id": variant_id, "input": var_channel_inputs}
                )

            # 5. Insert into variant_map
            cur.execute(
                """
                INSERT INTO variant_map (sku, saleor_variant_id, saleor_product_id, refreshed_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (sku) DO UPDATE SET
                    saleor_variant_id = EXCLUDED.saleor_variant_id,
                    saleor_product_id = EXCLUDED.saleor_product_id,
                    refreshed_at = NOW();
                """,
                (sku, variant_id, product_id)
            )

        # 3. Log into processed_message
        cur.execute(
            """
            INSERT INTO processed_message (idem_key, flow, status, detail, processed_at)
            VALUES (%s, 'product', 'ok', %s, NOW())
            ON CONFLICT (idem_key) DO NOTHING;
            """,
            (idem_key, f"Handled SKU {sku}")
        )
        self.db.commit()
        cur.close()
        logger.info("Successfully synced SKU %s to Saleor.", sku)
        return True

