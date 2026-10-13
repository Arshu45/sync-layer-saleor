"""
Tests for prism_sync.schemas validation
"""

import unittest
from datetime import datetime
import uuid
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from prism_sync.schemas import (
    validate_envelope,
    validate_product_payload,
    validate_message,
    build_envelope,
    ValidationError
)


class TestSchemas(unittest.TestCase):

    def test_valid_product_payload(self):
        payload = {
            "sku": "TSHIRT-BLK-L",
            "odoo_product_id": 101,
            "name": "Classic T-Shirt Black L",
            "description": "100% cotton crew neck",
            "category_path": "Apparel / Men / Tops",
            "banner": "prism-style",
            "attributes": {"Size": "L", "Color": "Black"},
            "active": True,
            "channels": [
                {"channel_slug": "online", "price_inr": 899.0},
                {"channel_slug": "outlet", "price_inr": 699.0}
            ]
        }
        validated = validate_product_payload(payload)
        self.assertEqual(validated["sku"], "TSHIRT-BLK-L")
        self.assertEqual(len(validated["channels"]), 2)
        self.assertEqual(validated["attributes"]["Size"], "L")

    def test_invalid_product_payload_missing_sku(self):
        payload = {
            "sku": "",
            "odoo_product_id": 101,
            "name": "Invalid SKU Item"
        }
        with self.assertRaises(ValidationError) as ctx:
            validate_product_payload(payload)
        self.assertIn("missing non-empty string 'sku'", str(ctx.exception))

    def test_build_envelope(self):
        payload = {
            "sku": "TSHIRT-BLK-L",
            "odoo_product_id": 101,
            "name": "Classic T-Shirt",
            "attributes": {},
            "channels": [{"channel_slug": "online", "price_inr": 500}]
        }
        msg = build_envelope(
            flow="product",
            action="created",
            idem_key="product:TSHIRT-BLK-L:2026-10-10T12:00:00Z",
            payload=payload
        )
        self.assertEqual(msg["source"], "odoo")
        self.assertEqual(msg["flow"], "product")
        self.assertEqual(msg["action"], "created")
        self.assertEqual(msg["schema"], 1)
        self.assertTrue(uuid.UUID(msg["msg_id"]))


if __name__ == "__main__":
    unittest.main()

