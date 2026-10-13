"""
Unit tests for relay and worker logic (using mocked connections)
"""

import unittest
from unittest.mock import MagicMock, patch
import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Ensure pika and psycopg2 are mocked if not installed in current environment
for mod in ["pika", "pika.adapters", "pika.adapters.blocking_connection", "psycopg2", "psycopg2.extras"]:
    if mod not in sys.modules:
        try:
            __import__(mod)
        except ImportError:
            sys.modules[mod] = MagicMock()

from prism_sync.relay import RelayService
from prism_sync.handlers.products import ProductHandler
from prism_sync.schemas import build_envelope


class TestRelayLogic(unittest.TestCase):

    def setUp(self):
        self.relay = RelayService()
        self.relay.db_conn = MagicMock()
        self.relay.db_conn.closed = False
        self.relay.rmq_conn = MagicMock()
        self.relay.rmq_conn.is_open = True
        self.relay.rmq_channel = MagicMock()
        self.relay.rmq_channel.is_open = True

    def test_process_batch_successful_publish(self):
        # Mock pending outbox row
        mock_cursor = MagicMock()
        self.relay.db_conn.cursor.return_value = mock_cursor

        sample_envelope = {
            "id": 1,
            "msg_id": "11111111-1111-4111-8111-111111111111",
            "idem_key": "product:TEST-SKU:2026-10-10T12:00:00Z",
            "source": "odoo",
            "flow": "product",
            "action": "created",
            "schema": 1,
            "as_of": "2026-10-10T12:00:00Z",
            "payload": {
                "sku": "TEST-SKU",
                "odoo_product_id": 10,
                "name": "Test Shirt",
                "channels": [{"channel_slug": "online", "price_inr": 299.0}]
            },
            "routing_key": "odoo.product.created"
        }
        mock_cursor.fetchall.return_value = [sample_envelope]

        published = self.relay.process_batch()
        self.assertEqual(published, 1)
        self.relay.rmq_channel.basic_publish.assert_called_once()
        self.relay.db_conn.commit.assert_called_once()

    def test_process_batch_schema_validation_failure(self):
        mock_cursor = MagicMock()
        self.relay.db_conn.cursor.return_value = mock_cursor

        # Invalid envelope (missing sku)
        invalid_envelope = {
            "id": 2,
            "msg_id": "22222222-2222-4222-8222-222222222222",
            "idem_key": "product:INVALID:2026-10-10T12:00:00Z",
            "source": "odoo",
            "flow": "product",
            "action": "created",
            "schema": 1,
            "as_of": "2026-10-10T12:00:00Z",
            "payload": {
                "sku": "",  # Empty SKU
                "odoo_product_id": 20,
                "name": "Bad Product"
            },
            "routing_key": "odoo.product.created"
        }
        mock_cursor.fetchall.return_value = [invalid_envelope]

        published = self.relay.process_batch()
        self.assertEqual(published, 0)
        # Verify it did not publish to RabbitMQ
        self.relay.rmq_channel.basic_publish.assert_not_called()
        # Verify it executed update to state='failed'
        mock_cursor.execute.assert_any_call(
            """
            UPDATE prism_sync_outbox
            SET state = 'failed', last_error = %s, attempts = attempts + 1
            WHERE id = %s;
            """,
            (unittest.mock.ANY, 2)
        )


class TestProductHandler(unittest.TestCase):

    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_saleor = MagicMock()
        self.mock_saleor.get_channel_ids.return_value = {"online": "Channel:1"}
        self.mock_saleor.get_first_product_type_id.return_value = "ProductType:1"
        self.handler = ProductHandler(self.mock_db, self.mock_saleor)

    def test_handle_existing_product_update(self):
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value = mock_cursor

        # 1. Idempotency returns None (not processed yet)
        # 2. variant_map returns (variant_id="Variant:10", product_id="Product:5")
        mock_cursor.fetchone.side_effect = [
            None,
            ("Variant:10", "Product:5")
        ]

        envelope = build_envelope(
            flow="product",
            action="updated",
            idem_key="product:EXISTING-SKU:2026-10-10T12:00:00Z",
            payload={
                "sku": "EXISTING-SKU",
                "odoo_product_id": 50,
                "name": "Updated Name",
                "channels": [{"channel_slug": "online", "price_inr": 850.0}]
            }
        )

        res = self.handler.handle(envelope)
        self.assertTrue(res)
        # Verify GraphQL mutation was executed for update
        self.assertTrue(self.mock_saleor.execute.called)
        self.mock_db.commit.assert_called_once()

    def test_handle_idempotent_duplicate_skips(self):
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value = mock_cursor

        # Idempotency returns existing record ('ok',)
        mock_cursor.fetchone.return_value = ("ok",)

        envelope = build_envelope(
            flow="product",
            action="updated",
            idem_key="product:DUP-SKU:2026-10-10T12:00:00Z",
            payload={
                "sku": "DUP-SKU",
                "odoo_product_id": 99,
                "name": "Dup Product",
                "channels": []
            }
        )

        res = self.handler.handle(envelope)
        self.assertTrue(res)
        # Should NOT call Saleor GraphQL
        self.mock_saleor.execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
