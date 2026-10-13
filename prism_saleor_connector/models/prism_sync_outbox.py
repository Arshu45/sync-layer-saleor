import logging
import uuid
from datetime import datetime, timedelta
from odoo import api, fields, models

_logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1


class PrismSyncOutbox(models.Model):
    _name = "prism.sync.outbox"
    _description = "PRISM Sync Outbox Table"
    _order = "id asc"

    msg_id = fields.Char(string="Message ID", required=True, index=True)
    idem_key = fields.Char(string="Idempotency Key", required=True, index=True)
    source = fields.Char(string="Source System", required=True, default="odoo")
    flow = fields.Selection(
        [
            ("product", "Product"),
            ("stock", "Stock"),
            ("customer", "Customer"),
            ("order", "Order"),
        ],
        string="Flow",
        required=True,
        index=True,
    )
    action = fields.Char(string="Action", required=True)
    schema = fields.Integer(string="Schema Version", required=True, default=SCHEMA_VERSION)
    as_of = fields.Datetime(string="As Of UTC", required=True, default=fields.Datetime.now)
    payload = fields.Json(string="PRISM JSON Payload", required=True)
    routing_key = fields.Char(string="Routing Key", required=True, index=True)
    state = fields.Selection(
        [
            ("pending", "Pending"),
            ("sent", "Sent"),
            ("failed", "Failed"),
        ],
        string="State",
        default="pending",
        index=True,
        required=True,
    )
    attempts = fields.Integer(string="Attempts", default=0)
    last_error = fields.Text(string="Last Error")
    sent_at = fields.Datetime(string="Sent At")

    _sql_constraints = [
        ("msg_id_unique", "unique(msg_id)", "msg_id must be unique across all messages."),
    ]

    @api.model
    def enqueue(self, flow, action, idem_key, payload, as_of=None):
        """
        Enqueues a message into the outbox in the CURRENT database transaction.
        Never executes any network call.
        """
        routing_key = f"odoo.{flow}.{action}"
        msg_uuid = str(uuid.uuid4())
        record = self.create({
            "msg_id": msg_uuid,
            "idem_key": idem_key,
            "source": "odoo",
            "flow": flow,
            "action": action,
            "schema": SCHEMA_VERSION,
            "as_of": as_of or fields.Datetime.now(),
            "payload": payload,
            "routing_key": routing_key,
            "state": "pending",
        })
        _logger.debug(
            "PRISM Outbox: enqueued %s (%s) with idem_key=%s",
            routing_key, msg_uuid, idem_key
        )
        return record

    @api.model
    def _purge_sent_outbox(self, days=7):
        """
        Housekeeping cron: deletes successfully sent rows older than `days` days.
        """
        cutoff = fields.Datetime.now() - timedelta(days=days)
        records_to_delete = self.search([
            ("state", "=", "sent"),
            ("sent_at", "<", cutoff)
        ])
        count = len(records_to_delete)
        if count:
            records_to_delete.unlink()
            _logger.info("PRISM Outbox: purged %d sent records older than %d days", count, days)
        return True

