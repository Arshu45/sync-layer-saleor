"""
PRISM RetailMind Sync Layer: Schemas & Envelope Validation (Schema Version 1)
Governs messages exchanged over RabbitMQ between Odoo 17 CE and Saleor 3.23.
"""

from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Optional
import uuid

SCHEMA_VERSION = 1
VALID_SOURCES = {"odoo", "saleor"}
VALID_FLOWS = {"product", "stock", "order", "customer"}
VALID_ACTIONS = {"created", "updated", "changed", "paid", "cancelled"}


class ValidationError(Exception):
    """Raised when an envelope or payload violates PRISM Schema v1 specification."""
    pass


def _validate_uuid(val: str, field_name: str) -> None:
    try:
        uuid.UUID(str(val))
    except (ValueError, TypeError, AttributeError):
        raise ValidationError(f"Field '{field_name}' must be a valid UUID string, got: {val!r}")


def _validate_iso8601(val: str, field_name: str) -> None:
    if not isinstance(val, str):
        raise ValidationError(f"Field '{field_name}' must be an ISO-8601 string, got: {val!r}")
    try:
        # Handles standard ISO format with Z or timezone offset
        datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        raise ValidationError(f"Field '{field_name}' must be a valid ISO-8601 timestamp, got: {val!r}")


def validate_product_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates a Product Payload (flow='product', action='created' | 'updated').
    Expected shape:
      sku: str (non-empty)
      odoo_product_id: int
      name: str (non-empty)
      description: str (optional)
      category_path: str (optional)
      banner: str (optional)
      attributes: dict[str, str] (name-value map)
      active: bool
      channels: list of {channel_slug: str, price_inr: float | int}
    """
    if not isinstance(payload, dict):
        raise ValidationError(f"Product payload must be a JSON object, got: {type(payload).__name__}")

    sku = payload.get("sku")
    if not sku or not isinstance(sku, str) or not sku.strip():
        raise ValidationError("Product payload missing non-empty string 'sku'")

    odoo_product_id = payload.get("odoo_product_id")
    if odoo_product_id is None or not isinstance(odoo_product_id, int):
        raise ValidationError("Product payload requires integer 'odoo_product_id'")

    name = payload.get("name")
    if not name or not isinstance(name, str) or not name.strip():
        raise ValidationError("Product payload missing non-empty string 'name'")

    attributes = payload.get("attributes", {})
    if not isinstance(attributes, dict):
        raise ValidationError("Product payload 'attributes' must be an object (dict)")

    channels = payload.get("channels", [])
    if not isinstance(channels, list):
        raise ValidationError("Product payload 'channels' must be a list")

    for idx, ch in enumerate(channels):
        if not isinstance(ch, dict):
            raise ValidationError(f"Channel entry at index {idx} must be an object")
        slug = ch.get("channel_slug")
        raw_price = ch.get("price_inr")
        if not slug or not isinstance(slug, str):
            raise ValidationError(f"Channel entry at index {idx} missing valid 'channel_slug'")
        if raw_price is None:
            raise ValidationError(f"Channel entry at index {idx} requires non-negative numeric 'price_inr'")
        try:
            price_val = float(raw_price)
            if price_val < 0:
                raise ValueError()
        except (ValueError, TypeError):
            raise ValidationError(f"Channel entry at index {idx} requires non-negative numeric 'price_inr'")

    # Optional fields defaults
    res = {
        "sku": sku.strip(),
        "odoo_product_id": odoo_product_id,
        "name": name.strip(),
        "description": str(payload.get("description") or ""),
        "category_path": str(payload.get("category_path") or ""),
        "banner": str(payload.get("banner") or ""),
        "attributes": {str(k): str(v) for k, v in attributes.items()},
        "active": bool(payload.get("active", True)),
        "channels": [
            {
                "channel_slug": ch["channel_slug"].strip(),
                "price_inr": float(ch["price_inr"])
            }
            for ch in channels
        ]
    }
    if "odoo_template_id" in payload:
        res["odoo_template_id"] = payload["odoo_template_id"]
    return res


def validate_envelope(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates standard 8-field envelope.
    """
    if not isinstance(msg, dict):
        raise ValidationError(f"Message must be a JSON object, got: {type(msg).__name__}")

    # 1. msg_id
    msg_id = msg.get("msg_id")
    _validate_uuid(msg_id, "msg_id")

    # 2. idem_key
    idem_key = msg.get("idem_key")
    if not idem_key or not isinstance(idem_key, str) or not idem_key.strip():
        raise ValidationError("Message missing non-empty string 'idem_key'")

    # 3. source
    source = msg.get("source")
    if source not in VALID_SOURCES:
        raise ValidationError(f"Invalid source '{source}', must be one of: {VALID_SOURCES}")

    # 4. flow
    flow = msg.get("flow")
    if flow not in VALID_FLOWS:
        raise ValidationError(f"Invalid flow '{flow}', must be one of: {VALID_FLOWS}")

    # 5. action
    action = msg.get("action")
    if action not in VALID_ACTIONS:
        raise ValidationError(f"Invalid action '{action}', must be one of: {VALID_ACTIONS}")

    # 6. schema
    schema = msg.get("schema")
    if schema != SCHEMA_VERSION:
        raise ValidationError(f"Unsupported schema version '{schema}', expected {SCHEMA_VERSION}")

    # 7. as_of
    as_of = msg.get("as_of")
    _validate_iso8601(as_of, "as_of")

    # 8. payload
    payload = msg.get("payload")
    if not isinstance(payload, dict):
        raise ValidationError("Message 'payload' must be a JSON object")

    return {
        "msg_id": str(msg_id),
        "idem_key": idem_key.strip(),
        "source": source,
        "flow": flow,
        "action": action,
        "schema": schema,
        "as_of": as_of,
        "payload": payload
    }


def validate_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates envelope and flow-specific payload.
    """
    envelope = validate_envelope(msg)
    flow = envelope["flow"]

    if flow == "product":
        envelope["payload"] = validate_product_payload(envelope["payload"])
    
    return envelope


def build_envelope(
    flow: str,
    action: str,
    idem_key: str,
    payload: Dict[str, Any],
    source: str = "odoo",
    msg_id: Optional[str] = None,
    as_of: Optional[str] = None
) -> Dict[str, Any]:
    """
    Helper to construct and validate a fresh PRISM envelope.
    """
    envelope = {
        "msg_id": msg_id or str(uuid.uuid4()),
        "idem_key": idem_key,
        "source": source,
        "flow": flow,
        "action": action,
        "schema": SCHEMA_VERSION,
        "as_of": as_of or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "payload": payload
    }
    return validate_message(envelope)
