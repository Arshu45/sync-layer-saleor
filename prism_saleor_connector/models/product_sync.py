import logging
from datetime import datetime, timedelta
from odoo import api, fields, models

_logger = logging.getLogger(__name__)

WATERMARK_PARAM = "prism_sync.watermark.products"


class PrismProductSync(models.AbstractModel):
    _name = "prism.product.sync"
    _description = "PRISM Product Catalog Sync Collector"

    @api.model
    def _collect_product_changes(self):
        """
        Scheduled action collector for products.
        Runs periodically (e.g. nightly or every 10 mins).
        Queries products and templates changed since (watermark - 2 minutes).
        Enqueues outbox records and updates the watermark in the same transaction.
        """
        config_param = self.env["ir.config_parameter"].sudo()
        watermark_str = config_param.get_param(WATERMARK_PARAM)
        now = fields.Datetime.now()

        if watermark_str:
            try:
                watermark = fields.Datetime.to_datetime(watermark_str)
            except Exception:
                watermark = now - timedelta(days=1)
        else:
            # If no watermark exists, fallback to last 1 day (or 30 days)
            watermark = now - timedelta(days=1)

        # 2-minute safety overlap buffer to handle PostgreSQL transaction start timestamps
        lookback = watermark - timedelta(minutes=2)
        _logger.info("PRISM Sync: collecting products modified since %s", lookback)

        # Find variants or templates modified since lookback
        domain = [
            "|",
            ("write_date", ">=", lookback),
            ("product_tmpl_id.write_date", ">=", lookback),
        ]
        variants = self.env["product.product"].with_context(active_test=False).search(domain)
        _logger.info("PRISM Sync: found %d candidate product variants", len(variants))

        outbox = self.env["prism.sync.outbox"]
        enqueued_count = 0

        for variant in variants:
            tmpl = variant.product_tmpl_id
            sku = (variant.default_code or "").strip()
            if not sku:
                # Fallback SKU if internal reference is empty
                sku = f"ODOO-P{variant.id}"

            # Extract variant attributes
            attributes = {}
            for ptav in variant.product_template_attribute_value_ids:
                attr_name = ptav.attribute_id.name or "Attribute"
                attr_value = ptav.name or ""
                attributes[attr_name] = attr_value

            # Price calculations across channels
            base_price = float(variant.lst_price or 0.0)
            channels = [
                {"channel_slug": "online", "price_inr": base_price},
                {"channel_slug": "social-commerce", "price_inr": base_price},
                {"channel_slug": "outlet", "price_inr": round(base_price * 0.85, 2)},
            ]

            category_path = tmpl.categ_id.complete_name if tmpl.categ_id else "Default"
            is_active = bool(variant.active and tmpl.active)
            latest_write = max(variant.write_date or now, tmpl.write_date or now)
            latest_write_iso = latest_write.strftime("%Y-%m-%dT%H:%M:%SZ")

            payload = {
                "sku": sku,
                "odoo_product_id": variant.id,
                "odoo_template_id": tmpl.id,
                "name": tmpl.name or variant.display_name or "Unnamed Product",
                "description": tmpl.description_sale or tmpl.description or "",
                "category_path": category_path,
                "banner": "prism",
                "attributes": attributes,
                "active": is_active,
                "channels": channels,
            }

            idem_key = f"product:{sku}:{latest_write_iso}"
            action = "updated" if variant.create_date != variant.write_date else "created"

            outbox.enqueue(
                flow="product",
                action=action,
                idem_key=idem_key,
                payload=payload,
                as_of=latest_write,
            )
            enqueued_count += 1

        # Advance watermark inside the same transaction
        config_param.set_param(WATERMARK_PARAM, fields.Datetime.to_string(now))
        _logger.info("PRISM Sync: enqueued %d product outbox records. Advanced watermark to %s", enqueued_count, now)
        return True

