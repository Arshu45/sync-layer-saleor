{
    "name": "PRISM Saleor Connector",
    "version": "17.0.1.0.0",
    "category": "Sales/E-commerce",
    "summary": "Transactional Outbox Sync Layer between Odoo 17 CE and Saleor 3.23",
    "author": "PRISM RetailMind Engineering",
    "license": "LGPL-3",
    "depends": [
        "base",
        "product",
        "sale",
        "stock",
    ],
    "data": [
        "security/ir.model.access.csv",
        "data/ir_cron.xml",
    ],
    "installable": True,
    "application": False,
    "auto_install": False,
}

