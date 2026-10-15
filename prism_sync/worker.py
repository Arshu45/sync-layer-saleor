#!/usr/bin/env python3
"""
PRISM RetailMind: Sync Worker Consumer (prism-sync-worker)
Consumes queues from RabbitMQ (to_saleor.products) and applies changes
to Saleor 3.23 via GraphQL mutations and prism_sync bookkeeping DB.
"""

import json
import logging
import os
import signal
import sys
import time

from pathlib import Path

# Ensure package root is in sys.path
package_root = Path(__file__).resolve().parent.parent
if str(package_root) not in sys.path:
    sys.path.insert(0, str(package_root))

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

import pika
import psycopg2

try:
    from prism_sync.schemas import validate_message
    from prism_sync.utils.saleor_client import SaleorClient
    from prism_sync.handlers.products import ProductHandler
except ModuleNotFoundError:
    from schemas import validate_message
    from utils.saleor_client import SaleorClient
    from handlers.products import ProductHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [worker] %(message)s"
)
logger = logging.getLogger("prism-sync-worker")

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://prism_sync:prism_sync@127.0.0.1:5672/prism")
SYNC_DSN = os.getenv("SYNC_DSN", "host=127.0.0.1 dbname=prism_sync user=prism_sync password=prism_sync")
SALEOR_GRAPHQL = os.getenv("SALEOR_GRAPHQL", "http://127.0.0.1:8000/graphql/")
SALEOR_APP_TOKEN = os.getenv("SALEOR_APP_TOKEN", "")

QUEUE_PRODUCTS = "to_saleor.products"

running = True


def handle_shutdown(signum, frame):
    global running
    logger.info("Received termination signal (%d), stopping worker...", signum)
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


class SyncWorker:
    def __init__(self):
        self.db_conn = None
        self.saleor_client = None
        self.product_handler = None
        self.rmq_conn = None
        self.rmq_channel = None

    def initialize(self):
        logger.info("Connecting to prism_sync DB...")
        self.db_conn = psycopg2.connect(SYNC_DSN)
        self.db_conn.autocommit = False

        logger.info("Initializing Saleor GraphQL client at %s...", SALEOR_GRAPHQL)
        self.saleor_client = SaleorClient(endpoint=SALEOR_GRAPHQL, token=SALEOR_APP_TOKEN)
        self.product_handler = ProductHandler(self.db_conn, self.saleor_client)

        logger.info("Connecting to RabbitMQ broker at %s...", RABBIT_URL.split("@")[-1])
        parameters = pika.URLParameters(RABBIT_URL)
        parameters.heartbeat = 60
        self.rmq_conn = pika.BlockingConnection(parameters)
        self.rmq_channel = self.rmq_conn.channel()
        self.rmq_channel.basic_qos(prefetch_count=10)

    def close(self):
        try:
            if self.rmq_channel and self.rmq_channel.is_open:
                self.rmq_channel.close()
            if self.rmq_conn and self.rmq_conn.is_open:
                self.rmq_conn.close()
        except Exception:
            pass

        try:
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
        except Exception:
            pass

    def on_product_message(self, ch, method, properties, body):
        try:
            raw = json.loads(body.decode("utf-8"))
            envelope = validate_message(raw)
            logger.info("Processing message msg_id=%s flow=product action=%s", envelope["msg_id"], envelope["action"])
            
            success = self.product_handler.handle(envelope)
            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info("ACK message msg_id=%s", envelope["msg_id"])
            else:
                logger.warning("Handler returned failure for msg_id=%s, requeuing...", envelope["msg_id"])
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except Exception as exc:
            logger.exception("Error processing product message: %s", exc)
            # Nack without requeue if message is poison so it routes to dead-letter queue (DLX)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def run(self):
        while running:
            try:
                self.initialize()
                logger.info("Subscribing to queue '%s'...", QUEUE_PRODUCTS)
                self.rmq_channel.basic_consume(
                    queue=QUEUE_PRODUCTS,
                    on_message_callback=self.on_product_message,
                    auto_ack=False
                )
                logger.info("PRISM Sync Worker started. Waiting for messages...")

                while running and self.rmq_channel.is_open:
                    self.rmq_conn.process_data_events(time_limit=1)

            except (psycopg2.OperationalError, pika.exceptions.AMQPConnectionError) as conn_err:
                logger.warning("Worker connection dropped: %s. Reconnecting in 5s...", conn_err)
                self.close()
                time.sleep(5)
            except Exception as exc:
                logger.exception("Worker error: %s. Retrying in 5s...", exc)
                self.close()
                time.sleep(5)

        logger.info("Worker stopped.")
        self.close()


def main():
    worker = SyncWorker()
    worker.run()


if __name__ == "__main__":
    main()

