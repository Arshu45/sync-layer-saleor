#!/usr/bin/env python3
"""
PRISM RetailMind: Outbox Relay Daemon (prism-sync-relay)
Polls the prism_sync_outbox table in Odoo PostgreSQL, validates schemas,
and publishes confirmed persistent messages to RabbitMQ exchange 'prism.sync'.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
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
from psycopg2.extras import RealDictCursor

try:
    from prism_sync.schemas import validate_message, ValidationError
except ModuleNotFoundError:
    from schemas import validate_message, ValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [relay] %(message)s"
)
logger = logging.getLogger("prism-sync-relay")

# Environment variables
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://prism_sync:prism_sync@127.0.0.1:5672/prism")
ODOO_DB_DSN = os.getenv("ODOO_DB_DSN") or os.getenv("ODOO_RO_DSN", "host=127.0.0.1 dbname=odoo user=prism_sync password=prism_sync")
EXCHANGE_NAME = os.getenv("RABBIT_EXCHANGE", "prism.sync")
POLL_INTERVAL_SEC = float(os.getenv("RELAY_POLL_INTERVAL", "5.0"))
BATCH_SIZE = int(os.getenv("RELAY_BATCH_SIZE", "100"))

running = True


def handle_shutdown(signum, frame):
    global running
    logger.info("Received termination signal (%d), shutting down relay gracefully...", signum)
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


class RelayService:
    def __init__(self):
        self.db_conn: Optional[psycopg2.extensions.connection] = None
        self.rmq_conn: Optional[pika.BlockingConnection] = None
        self.rmq_channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

    def connect_db(self):
        if self.db_conn and not self.db_conn.closed:
            return
        logger.info("Connecting to Odoo PostgreSQL database...")
        self.db_conn = psycopg2.connect(ODOO_DB_DSN)
        self.db_conn.autocommit = False
        logger.info("Connected to Odoo database.")

    def connect_rabbitmq(self):
        if self.rmq_conn and self.rmq_conn.is_open:
            return
        logger.info("Connecting to RabbitMQ broker at %s ...", RABBIT_URL.split("@")[-1])
        parameters = pika.URLParameters(RABBIT_URL)
        parameters.heartbeat = 60
        self.rmq_conn = pika.BlockingConnection(parameters)
        self.rmq_channel = self.rmq_conn.channel()
        # Enable Publisher Confirms
        self.rmq_channel.confirm_delivery()
        logger.info("Connected to RabbitMQ with publisher confirms enabled.")

    def close(self):
        try:
            if self.rmq_channel and self.rmq_channel.is_open:
                self.rmq_channel.close()
            if self.rmq_conn and self.rmq_conn.is_open:
                self.rmq_conn.close()
        except Exception as e:
            logger.debug("Error closing RabbitMQ: %s", e)

        try:
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
        except Exception as e:
            logger.debug("Error closing DB: %s", e)

    def process_batch(self) -> int:
        self.connect_db()
        self.connect_rabbitmq()

        cur = self.db_conn.cursor(cursor_factory=RealDictCursor)
        select_sql = """
            SELECT id, msg_id, idem_key, source, flow, action, schema, as_of, payload, routing_key
            FROM prism_sync_outbox
            WHERE state = 'pending'
            ORDER BY id ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED;
        """
        cur.execute(select_sql, (BATCH_SIZE,))
        rows = cur.fetchall()

        if not rows:
            self.db_conn.commit()
            cur.close()
            return 0

        logger.info("Fetched %d pending outbox rows to publish.", len(rows))
        published_count = 0

        for row in rows:
            row_id = row["id"]
            raw_payload = row["payload"]
            if isinstance(raw_payload, str):
                try:
                    payload_dict = json.loads(raw_payload)
                except Exception as e:
                    self._mark_failed(cur, row_id, f"Malformed JSON in outbox payload: {e}")
                    continue
            else:
                payload_dict = raw_payload

            as_of_val = row["as_of"]
            if isinstance(as_of_val, datetime):
                as_of_str = as_of_val.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                as_of_str = str(as_of_val)

            envelope = {
                "msg_id": row["msg_id"],
                "idem_key": row["idem_key"],
                "source": row["source"],
                "flow": row["flow"],
                "action": row["action"],
                "schema": row["schema"],
                "as_of": as_of_str,
                "payload": payload_dict
            }

            # 1. Validate envelope and payload schema
            try:
                validated_msg = validate_message(envelope)
            except ValidationError as val_err:
                logger.error("Schema validation failed for outbox id=%d msg_id=%s: %s", row_id, row["msg_id"], val_err)
                self._mark_failed(cur, row_id, str(val_err))
                continue

            # 2. Publish with publisher confirms
            routing_key = row["routing_key"]
            body_bytes = json.dumps(validated_msg).encode("utf-8")
            props = pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type="application/json",
                message_id=row["msg_id"],
            )

            try:
                self.rmq_channel.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key=routing_key,
                    body=body_bytes,
                    properties=props,
                    mandatory=True,
                )
                self._mark_sent(cur, row_id)
                published_count += 1
                logger.info("Published msg_id=%s routing_key=%s outbox_id=%d", row["msg_id"], routing_key, row_id)
            except Exception as pub_err:
                logger.error("Failed to publish message msg_id=%s: %s", row["msg_id"], pub_err)
                # Rollback current batch transaction so the unconfirmed row remains pending
                self.db_conn.rollback()
                cur.close()
                raise pub_err

        self.db_conn.commit()
        cur.close()
        return published_count

    def _mark_sent(self, cur, row_id: int):
        cur.execute(
            """
            UPDATE prism_sync_outbox
            SET state = 'sent', sent_at = NOW(), attempts = attempts + 1
            WHERE id = %s;
            """,
            (row_id,)
        )

    def _mark_failed(self, cur, row_id: int, error_text: str):
        cur.execute(
            """
            UPDATE prism_sync_outbox
            SET state = 'failed', last_error = %s, attempts = attempts + 1
            WHERE id = %s;
            """,
            (error_text, row_id)
        )

    def sleep_with_heartbeat(self, seconds: float):
        start = time.time()
        while running and (time.time() - start) < seconds:
            try:
                if self.rmq_conn and self.rmq_conn.is_open:
                    self.rmq_conn.process_data_events(time_limit=1.0)
                else:
                    time.sleep(1.0)
            except Exception:
                time.sleep(1.0)

    def run(self):
        logger.info("Starting PRISM Outbox Relay loop...")
        while running:
            try:
                count = self.process_batch()
                if count == 0 and running:
                    self.sleep_with_heartbeat(POLL_INTERVAL_SEC)
            except (psycopg2.OperationalError, pika.exceptions.AMQPConnectionError) as conn_err:
                logger.warning("Connection error: %s. Reconnecting in 5 seconds...", conn_err)
                self.close()
                time.sleep(5)
            except Exception as exc:
                logger.exception("Unexpected error in relay loop: %s. Retrying in 5 seconds...", exc)
                self.close()
                time.sleep(5)

        logger.info("Exiting relay loop.")
        self.close()


def main():
    relay = RelayService()
    relay.run()


if __name__ == "__main__":
    main()

