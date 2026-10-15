#!/usr/bin/env python3
"""
PRISM RetailMind: Outbox Relay Daemon (prism-sync-relay)
Polls the prism_sync_outbox table in Odoo PostgreSQL, validates schemas,
and pushes rows to the PRISM FastAPI Ingress API (which confirms delivery to RabbitMQ).
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

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
        Path.cwd() / ".env",
    ]:
        if env_candidate and Path(env_candidate).is_file():
            load_dotenv(env_candidate)
            break
except ImportError:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor
import requests

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
ODOO_DB_DSN = os.getenv("ODOO_DB_DSN") or os.getenv("ODOO_RO_DSN", "host=172.27.133.76 port=5432 dbname=odoo user=prism_sync password=cybage@123")
INGRESS_URL = os.getenv("INGRESS_URL", "http://127.0.0.1:8095").rstrip("/")
INGEST_API_KEY = os.getenv("INGEST_API_KEY", "prism_secret_2026")
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
        self.http_session = requests.Session()
        self.http_session.headers.update({
            "X-API-Key": INGEST_API_KEY,
            "Content-Type": "application/json",
            "User-Agent": "prism-sync-relay/1.0"
        })

    def connect_db(self):
        if self.db_conn and not self.db_conn.closed:
            return
        logger.info("Connecting to Odoo PostgreSQL database...")
        self.db_conn = psycopg2.connect(ODOO_DB_DSN)
        self.db_conn.autocommit = False
        logger.info("Connected to Odoo database.")

    def close(self):
        try:
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
        except Exception as e:
            logger.debug("Error closing DB: %s", e)
        try:
            self.http_session.close()
        except Exception:
            pass

    def process_batch(self) -> int:
        self.connect_db()

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

        logger.info("Fetched %d pending outbox rows to process.", len(rows))
        pushed_count = 0

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
                "payload": payload_dict,
                "routing_key": row["routing_key"]
            }

            # 1. Local Schema Pre-validation
            try:
                validated_msg = validate_message(envelope)
            except ValidationError as val_err:
                logger.error("Schema validation failed for outbox id=%d msg_id=%s: %s", row_id, row["msg_id"], val_err)
                self._mark_failed(cur, row_id, str(val_err))
                continue

            # 2. Push to FastAPI Ingress API
            flow = row["flow"]
            ingress_endpoint = f"{INGRESS_URL}/ingest/odoo/{flow}"

            try:
                resp = self.http_session.post(
                    ingress_endpoint,
                    json=validated_msg,
                    timeout=10.0
                )
            except requests.RequestException as req_err:
                logger.warning(
                    "Ingress API at %s unreachable: %s. Rolling back batch, will retry next cycle.",
                    INGRESS_URL, req_err
                )
                self.db_conn.rollback()
                cur.close()
                return pushed_count

            if resp.status_code == 200:
                self._mark_sent(cur, row_id)
                pushed_count += 1
                logger.info(
                    "Pushed msg_id=%s flow=%s outbox_id=%d -> Ingress (200 OK)",
                    row["msg_id"], flow, row_id
                )
            elif resp.status_code == 422:
                err_text = f"Ingress 422 Validation Error: {resp.text}"
                logger.error("Outbox id=%d msg_id=%s rejected by Ingress: %s", row_id, row["msg_id"], err_text)
                self._mark_failed(cur, row_id, err_text)
            elif resp.status_code == 401:
                logger.critical("Ingress 401 Unauthorized: Invalid INGEST_API_KEY! Please verify prism-sync.env.")
                self.db_conn.rollback()
                cur.close()
                return pushed_count
            elif resp.status_code == 503:
                logger.warning(
                    "Ingress returned 503 (RabbitMQ publish failed). Message msg_id=%s remains pending for retry.",
                    row["msg_id"]
                )
                self.db_conn.rollback()
                cur.close()
                return pushed_count
            else:
                logger.error(
                    "Ingress unexpected status %d: %s for msg_id=%s. Rolling back.",
                    resp.status_code, resp.text, row["msg_id"]
                )
                self.db_conn.rollback()
                cur.close()
                return pushed_count

        self.db_conn.commit()
        cur.close()
        return pushed_count

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

    def sleep(self, seconds: float):
        start = time.time()
        while running and (time.time() - start) < seconds:
            time.sleep(0.5)

    def run(self):
        logger.info("Starting PRISM Outbox Relay loop (Target: %s)...", INGRESS_URL)
        while running:
            try:
                count = self.process_batch()
                if count == 0 and running:
                    self.sleep(POLL_INTERVAL_SEC)
            except psycopg2.OperationalError as conn_err:
                logger.warning("Database connection error: %s. Reconnecting in 5 seconds...", conn_err)
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
