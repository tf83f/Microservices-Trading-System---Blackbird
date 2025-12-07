"""
============================================================
                     DATABASE MANAGER
============================================================

Responsible for:
1. Managing PostgreSQL connections
2. Executing safe batched inserts
3. Handling conflict-safe upserts
4. Ensuring stable database interaction throughout the
   trading & signal pipeline.

This module is intentionally resilient:
- Reconnects automatically if connection drops.
- Logs errors without interrupting.
- Never throws uncaught exceptions to the upper layers,
  so the execution engine remains stable even if the DB fails.

============================================================
"""

import psycopg2
import psycopg2.extras
from typing import List, Tuple, Dict
from config import TableConfig
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    """PostgreSQL interface with safe batch insertion."""

    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None

    def connect(self) -> None:
        """Establish database connection if not already connected."""
        try:
            if not self.conn or self.conn.closed:
                self.conn = psycopg2.connect(**self.db_config)
                logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")

    def close(self) -> None:
        """Close the PostgreSQL connection."""
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error while closing DB connection: {e}")

    def _ensure_conn(self) -> bool:
        """Try reconnecting automatically if connection dropped."""
        if not self.conn or self.conn.closed:
            logger.warning("Database connection lost — attempting reconnect…")
            try:
                self.conn = psycopg2.connect(**self.db_config)
                logger.info("Database reconnection successful")
            except Exception as e:
                logger.error(f"Database reconnection failed: {e}")
                return False
        return True

    def insert_batch(self, table_config: TableConfig, data: List[Tuple]) -> None:
        """
        Insert multiple rows safely.
        On any DB error:
            - log failure
            - rollback transaction
            - do NOT interrupt caller
        """
        if not data:
            return

        if not self._ensure_conn():
            logger.error("insert_batch aborted: DB unavailable")
            return

        cursor = self.conn.cursor()
        sql = f"""
            INSERT INTO {table_config.name}
            ({','.join(table_config.columns)})
            VALUES %s;
        """

        try:
            psycopg2.extras.execute_values(cursor, sql, data)
            self.conn.commit()
            logger.info(f"Inserted batch into {table_config.name} ({len(data)} rows)")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"DB insert_batch failed ({table_config.name}): {e}")
        finally:
            cursor.close()

    def insert_with_conflict(
        self,
        table_config: TableConfig,
        data: List[Tuple],
        conflict_columns: List[str]
    ) -> None:
        """
        Insert while ignoring duplicates based on conflict columns.
        """
        if not data:
            return

        if not self._ensure_conn():
            logger.error("insert_with_conflict aborted: DB unavailable")
            return

        cursor = self.conn.cursor()

        sql = f"""
            INSERT INTO {table_config.name}
            ({','.join(table_config.columns)})
            VALUES %s
            ON CONFLICT ({','.join(conflict_columns)}) DO NOTHING;
        """

        try:
            psycopg2.extras.execute_values(cursor, sql, data)
            self.conn.commit()
            logger.info(
                f"Upsert batch into {table_config.name} ({len(data)} rows) — duplicates ignored"
            )
        except Exception as e:
            self.conn.rollback()
            logger.error(
                f"DB insert_with_conflict failed ({table_config.name}): {e}"
            )
        finally:
            cursor.close()
