"""
============================================================
                       DATA STORAGE SERVICE
============================================================

Consumes orderbook updates from RabbitMQ and inserts them
into PostgreSQL using DatabaseManager.

Responsibilities:
1. Connect to RabbitMQ exchange "data_feeder"
2. Consume orderbook updates from routing key "storage"
3. Parse/flatten entries into DB-ready rows
4. Insert batches safely into PostgreSQL
5. Stay resilient:
       - Bad messages are isolated
       - DB failures never stop the consumer

============================================================
"""

import pika
import json
import logging
import sys
from database_manager import DatabaseManager
from config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DataStorage:
    """
    Consumes orderbook data from RabbitMQ and inserts it into PostgreSQL.
    """

    def __init__(self):
        self.db_manager = None
        self.connection = None
        self.channel = None
        self.queue_name = None

        try:
            if getattr(Config, "DUMP_ORDER_TO_DB", False):
                self.db_manager = DatabaseManager(Config.DB_CONFIG)
                self.db_manager.connect()

            self._start_rabbitmq()

        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            self.cleanup()
            raise

    def _start_rabbitmq(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="localhost")
            )

            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange="data_feeder",
                exchange_type="direct"
            )

            result = self.channel.queue_declare(queue="storage", exclusive=True)
            self.queue_name = result.method.queue

            self.channel.queue_bind(
                exchange="data_feeder",
                queue=self.queue_name,
                routing_key="storage"
            )

            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.callback,
                auto_ack=True
            )

            logger.info(f"Storage service consuming on queue: {self.queue_name}")
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def _parse_orderbook(self, message):
        """
        Converts a feeder orderbook update into DB-ready rows.
        Returns [] on parsing failure.
        """
        try:
            rows = []

            topic = message.get('topic')
            msg_type = message.get('type')  
            ts_message = message.get('ts')
            channel = message.get('channel')
            orderbook = message.get('data')
            ts_orderbook = int(message.get('cts'))
            matching_key = message.get('matching_key')

            orderbook = message.get("data")
            if not orderbook:
                logger.warning("Received message missing 'data' block")
                return []

            symbol_name = orderbook.get('s')
            update_id = int(orderbook.get('u'))
            sequence_id = int(orderbook.get('seq'))
            # Bids
            for bid in orderbook.get('b', []): 
                rows.append((
                    topic, msg_type, ts_message, channel, symbol_name, 
                    update_id, sequence_id, 'bid', bid[0], bid[1], ts_orderbook, matching_key
                ))

            # Asks
            for ask in orderbook.get('a', []):  
                rows.append((
                    topic, msg_type, ts_message, channel, symbol_name, 
                    update_id, sequence_id, 'ask', ask[0], ask[1], ts_orderbook, matching_key
                ))
            return rows
        except Exception as e:
            logger.error(f"Orderbook parse error: {e}")
            return []


    def callback(self, ch, method, properties, body):
        """Handles incoming messages. Parsing or DB errors never stop the consumer."""
        try:
            msg = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return

        rows = self._parse_orderbook(msg)

        if rows and getattr(Config, "DUMP_ORDER_TO_DB", False):
            try:
                self.db_manager.insert_batch(Config.TABLES["orderbook"], rows)
            except Exception as e:
                logger.error(f"DB insert failed: {e}")

    def cleanup(self):
        """Close RabbitMQ & DB resources safely."""
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
                self.channel.close()
                logger.info("RabbitMQ channel closed")

            if self.connection and self.connection.is_open:
                self.connection.close()
                logger.info("RabbitMQ connection closed")

            if self.db_manager:
                self.db_manager.close()
                logger.info("Database connection closed")

        except Exception as e:
            logger.error(f"Cleanup error: {e}")

if __name__ == "__main__":
    try:
        DataStorage()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
