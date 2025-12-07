from dataclasses import dataclass
from typing import List

@dataclass
class TableConfig:
    """Database table configuration"""
    name: str
    columns: List[str]


class Config:
    """Application configuration"""
    
    TABLES = {
        'orderbook': TableConfig('bybit.orderbook', ['topic', 'type', 'ts_message', 'channel', 'symbol_name', 'update_id', 'sequence_id', 'side', 'price', 'size', 'ts_orderbook','matching_key']),
        'order': TableConfig('bybit.order', ['channel', 'symbol_name', 'price', 'size','type', 'flux','ts_orderbook']),
        
    }

    ORDERBOOK_DEPTH = 1
    TESTNET = False
    API_KEY = "xxxxxxxxxxxxxxxxxx"
    API_SECRET = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    UNIQUE_ORDER = True
    

    MAX_RISK_OPEN_POSITION = 1000000

    DB_CONFIG = {
    "database": "postgres",
    "host": "localhost",
    "user": "postgres",
    "password": "guest",
    "port": "5433",
    }
    DUMP_ORDERBOOK_TO_DB = True
    DUMP_ORDER_TO_DB = True
    REAL_ORDER_SENDING = False
    