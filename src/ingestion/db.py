import sqlite3
from pathlib import Path
from typing import Union
from ingestion.models import Transaction
from datetime import datetime

def init_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            user_id TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL
        )  
        """
    )

    conn.commit()
    conn.close()


def count_transactions(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM transactions"
    )
    result = cursor.fetchone()
    conn.close()
    return result[0]

def insert_transaction(db_path: Path, 
                       transaction: Union[Transaction, tuple, None] = None,
                       transaction_id: str = None, 
                       timestamp: str = None, 
                       user_id: str = None, 
                       amount: float = None,
                       currency: str = None) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if isinstance(transaction, Transaction):
        transaction_id = transaction.transaction_id
        timestamp = str(transaction.timestamp)
        user_id = transaction.user_id
        amount = transaction.amount
        currency = transaction.currency
    

    cursor.execute(
        """
        INSERT INTO transactions (transaction_id, timestamp, user_id, amount, currency)
        VALUES (?, ?, ?, ?, ?)
        """,
        (transaction_id, timestamp, user_id, amount, currency)
    )
    conn.commit()
    conn.close()
