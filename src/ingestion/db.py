import sqlite3
from pathlib import Path
from typing import Union
from ingestion.models import Transaction
from datetime import datetime
import pandas as pd

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

def insert_transaction(db_path: Path, transaction_data: dict) -> None:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO transactions (transaction_id, timestamp, user_id, amount, currency)
            VALUES (?, ?, ?, ?, ?)
            """,
            (transaction_data['transaction_id'], 
            str(transaction_data['timestamp']), 
            transaction_data['user_id'], 
            transaction_data['amount'], 
            transaction_data['currency'])
        )
        conn.commit()
