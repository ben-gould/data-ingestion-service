import sqlite3
from pathlib import Path

def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
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


def count_transactions(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM transactions"
        )
        result = cursor.fetchone()
    return result[0]

def insert_transaction(db_path: Path, transaction_data: dict) -> None:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO transactions (transaction_id, timestamp, user_id, amount, currency)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (transaction_id) DO UPDATE SET
                timestamp = excluded.timestamp,
                user_id = excluded.user_id,
                amount = excluded.amount,
                currency = excluded.currency
            """,
            (transaction_data['transaction_id'], 
            str(transaction_data['timestamp']), 
            transaction_data['user_id'], 
            transaction_data['amount'], 
            transaction_data['currency'])
        )
        conn.commit()

def transaction_exists(db_path: Path, transaction_id: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM transactions WHERE transaction_id = ?",
            (transaction_id,)
        )
        return cursor.fetchone() is not None