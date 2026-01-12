import logging
from pathlib import Path

import pytest

from ingestion.ingest import load_transactions

def test_invalid_rows_logged_and_skipped(temp_path, caplog):
    """
    Docstring for test_invalid_rows_logged_and_skipped
    Invalid rows should be skipped and logged, valid rows counted
    :param temp_path: temporary path
    :param caplog: Description
    """

     # 1. Create a temporary CSV file
    csv_path = temp_path / "test_transactions.csv" # TODO either change this file name or make it
    
    csv_path.write_text(
        """transaction_id,timestamp,user_id,amount,currency
        tx001,2023-01-01T10:00:00,user1,12.50,USD
        tx002,2023-01-01T10:05:00,user2,user2,USD"""
        .strip()
    )

    # 2. Capture WARNING-level logs
    caplog.set_level(logging.WARNING)

    # 3. Run ingestion
    result = load_transactions(csv_path)

    # 4. Assert correct number of valid rows
    assert len(result) == 1

    # 5. Assert warning was logged
    assert "Invalid transaction row" in caplog.text