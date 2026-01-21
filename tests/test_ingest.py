import logging
from pathlib import Path

import pytest

from ingestion.ingest import load_transactions
from ingestion.db import count_transactions, init_db
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)

def test_invalid_rows_logged_and_skipped(tmp_path, caplog):
    """
    Docstring for test_invalid_rows_logged_and_skipped
    Invalid rows should be skipped and logged, valid rows counted
    :param temp_path: temporary path
    :param caplog: Description
    """

     # 1. Create a temporary CSV file
    csv_path = tmp_path / "test_transactions.csv" 
    
    csv_path.write_text(
        """transaction_id,timestamp,user_id,amount,currency
        tx001,2023-01-01T10:00:00,user1,12.50,USD
        tx002,2023-01-01T10:05:00,user2,user2,USD"""
        .strip()
    )

    test_db_path = tmp_path / "test.db"
    init_db(test_db_path)

    # 2. Capture WARNING-level logs
    caplog.set_level(logging.WARNING)

    # 3. Run ingestion
    result = load_transactions(csv_path, test_db_path)
    result_sql = count_transactions(test_db_path)

    # 4. Assert correct number of valid rows
    assert len(result) == 1
    assert result_sql == 1

    # 5. Assert warning was logged
    assert "Found 1 validation errors" in caplog.text