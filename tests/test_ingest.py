from pathlib import Path
from ingestion.ingest import load_transactions
from ingestion.db import count_transactions, init_db
import logging
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)

def test_valid_csv(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    valid_csv_path = tmp_path / "test_valid_csv.csv" 
    
    valid_csv_path.write_text(
        """transaction_id,timestamp,user_id,amount,currency
        tx001,2023-01-01T10:00:00,user1,12.50,USD
        tx002,2023-01-01T10:05:00,user2,7.99,USD
        tx003,2023-01-01T10:10:00,user3,20.00,EUR
        tx004,2023-01-01T10:10:00,user4,1.10,EUR"""
        .strip()
    )

    test_valid_csv_db_path = tmp_path / "test_valid_csv.db"
    init_db(test_valid_csv_db_path)

    valid_csv_result, _ = load_transactions(valid_csv_path, test_valid_csv_db_path)
    valid_csv_result_sql = count_transactions(test_valid_csv_db_path)

    print(f"Caplog text: '{caplog.text}'")  # Add this
    print(f"Caplog records: {caplog.records}")

    assert len(valid_csv_result) == 4
    assert valid_csv_result_sql == 4
    assert "All rows passed validation" in caplog.text

def test_invalid_csv(tmp_path, caplog):
    invalid_csv_path = tmp_path / "test_invalid_csv.csv" 
    
    invalid_csv_path.write_text(
        """transaction_id,timestamp,user_id,amount,currency
        tx001,2023-01-01T10:00:00,user1,12.50,USD
        tx002,2023-01-01T10:05:00,user2,user2,USD"""
        .strip()
    )

    test_invalid_csv_db_path = tmp_path / "test_invalid_csv.db"
    init_db(test_invalid_csv_db_path)

    caplog.set_level(logging.WARNING)

    invalid_csv_result, _ = load_transactions(invalid_csv_path, test_invalid_csv_db_path)
    invalid_csv_result_sql = count_transactions(test_invalid_csv_db_path)

    assert len(invalid_csv_result) == 1
    assert invalid_csv_result_sql == 1
    assert "Found 1 validation errors" in caplog.text

def test_valid_json(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    valid_json_path = tmp_path / "test_valid_json.json"

    valid_json_path.write_text(json.dumps(
        [
            {"transaction_id": "jsontx001a","timestamp": "2023-01-01T10:00:00", "user_id": "user1","amount": 12.50, "currency": "USD"},
            {"transaction_id": "jsontx002a","timestamp": "2023-01-01T10:05:00", "user_id": "user2","amount": 7.99, "currency": "USD"},
            {"transaction_id": "jsontx003a","timestamp": "2023-01-01T10:10:00", "user_id": "user3","amount": 20.00, "currency": "EUR"},
            {"transaction_id": "jsontx004a","timestamp": "2023-01-01T10:10:00", "user_id": "user4","amount": 1.10, "currency": "EUR"}
        ], indent = 2)
    )

    test_valid_json_db_path = tmp_path / "test_valid_json.db"
    init_db(test_valid_json_db_path)

    caplog.set_level(logging.INFO)

    valid_json_result, _ = load_transactions(valid_json_path, test_valid_json_db_path)
    valid_json_result_sql = count_transactions(test_valid_json_db_path)

    assert len(valid_json_result) == 4
    assert valid_json_result_sql == 4
    assert "All rows passed validation" in caplog.text


def test_invalid_json(tmp_path, caplog):
    invalid_json_path = tmp_path / "test_invalid_json.json"
    invalid_json_path.write_text(json.dumps(
        [
            {"transaction_id":"jsontx001","timestamp":"2023-01-01T10:00:00","user_id":"user1","amount":12.50,"currency":"USD"},
            {"transaction_id":"jsontx002","timestamp":"2023-01-01T10:05:00","user_id":"user2","amount":"user2","currency":"USD"}
        ], indent = 2)
    )

    test_invalid_json_db_path = tmp_path / "test_invalid_json.db"
    init_db(test_invalid_json_db_path)

    caplog.set_level(logging.WARNING)

    invalid_json_result, _ = load_transactions(invalid_json_path, test_invalid_json_db_path)
    invalid_json_result_sql = count_transactions(test_invalid_json_db_path)

    assert len(invalid_json_result) == 1
    assert invalid_json_result_sql == 1
    assert "Found 1 validation errors" in caplog.text

# def test_equivalent_csv_json()