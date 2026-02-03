from unittest.mock import patch
from ingestion.cli import main
from ingestion.db import count_transactions, insert_transaction
import pytest
import pandas as pd

def test_cli_accepts_input_and_db_args(tmp_path):
    # Test that argparse correctly parses --input and --db
    test_csv_path = tmp_path / "test.csv"
    test_csv_path.write_text("transaction_id,timestamp,user_id,amount,currency\ntx001,2023-01-01T10:00:00,user1,12.50,USD")

    test_db_path = tmp_path / "test.db"

    # Fake command: python -m ingestion --input test.csv --db test.db
    fake_argv = ['ingestion', '--input', str(test_csv_path), '--db', str(test_db_path)]

    with patch('sys.argv', fake_argv):
        main() 

    assert count_transactions(test_db_path) > 0
    
def test_cli_uses_config_when_no_db_provided(tmp_path):
    # Test fallback to config.yaml
    config_db_path = tmp_path / "test.db"

    test_csv_path = tmp_path / "test.csv"
    test_csv_path.write_text("transaction_id,timestamp,user_id,amount,currency\ntx001,2024-01-01,user1,100,USD")

    test_config_path = tmp_path / "test_config.yaml"
    test_config_path.write_text(f"database: \n path: {config_db_path}")

    # Fake command: python -m ingestion --input test.csv --db test.db
    fake_argv = ['ingestion', '--input', str(test_csv_path), '--config', str(test_config_path)]

    with patch('sys.argv', fake_argv):
        main()
    
    assert config_db_path.exists()
    assert count_transactions(config_db_path) == 1

    
def test_cli_requires_input_file(capsys):
    # Test that missing --input raises error

    fake_argv = ['ingestion']

    with patch('sys.argv', fake_argv):
        with pytest.raises(SystemExit):
            main()
    
    captured = capsys.readouterr()
    assert 'required' in captured.err.lower()
    assert 'input' in captured.err.lower()

    
