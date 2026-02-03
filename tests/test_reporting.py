import pandas as pd
from ingestion.reporting import calculate_data_quality_metrics, summarize_errors, generate_quality_report
from ingestion.validation import ValidationError, ValidationResult
from conftest import generate_transactions
from ingestion.ingest import load_transactions
from pathlib import Path 

def test_calculate_metrics_basic():
    # Create small test df, verify metrics dict has expected keys/values
    df = pd.DataFrame(
        {
            'transaction_id': ['tx001', 'tx002', 'tx003'],
            'timestamp': ['2023-01-01T10:00:00', None,'2023-01-01T20:00:00'],
            'user_id': ['user_2', 'user_2', 'user_3'],
            'amount': [5, 10, 20],
            'currency': ['USD', 'USD', 'USD']
        }
    )

    metrics = calculate_data_quality_metrics(df)

    assert metrics['timestamp']['null_count'] == 1
    assert metrics['amount']['min'] == 5
    assert metrics['user_id']['value_counts']['user_2'] == 2


def test_summarize_errors_counts_correctly():
    # Create ValidationResult with known errors, verify counts
    result = ValidationResult(is_valid=False, errors=[
        ValidationError(row_number=1, column='amount', value=-1, error_message='Value cannot be negative'),
        ValidationError(row_number=3, column='timestamp', value='None', error_message='invalid_date')
    ])

    summary = summarize_errors(result)

    assert summary['negative_amount'] == 1
    assert summary['invalid_date'] == 1
    

def test_generate_report_creates_html_file(tmp_path):
    csv_path = tmp_path / 'test.csv'
    db_path = tmp_path / 'test.db'

    csv_path.write_text(
        """transaction_id,timestamp,user_id,amount,currency
        tx001,2023-01-01T10:00:00,user1,12.50,USD
        tx002,2023-01-01T10:05:00,user2,user2,USD"""
    )
    
    # Run ingestion - this returns summary AND generates report
    result_list, result_dict = load_transactions(csv_path, db_path, tmp_path)
    
    # Check report was created
    report_path = result_dict['report_path']  # Or wherever you store it
    assert Path(report_path).exists()

    assert Path(report_path).exists()
    html_content = Path(report_path).read_text()
    assert '<html>' in html_content
    assert 'Data Quality Report' in html_content
    assert str(result_dict['total_rows']) in html_content


