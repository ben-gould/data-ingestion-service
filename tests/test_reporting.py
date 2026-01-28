import pandas as pd
from ingestion.reporting import calculate_data_quality_metrics

# def test_calculate_metrics_basic():
    # Create small test df, verify metrics dict has expected keys/values
    # metrics = calculate_data_quality_metrics(test_df)

# def test_summarize_errors_counts_correctly():
    # Create ValidationResult with known errors, verify counts
    
def test_generate_report_creates_html_file():
    test_df = pd.DataFrame({
        'transaction_id':['tx01', 'tx02', 'tx03'],
        'timestamp': [None, '2023-01-01T10:00:00', '2023-01-01T15:00:00'],
        'user_id': ['user1','user2','user1'],
        'amount': [100, -200, 300],
        'currency': ['USD', 'EUR', 'JPY']
    })


