import sys
from pathlib import Path

# Add src to path so Python can find the ingestion module
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.validation import validate_positive, ValidationError
import csv
import pandas as pd

def test_validate_positive(tmp_path):
    csv_path = tmp_path / "test_validate_positive.csv"
    csv_path.write_text(
        """transaction_id,timestamp,user_id,amount,currency
        tx_001,datetimeTODO,user_01,-100,USD
        tx002,datetimeTODO,user_02,None,EUR
        tx003,datetimeTODO,user_02,5,JPY
        """.strip()
    )

    df = pd.read_csv(csv_path, dtype={'transaction_id':str,
                                      'timestamp': str,
                                      'user_id': str,
                                      'amount': float,
                                      'currency': str})

    results = []
    for row in df.itertuples(index=False):
        results.append(validate_positive(value= row.amount, row_num= row.index, column= 'amount'))
        print(type(row.amount))
        
    assert type(results[0]) == ValidationError
    assert type(results[1]) == ValidationError
    assert results[2] is None