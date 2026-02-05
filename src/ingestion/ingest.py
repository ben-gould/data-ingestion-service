import csv
import yaml
import pandas as pd
from pathlib import Path 
from ingestion.models import Transaction 
import logging
from ingestion.db import init_db
from ingestion.db import insert_transaction
from ingestion.validation import DataValidator, validate_not_null, validate_positive, validate_date_not_future
from ingestion.config import load_config
#from ingestion.reporting import generate_quality_report
from ingestion.db import transaction_exists
from ingestion.reporting import generate_quality_report

logger = logging.getLogger(__name__)

def detect_file_format(filepath: Path) -> str:
        if str(filepath).endswith('.csv'):
            return "csv"
        elif str(filepath).endswith('json'):
            return "json"
        else:
            raise ValueError(f"Data at {filepath} to be ingested is not a supported type (csv or JSON)")
     

def read_file(filepath: Path) -> pd.DataFrame:
    filetype = detect_file_format(filepath)
    if filetype == "csv":
        df = pd.read_csv(filepath, dtype={
                        'transaction_id': str,
                        'timestamp': str,
                        'user_id': str,
                        'amount': str, 
                        'currency': str
                        })
    elif filetype == "json":
        df = pd.read_json(filepath, orient= 'records')
    return df

def load_transactions(file_path: Path, db_path: Path, output_dir=Path('reports')):
    filetype = detect_file_format(file_path)
    df = read_file(file_path)

    logger.info(f"Read in csv with {len(df)} rows from {file_path}. Filetype is {filetype}.")

    validator = DataValidator()
    validator.add_rule('amount', validate_not_null)
    validator.add_rule('amount', validate_positive)
    validator.add_rule('timestamp', validate_not_null)
    validator.add_rule('timestamp', validate_date_not_future)
    validator.add_rule('user_id', validate_not_null)

    validation_result = validator.validate(df)
    if not validation_result.is_valid:
        logger.warning(f"Found {len(validation_result.errors)} validation errors")

        invalid_row_numbers = {err.row_number for err in validation_result.errors}
        valid_df = df[~df.index.isin(invalid_row_numbers)]
        invalid_df = df[df.index.isin(invalid_row_numbers)]

        logger.info(f"Logging {len(valid_df)} rows and rejecting {len(invalid_df)} rows")
        for error in validation_result.errors:
             logger.info(f"The error for row {error.row_number} is {error.error_message}")
    
    else:
        valid_df = df
        invalid_df = pd.DataFrame()
        logger.info("All rows passed validation")

    transactions = []
    success_count = 0
    updated_count = 0
    for i, row in valid_df.iterrows(): 
        transaction = Transaction(**row)
        transactions.append(transaction)
        try:
            if transaction_exists(db_path, row['transaction_id']):
                insert_transaction(db_path, row.to_dict()) 
                updated_count +=1
            else:
                insert_transaction(db_path, row.to_dict())
                success_count += 1
        except Exception as e:
             logger.warning(f"Failed to insert validated row {i}, got error: {e}")

    transactions_dict = {
         "total_rows": len(df),
         "valid_rows" : len(valid_df),
         "invalid_rows" : len(invalid_df),
         "inserted_rows": success_count,
         "updated_rows" : updated_count
    }

    report_path = generate_quality_report(df, validation_result, transactions_dict)

    transactions_dict['report_path'] = report_path

    return transactions, transactions_dict