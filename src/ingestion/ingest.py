import csv
import pandas as pd
from pathlib import Path 
from ingestion.models import Transaction 
import logging
from pydantic import ValidationError
from ingestion.db import init_db
from ingestion.db import insert_transaction
from ingestion.validation import DataValidator, validate_not_null, validate_positive, validate_date_not_future

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)

db_path = Path("data.db")
if db_path.exists():
       db_path.unlink()
init_db(db_path)
logger.info("The database %s is ready", db_path)


def load_transactions(csv_path: Path, db_path: Path):
    df = pd.read_csv(csv_path, dtype={
         'transaction_id': str,
    'timestamp': str,
    'user_id': str,
    'amount': str, 
    'currency': str
    }) 
    logger.info(f"Read in csv with {len(df)} rows from {csv_path}")

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
        logger.info("All rows passed validation")

    transactions = []
    for _, row in valid_df.iterrows(): 
        transaction = Transaction(**row)
        transactions.append(transaction)
        insert_transaction(db_path, row.to_dict()) 
    return transactions 


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    data_path = Path("/Users/bengould/Documents/Projects/data-ingestion-service/data/sample.csv")
    txns = load_transactions(data_path, Path("data.db"))
    logger.info("Loaded %s transactions", len(txns))