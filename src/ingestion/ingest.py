import csv
from pathlib import Path 
from ingestion.models import Transaction 
import logging
from pydantic import ValidationError
from ingestion.db import init_db
from ingestion.db import insert_transaction

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
    transactions = []
    with csv_path.open() as f:
        logger.info("Starting ingestion from %s", csv_path)
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start = 1): 
            failure_count = 0
            try:
                transaction = Transaction(**row)
                transactions.append(transaction)
                insert_transaction(db_path, transaction)
            except ValidationError as e:
                 logger.warning("Invalid transaction row: %s", e)
                 logger.warning("Skipping row %d due to validation error: %s",
                i,
                e
                )  
                 failure_count += 1

        logger.info("Loaded %s rows successfully from csv, with %s failures", len(transactions), failure_count)
    return transactions 

if __name__ == "__main__":
    data_path = Path("/Users/bengould/Documents/Projects/data-ingestion-service/data/sample.csv")
    txns = load_transactions(data_path, Path("data.db"))
    logger.info("Loaded %s transactions", len(txns))