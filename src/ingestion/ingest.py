import csv
from pathlib import Path 
from ingestion.models import Transaction 
import logging
from pydantic import ValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)


def load_transactions(csv_path: Path):
    transactions = []
    with csv_path.open() as f:
        logger.info("Starting ingestion from %s", csv_path)
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start = 1): 
            failure_count = 0
            try:
                transaction = Transaction(**row)
                transactions.append(transaction)
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
    data_path = Path("data/sample.csv")
    txns = load_transactions(data_path)
    logger.info("Loaded %s transactions", len(txns))