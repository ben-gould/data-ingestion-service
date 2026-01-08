import csv
from pathlib import Path 
from ingestion.models import Transaction 

def load_transactions(csv_path: Path):
    transactions = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader: 
            transaction = Transaction(**row)
            transactions.append(transaction)
    return transactions 

if __name__ == "__main__":
    data_path = Path("data/sample.csv")
    txns = load_transactions(data_path)
    print(f"Loaded {len(txns)} transactions")