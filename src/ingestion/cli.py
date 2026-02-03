import argparse
from pathlib import Path
from ingestion.db import init_db
from ingestion.ingest import load_transactions
from ingestion.config import load_config
import logging 

def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    parser = argparse.ArgumentParser(description="Data ingestion pipeline",
                                     epilog="Example: python -m ingestion --input data.csv --db output.db")
    parser.add_argument('--input', type=Path, required=True, help='Input file (CSV/JSON)')
    parser.add_argument('--db', type=Path, help='Database path')
    parser.add_argument('--config', type=Path, default='config.yaml', help='Config file')

    args = parser.parse_args()
    config = load_config(args.config)
    db_path = args.db or Path(config['database']['path'])
    init_db(db_path)

    _, result_dict = load_transactions(args.input, db_path)
    print(f"Arguments loaded to load_transactions: {args.input}")

    print(f"Processed {result_dict['total_rows']} rows")
    print(f"Inserted {result_dict['valid_rows']} rows")
    print(f"Rejected {result_dict['invalid_rows']} rows")

if __name__ == "__main__":
    main()