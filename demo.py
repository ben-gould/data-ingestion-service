import pandas as pd
from pathlib import Path 
from ingestion.config import load_config
from ingestion.db import init_db
from tests.conftest import generate_transactions
from ingestion.ingest import load_transactions

"""
Demo script: runs the full ingestion pipeline on sample data.
Generates a quality report showing validation in action.

Usage: uv run python demo.py
"""

print("=" * 60)
print("Data Ingestion Pipeline Demo")
print("=" * 60)

config = load_config()
db_path = Path(config['database']['path'])

if db_path.exists():
    db_path.unlink()  # Delete old database
    print("   ✓ Cleared old database")
init_db(db_path)

# 1. Generate synthetic data (use your generator)
print("\n1. Generating 500 transactions (10% will have validation errors)...")
data = generate_transactions()
df = pd.DataFrame(data)

# 2. Save to data/demo_input.csv and data/demo_input.json
df.to_csv('data/demo_input.csv')
df.to_json('data/demo_input.json')
print("   ✓ Saved to data/demo_input.csv and data/demo_input.json")

# 3. Run ingestion on both
print("\n2. Ingesting CSV file...")
_, csv_result = load_transactions('data/demo_input.csv', db_path)
print(f"   ✓ Total rows: {csv_result['total_rows']}")
print(f"   ✓ Valid rows inserted: {csv_result['inserted_rows']}")
print(f"   ✗ Invalid rows rejected: {csv_result['invalid_rows']}")

print("\n3. Ingesting JSON file...")
_, json_result = load_transactions('data/demo_input.json', db_path)
print(f"   ✓ Updated rows: {json_result['updated_rows']}")

# 4. Print summary results
print(csv_result)
print(json_result)

# 5. Tell user where to find the HTML report
print("\n" + "=" * 60)
print("Demo Complete!")
print("=" * 60)
print(f"\n📊 View quality reports:")
print(f"   CSV:  {csv_result['report_path']}")
print(f"   JSON: {json_result['report_path']}")