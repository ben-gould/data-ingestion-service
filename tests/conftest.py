import json
import random
from datetime import datetime, timedelta

def generate_transactions(n=500, error_rate=0.1):
    """Generate n transactions with ~error_rate fraction having errors."""
    transactions = []
    base_date = datetime(2023, 1, 1)
    errors = ['negative_amount', 'null_amount', 'bad_currency', 'future_date', 'string_amount']

    for i in range(n):
        # assign values assuming valid
        transaction_id = f"tx{i:05d}"
        delta = random.randint(0,365)
        timestamp = base_date - timedelta(days = delta)
        user_id = f"user{random.randint(1,n)}"
        amount = random.uniform(1,250)
        currency = random.choice(['USD', 'EUR', 'JPY', 'RMB'])

        # overwrite value with invalid entry if there's an error
        rand = random.random()
        has_error = (rand < error_rate)

        if has_error:
            error = random.choice(errors)

            if error == 'negative_amount':
                amount = -random.uniform(1,250)
            elif error == 'null_amount':
                amount = None
            elif error == 'bad_currency':
                currency = 'INVALID'
            elif error == 'future_date':
                timestamp = base_date + timedelta(days = delta)
            elif error == 'string_amount':
                amount = str(random.uniform(1,250))

        transaction = {
            'transaction_id' : transaction_id,
            'timestamp' : timestamp.isoformat(),
            'user_id' : user_id,
            'amount' : amount,
            'currency' : currency
        } 
        transactions.append(transaction)

    return transactions