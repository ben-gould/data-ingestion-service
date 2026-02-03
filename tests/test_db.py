import sqlite3
from ingestion.db import count_transactions, init_db, insert_transaction, transaction_exists

def test_init_db_creates_table(tmp_path):
    # verify table exists after init
    test_db_path = tmp_path / "test.db"
    init_db(test_db_path)

    with sqlite3.connect(test_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='transactions'
            """
        )
        result = cursor.fetchone()
    
    assert result is not None 
    assert result[0] == 'transactions'

def test_transaction_exists_returns_true_for_existing(tmp_path):
    # insert one, check that it exists
    test_db_path = tmp_path / "test.db"
    init_db(test_db_path)

    insert_transaction(test_db_path, 
                       {'transaction_id': 'tx001',
                        'timestamp': '2023-01-01T10:00:00',
                        'user_id': 'user1',
                        'amount':12.50,
                        'currency': 'USD'})
    
    transactionexists = transaction_exists(test_db_path, 'tx001')

    assert transactionexists

def test_transaction_exists_returns_false_for_new(tmp_path):
    # check non-existent ID returns False

    test_db_path = tmp_path / "test.db"
    init_db(test_db_path)

    insert_transaction(test_db_path, 
                       {'transaction_id': 'tx001',
                        'timestamp': '2023-01-01T10:00:00',
                        'user_id': 'user1',
                        'amount':12.50,
                        'currency': 'USD'})
    
    transactionexists = transaction_exists(test_db_path, 'tx002')

    assert not transactionexists

def test_upsert_updates_existing_transaction(tmp_path):
    # insert same ID twice, verify UPDATE happened (count still 1, update 1, values changed)

    test_db_path = tmp_path / "test.db"
    init_db(test_db_path)
    
    insert_transaction(
        test_db_path,
        {'transaction_id': 'tx001',
         'timestamp': '2023-01-01T10:00:00',
         'user_id': 'user1',
         'amount':12.50,
         'currency': 'USD'}
    )

    insert_transaction(
        test_db_path,
        {'transaction_id': 'tx001',
         'timestamp': '2023-01-01T10:00:00',
         'user_id': 'user1',
         'amount':13.50,
         'currency': 'USD'}
    )

    assert count_transactions(test_db_path) == 1

    with sqlite3.connect(test_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''SELECT amount FROM transactions WHERE transaction_id=?''',
            ('tx001',)
        )
        amount = cursor.fetchone()[0]

    assert amount == 13.50


# def test_upsert_inserts_new_transaction():
    # verify fresh insert works
