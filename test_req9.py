import sqlite3
import pytest

@pytest.fixture(scope="module")
def db():
    # Using in-memory SQLite DB for testing
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    yield cursor
    conn.close()

def test_table_creation(db):
    db.execute("CREATE TABLE Customers (id INTEGER PRIMARY KEY, name TEXT, balance REAL)")
    db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Customers'")
    result = db.fetchone()
    assert result is not None, " Table creation failed"

def test_data_insertion(db):
    db.execute("INSERT INTO Customers (id, name, balance) VALUES (1, 'Alice', 5000.0)")
    db.execute("SELECT * FROM Customers WHERE id=1")
    result = db.fetchone()
    assert result[1] == "Alice", "Data insertion failed"

def test_duplicate_detection(db):
    try:
        db.execute("INSERT INTO Customers (id, name, balance) VALUES (1, 'Alice', 6000.0)")
        db.connection.commit()
        assert False, "Duplicate insertion allowed"
    except sqlite3.IntegrityError:
        assert True  

def test_duplicate_removal(db):
    db.execute("INSERT INTO Customers (id, name, balance) VALUES (2, 'Bob', 4000.0)")
    db.execute("INSERT INTO Customers (id, name, balance) VALUES (3, 'Bob', 4000.0)")
    db.execute("DELETE FROM Customers WHERE id=3")
    db.execute("SELECT COUNT(*) FROM Customers WHERE name='Bob'")
    result = db.fetchone()[0]
    assert result == 1, "Duplicate not removed properly"

def test_data_comparison(db):
    db.execute("SELECT balance FROM Customers WHERE id=1")
    alice_balance = db.fetchone()[0]
    db.execute("SELECT balance FROM Customers WHERE id=2")
    bob_balance = db.fetchone()[0]
    assert alice_balance > bob_balance, "Data comparison failed"

def test_output_validation(db):
    db.execute("SELECT name FROM Customers ORDER BY balance DESC LIMIT 1")
    richest_customer = db.fetchone()[0]
    assert richest_customer == "Alice", "Output validation failed"
