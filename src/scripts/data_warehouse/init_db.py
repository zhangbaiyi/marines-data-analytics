import sqlite3
import os
from src.utils.logging import LOGGER

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # goes from /src/scripts/data_warehouse -> /marines-data-analytics
DB_PATH = os.path.join(PROJECT_ROOT, "db", "database.sqlite3")
SQL_FILE = os.path.join(PROJECT_ROOT, "db_setup.sql")

def initialize_database():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    if not os.path.exists(DB_PATH):
        LOGGER.info("Creating new SQLite database and initializing schema...")
        with sqlite3.connect(DB_PATH) as conn:
            with open(SQL_FILE, "r") as f:
                conn.executescript(f.read())
        LOGGER.info("Database initialized.")
    else:
        LOGGER.info("Database already exists. Skipping initialization.")

if __name__ == "__main__":
    initialize_database()
