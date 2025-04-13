import sqlite3
import os
from src.utils.logging import LOGGER


DB_PATH = "db/database.sqlite"
SQL_FILE = "db_setup.sql"

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
