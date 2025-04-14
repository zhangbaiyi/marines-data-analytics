import os
import sqlite3

from src.utils.logging import LOGGER

SRC_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(__file__))
)  # goes from /src/scripts/data_warehouse -> /marines-data-analytics
DB_PATH = os.path.join(SRC_ROOT, "..", "db", "database.sqlite3")
SQL_FILE = os.path.join(SRC_ROOT, "scripts", "data_warehouse", "db_setup.sql")


def initialize_database():
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        if not os.path.exists(SQL_FILE):
            raise FileNotFoundError(f"SQL file '{SQL_FILE}' not found.")

        if not os.path.exists(DB_PATH):
            LOGGER.info(f"Creating SQLite DB at {DB_PATH}")
            with sqlite3.connect(DB_PATH) as conn:
                with open(SQL_FILE, "r") as f:
                    conn.executescript(f.read())
            LOGGER.info("Database initialized.")
        else:
            LOGGER.info("Database already exists. Skipping initialization.")
    except Exception as e:
        LOGGER.info(f"Error during DB init: {e}")
        exit(1)


if __name__ == "__main__":
    initialize_database()
