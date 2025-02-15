""" consumer_hansen.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
import sqlite3
import json
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.animation as animation

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger


#####################################
# Define File Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FILE = PROJECT_ROOT.joinpath("data", "project_live.json")
DB_PATH = PROJECT_ROOT.joinpath("project_db.sqlite")


#####################################
# Define Function to Initialize SQLite Database
#####################################


def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database -
    if it doesn't exist, create the 'streamed_messages' table
    and if it does, recreate it.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.

    """
    logger.info("Calling SQLite init_db() with {db_path=}.")
    try:
        # Ensure the directories for the db exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER
                )
            """
            )
            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")


#####################################
# Function to process latest message
# #####################################

def read_message():
    """
    Process a JSON message from a file.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        with open(DATA_FILE, "r") as file:
            data = json.load(file)
            if isinstance(data, list) and len(data) > 0:
                return data[-1]  # Return the latest message
    except (json.JSONDecodeError, FileNotFoundError):
        return None
    return None

#####################################
# Define Function to Insert a Processed Message into the Database
#####################################

def process_message(message: dict):
    """
    Processes each message into the SQLite database.

    Args:
    - message (dict): Processed message to insert.
    """

    logger.info(f"Calling SQLite insert_message() with: {message=}")
    logger.info(f"Database path: {DB_PATH}")

    # Extract message details
    sentiment = message.get("sentiment", 0)
    category = message.get("category", "other")
    timestamp = message.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    author = message.get("author", "Unknown")
    text = message.get("message", "")
    keyword_mentioned = message.get("keyword_mentioned", None)
    message_length = len(text)

    STR_PATH = str(DB_PATH)
    
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment, keyword_mentioned, message_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    text,
                    author,
                    timestamp,
                    category,
                    sentiment,
                    keyword_mentioned,
                    message_length,
                ),
            )
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")

# Example usage
if __name__ == "__main__":
    # Initialize the database
    init_db(DB_PATH)
    
    # Process the latest message
    message = read_message()
    if message:
        process_message(message)