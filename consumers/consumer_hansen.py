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
from collections import defaultdict
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

# Initialize a dictionary to store year, population, year counts, and average population
message_count = defaultdict(int)    # for storing message count by author
sentiment_avg = defaultdict(int)    # for average sentiment

count_messages = []
average_sentiment = []


#####################################
# Set up live visuals
#####################################
# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()


def update_visualization(json_file):
    """
    Create a bar chart visualization from the JSON file.

    Args:
    - json_file (pathlib.Path): Path to the JSON file.
    """
    logger.info(f"Creating visualization from {json_file}")
    
    import json
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_visualization(json_file):
    """
    Create a bar chart visualization from the JSON file.

    Args:
    - json_file (pathlib.Path): Path to the JSON file.
    """
    logger.info(f"Creating visualization from {json_file}")
    
    try:
        plt.ion()  # Turn on interactive mode
        fig, ax = plt.subplots()
        
        while True:
            with open(json_file, "r") as file:
                data = [json.loads(line) for line in file]
                
                # Initialize message_count and sentiments
                message_count = defaultdict(int)
                sentiments = defaultdict(list)
                
                for msg in data:
                    author = msg.get('author', 'Unknown')
                    message_count[author] += 1
                    sentiment = msg.get('sentiment', 0)
                    sentiments[author].append(sentiment)
                
                average_sentiment = {author: sum(sentiments[author])/count 
                                     for author, count in message_count.items()}
                
                # Update the plot
                ax.clear()
                authors = list(message_count.keys())
                avg_sentiments = [average_sentiment[author] for author in authors]
                
                ax.barh(authors, avg_sentiments, color='blue')
                ax.set_xlabel('Sentiment')
                ax.set_ylabel('Authors')
                ax.set_title('Sentiment Analysis of Messages')
                
                plt.draw()
                plt.pause(2)
                plt.gcf().canvas.flush_events()
            
            logger.info("Visualization updated successfully.")
    except Exception as e:
        logger.error(f"ERROR: Failed to create visualization: {e}")

#####################################
# Define Function to Initialize SQLite Database
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database - if it doesn't exist, create the 'streamed_messages' table
    and if it does, recreate it.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info(f"Calling SQLite init_db() with {db_path=}.")
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
#####################################

def read_message():
    """
    Process a JSON message from a file.
    """
    logger.info(f"Reading messages from {DATA_FILE}")
    try:
        with open(DATA_FILE, "r") as file:
            data = json.load(file)
            logger.info(f"Read data: {data}")

            if isinstance(data, dict):
                return data  # Return the message if it's a single dictionary
            elif isinstance(data, list) and len(data) > 0:
                return data[-1]  # Return the latest message if it's a list
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"Error reading message: {e}")
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
        logger.info(f"Message to be processed: {message}")
        process_message(message)
    else:
        logger.error("No message found to process.")

    update_visualization(DATA_FILE)
