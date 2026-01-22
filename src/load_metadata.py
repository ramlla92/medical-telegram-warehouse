import os
import json
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# PostgreSQL connection
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
db = os.getenv("POSTGRES_DB")

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# Path to raw telegram messages
RAW_DATA_PATH = Path("data/raw/telegram_messages")

def load_json_file(file_path):
    """Load a JSON file and return a list of messages."""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def insert_messages(messages):
    """Insert a list of messages into the database."""
    with engine.begin() as conn:  # begin() ensures commit
        for msg in messages:
            # Ensure message_date is a timestamp
            msg_date = msg.get("message_date")
            if msg_date:
                try:
                    msg_date = datetime.fromisoformat(msg_date)
                except ValueError:
                    msg_date = None

            conn.execute(
                text("""
                    INSERT INTO raw.telegram_messages
                    (message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards)
                    VALUES (:message_id, :channel_name, :message_date, :message_text, :has_media, :image_path, :views, :forwards)
                    ON CONFLICT (message_id) DO NOTHING
                """),
                {
                    "message_id": msg.get("message_id"),
                    "channel_name": msg.get("channel_name"),
                    "message_date": msg_date,
                    "message_text": msg.get("message_text"),
                    "has_media": msg.get("has_media", False),
                    "image_path": msg.get("image_path"),
                    "views": msg.get("views"),
                    "forwards": msg.get("forwards")
                }
            )

def main():
    """Main function to load all JSON files from data lake."""
    all_files = list(RAW_DATA_PATH.rglob("*.json"))
    print(f"Found {len(all_files)} JSON files.")

    for file in all_files:
        print(f"Loading {file}...")
        messages = load_json_file(file)
        insert_messages(messages)
    print("All messages loaded successfully.")

if __name__ == "__main__":
    main()
