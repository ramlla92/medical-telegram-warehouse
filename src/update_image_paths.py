# src/update_image_paths.py
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Connect to PostgreSQL
engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Path where images are stored
IMAGE_PATH = Path("data/raw/images")

# Loop through folders (channel_name) and images
for channel_folder in IMAGE_PATH.iterdir():
    if not channel_folder.is_dir():
        continue
    for img_file in channel_folder.iterdir():
        if not img_file.is_file():
            continue
        # Assume filename is message_id.jpg
        message_id = int(img_file.stem)
        path_str = str(img_file)

        # Update the PostgreSQL table
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE raw.telegram_messages
                    SET image_path = :path, has_media = TRUE
                    WHERE message_id = :message_id
                """),
                {"path": path_str, "message_id": message_id}
            )
        print(f"Updated message {message_id} for channel {channel_folder.name}")

print(" All image paths updated successfully!")
