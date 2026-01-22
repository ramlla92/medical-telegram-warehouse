import os
import json
import asyncio
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from loguru import logger

from channels import CHANNELS

# Load environment variables
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "telegram_session")

BASE_DATA_PATH = Path("data/raw/telegram_messages")
LOG_PATH = Path("logs")

LOG_PATH.mkdir(exist_ok=True, parents=True)
logger.add(LOG_PATH / "scraper_metadata.log", rotation="1 MB")


async def main():
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:

        today = datetime.utcnow().strftime("%Y-%m-%d")
        output_dir = BASE_DATA_PATH / today
        output_dir.mkdir(parents=True, exist_ok=True)

        for channel in CHANNELS:
            logger.info(f"Scraping metadata from {channel}")
            messages = []

            async for msg in client.iter_messages(channel, limit=200):
                messages.append({
                    "message_id": msg.id,
                    "channel_name": channel,
                    "message_date": msg.date.isoformat() if msg.date else None,
                    "message_text": msg.text,
                    "has_media": bool(msg.media),
                    "image_path": None,  # Filled by image scraper
                    "views": msg.views,
                    "forwards": msg.forwards
                })

            file_path = output_dir / f"{channel}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)

            logger.success(f"{channel}: {len(messages)} messages saved")


if __name__ == "__main__":
    asyncio.run(main())
