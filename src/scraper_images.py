import os
import asyncio
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from loguru import logger

from channels import CHANNELS

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "telegram_session")

IMAGE_BASE_PATH = Path("data/raw/images")
LOG_PATH = Path("logs")

LOG_PATH.mkdir(exist_ok=True, parents=True)
logger.add(LOG_PATH / "scraper_images.log", rotation="1 MB")


async def main():
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:

        for channel in CHANNELS:
            logger.info(f"Downloading images from {channel}")
            channel_dir = IMAGE_BASE_PATH / channel
            channel_dir.mkdir(parents=True, exist_ok=True)

            async for msg in client.iter_messages(channel, limit=200):
                if isinstance(msg.media, MessageMediaPhoto):
                    image_path = channel_dir / f"{msg.id}.jpg"
                    await client.download_media(msg.media, image_path)

            logger.success(f"Images downloaded for {channel}")


if __name__ == "__main__":
    asyncio.run(main())
