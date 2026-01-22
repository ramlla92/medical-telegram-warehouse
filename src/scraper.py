import os
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient
from loguru import logger

# Load environment variables from .env
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "telegram_session")

# Initialize logger
logger.add("logs/scraper_base.log", rotation="1 MB")

async def main():
    try:
        async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
            me = await client.get_me()
            logger.success(f"Connected successfully as {me.username or me.first_name}")
            print(f"Connected successfully as {me.username or me.first_name}")

            # Optional: list your dialogs (channels/chats you can access)
            async for dialog in client.iter_dialogs(limit=10):
                print(f"{dialog.name} | ID: {dialog.id}")

    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        print(f"Failed to connect: {e}")

if __name__ == "__main__":
    asyncio.run(main())
