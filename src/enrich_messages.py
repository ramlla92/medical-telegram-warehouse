# src/enrich_messages.py
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

engine = create_engine(DB_URL)

# Load messages
messages = pd.read_sql("SELECT message_id, channel_name, message_text, message_date, views FROM raw.telegram_messages", engine)

# Load YOLO detections
detections = pd.read_sql("SELECT message_id, channel_name, detected_class, confidence_score FROM raw.yolo_detections", engine)

# Load image categories
categories = pd.read_sql("SELECT message_id, channel_name, image_category FROM raw.image_classification", engine)

# Merge YOLO detections with categories
enriched = pd.merge(
    detections,
    categories,
    on=['message_id', 'channel_name'],
    how='left'
)

# Merge with messages
enriched = pd.merge(
    enriched,
    messages,
    on=['message_id', 'channel_name'],
    how='left'
)

# Save enriched table to Postgres (for dbt staging)
with engine.begin() as conn:
    enriched.to_sql(
        'enriched_messages',
        conn,
        schema='raw',
        if_exists='replace',
        index=False
    )

print(" Enriched messages saved to raw.enriched_messages")
