# src/classify_images.py
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"  # e.g., postgres
    f"{os.getenv('POSTGRES_PASSWORD')}@"            # your password
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"   # default localhost
    f"{os.getenv('POSTGRES_PORT', '5432')}/"       # default 5432
    f"{os.getenv('POSTGRES_DB')}"                  # your database
)

engine = create_engine(DB_URL)

# Load YOLO detections from Postgres
query = """
SELECT message_id, channel_name, detected_class
FROM raw.yolo_detections
"""
df = pd.read_sql(query, engine)

# Make sure detected_class is a list
df['detected_class'] = df['detected_class'].apply(lambda x: x.split(', ') if isinstance(x, str) else [])

# Group detections per image
grouped = df.groupby(['message_id', 'channel_name'])['detected_class'].apply(lambda x: sum(x, [])).reset_index()

# Classification logic
def classify(objects):
    has_person = 'person' in objects
    has_product = any(obj in ['bottle', 'box', 'medicine'] for obj in objects)

    if has_person and has_product:
        return 'promotional'
    elif has_product:
        return 'product_display'
    elif has_person:
        return 'lifestyle'
    else:
        return 'other'

grouped['image_category'] = grouped['detected_class'].apply(classify)

# Save classification results to Postgres
with engine.begin() as conn:
    grouped[['message_id', 'channel_name', 'image_category']].to_sql(
        'image_classification',
        conn,
        schema='raw',
        if_exists='replace',
        index=False
    )

print(" Image classification completed and saved to raw.image_classification")
