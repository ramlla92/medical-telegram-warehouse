import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

CSV_PATH = "data/raw/yolo_detections.csv"

engine = create_engine(DB_URL)

# Load CSV
df = pd.read_csv(CSV_PATH)

print(f"Loaded {len(df)} YOLO detection rows")

with engine.begin() as conn:
    # Ensure schema exists
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

    # Replace table each run (safe for experimentation)
    df.to_sql(
        "yolo_detections",
        conn,
        schema="raw",
        if_exists="replace",
        index=False
    )

print("YOLO detections loaded into Postgres (raw.yolo_detections)")
