from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()  # Load .env variables

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
db = os.getenv("POSTGRES_DB")

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# Test connection
with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print(result.fetchone())
