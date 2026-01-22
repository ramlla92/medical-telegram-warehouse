# api/crud.py
from sqlalchemy.orm import Session

def get_top_products(db: Session, limit: int = 10):
    result = db.execute("""
        SELECT message_text AS term, COUNT(*) AS frequency
        FROM raw.fct_messages
        GROUP BY message_text
        ORDER BY frequency DESC
        LIMIT :limit
    """, {"limit": limit})
    return [{"term": row[0], "frequency": row[1]} for row in result.fetchall()]

def get_channel_activity(db: Session, channel_name: str):
    result = db.execute("""
        SELECT s.message_date::date AS date, COUNT(*) AS messages_count
        FROM raw.fct_messages s
        WHERE s.channel_name = :channel_name
        GROUP BY s.message_date::date
        ORDER BY date
    """, {"channel_name": channel_name})
    return [{"date": str(row[0]), "messages_count": row[1]} for row in result.fetchall()]

def search_messages(db: Session, query: str, limit: int = 20):
    result = db.execute("""
        SELECT message_id, channel_name, message_text, views, forwards
        FROM raw.fct_messages
        WHERE message_text ILIKE :query
        ORDER BY message_date DESC
        LIMIT :limit
    """, {"query": f"%{query}%", "limit": limit})
    return [
        {
            "message_id": row[0],
            "channel_name": row[1],
            "message_text": row[2],
            "views": row[3],
            "forwards": row[4]
        } for row in result.fetchall()
    ]

def get_visual_content_stats(db: Session):
    result = db.execute("""
        SELECT f.channel_name, COUNT(DISTINCT f.message_id) AS images_count
        FROM raw.fct_image_detections f
        GROUP BY f.channel_name
        ORDER BY images_count DESC
    """)
    return [{"channel_name": row[0], "images_count": row[1]} for row in result.fetchall()]
