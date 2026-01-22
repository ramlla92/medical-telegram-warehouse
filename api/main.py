# api/main.py
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from . import db, schemas, crud

app = FastAPI(title="Telegram Analytics API")

# Dependency
def get_db():
    db_session = db.SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()

@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def top_products(limit: int = 10, db: Session = Depends(get_db)):
    return crud.get_top_products(db, limit)

@app.get("/api/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivity])
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    result = crud.get_channel_activity(db, channel_name)
    if not result:
        raise HTTPException(status_code=404, detail="Channel not found or no activity")
    return result

@app.get("/api/search/messages", response_model=List[schemas.MessageResult])
def search_messages(query: str, limit: int = 20, db: Session = Depends(get_db)):
    return crud.search_messages(db, query, limit)

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualContentStats])
def visual_content_stats(db: Session = Depends(get_db)):
    return crud.get_visual_content_stats(db)
