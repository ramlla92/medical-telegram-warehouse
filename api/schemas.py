# api/schemas.py
from pydantic import BaseModel
from typing import List

class TopProduct(BaseModel):
    term: str
    frequency: int

class ChannelActivity(BaseModel):
    date: str
    messages_count: int

class MessageResult(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    views: int
    forwards: int

class VisualContentStats(BaseModel):
    channel_name: str
    images_count: int
