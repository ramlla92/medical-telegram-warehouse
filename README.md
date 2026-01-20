# Medical Telegram Data Warehouse

This project builds an end-to-end ELT data platform that extracts data from Ethiopian medical Telegram channels, stores raw data in a data lake, transforms it into a PostgreSQL data warehouse using dbt, enriches image data with YOLO object detection, and exposes insights through a FastAPI analytical API.

## Tech Stack
- Python
- Telethon (Telegram API)
- PostgreSQL
- dbt
- FastAPI
- Dagster
- YOLOv8

## High-Level Architecture
Telegram → Data Lake → PostgreSQL → dbt → Analytics → API
