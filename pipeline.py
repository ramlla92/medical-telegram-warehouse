# pipeline.py
from dagster import op, job
import subprocess
import logging

logger = logging.getLogger(__name__)

# ------------------------------
# OPS
# ------------------------------

@op
def scrape_telegram_data():
    """Run your scraper"""
    logger.info("Scraping Telegram data...")
    # Example: call your scraper script
    subprocess.run(["python", "src/load_metadata.py"], check=True)
    return "Scrape complete"

@op
def load_raw_to_postgres():
    """Load JSON data to Postgres"""
    logger.info("Loading raw data to Postgres...")
    subprocess.run(["python", "src/test_pg.py"], check=True)
    return "Load complete"

@op
def run_dbt_transformations():
    """Run DBT models"""
    logger.info("Running dbt transformations...")
    subprocess.run(["dbt", "run", "--project-dir", "medical_warehouse/medical_warehouse"], check=True)
    return "DBT run complete"

@op
def run_yolo_enrichment():
    """Run YOLO or image enrichment"""
    logger.info("Running YOLO enrichment...")
    # Example: call your image enrichment script
    subprocess.run(["python", "src/update_image_paths.py"], check=True)
    return "YOLO enrichment complete"

# ------------------------------
# JOB
# ------------------------------

@job
def telegram_pipeline():
    scrape_result = scrape_telegram_data()
    load_result = load_raw_to_postgres()
    dbt_result = run_dbt_transformations()
    yolo_result = run_yolo_enrichment()

    # Define dependencies
    load_result = load_raw_to_postgres()
    dbt_result = run_dbt_transformations()
    yolo_result = run_yolo_enrichment()

    # Execution order
    load_result
    dbt_result
    yolo_result
