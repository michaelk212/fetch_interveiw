from sqlalchemy import create_engine
from etl_brands import process_brands
from etl_users import process_users
from etl_receipts import process_receipts
from etl_items import process_items
from initialize_db import initialize_database

def run_pipeline():
    db_path = "fetch_rewards.db"
    engine = initialize_database(db_path)
    process_brands(engine)
    process_users(engine)
    process_receipts(engine)
    process_items(engine)

if __name__ == "__main__":
    run_pipeline()