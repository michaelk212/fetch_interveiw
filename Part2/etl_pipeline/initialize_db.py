from sqlalchemy import create_engine


### We create this script to set up the SQLite databse that we will use to answer the queries

def initialize_database(db_path):
    engine = create_engine(f'sqlite:///{db_path}')
    with engine.connect() as conn:
        print("Database initialized.")
    return engine

if __name__ == "__main__":
    db_path = "fetch_rewards.db"
    initialize_database(db_path)


