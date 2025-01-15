import pandas as pd
from sqlalchemy import create_engine

def process_users(db_engine):
    users_file_path = 'data/users.json'
    users_df = pd.read_json(users_file_path, lines=True)

    users_df['_id'] = users_df['_id'].apply(lambda x: x.get('$oid', None) if isinstance(x, dict) else None)
    users_df.rename(columns={'_id': 'user_id'}, inplace=True)

    date_fields = ['createdDate', 'lastLogin']
    for field in date_fields:
        users_df[field] = users_df[field].apply(
            lambda x: pd.to_datetime(x.get('$date', None), unit='ms') if isinstance(x, dict) else None
        )

    users_df = users_df.drop_duplicates()

    table_name = 'users'
    users_df.to_sql(table_name, db_engine, if_exists='replace', index=False)
    print(f"'{table_name}' table loaded to SQL.")

if __name__ == "__main__":
    engine = create_engine('sqlite:///fetch_rewards.db')
    process_users(engine)