import pandas as pd
from sqlalchemy import create_engine

def process_brands(db_engine):
    brands_file_path = 'data/brands.json'
    brands_df = pd.read_json(brands_file_path, lines=True)

    # Ensure `barcode` is of string type
    brands_df['barcode'] = brands_df['barcode'].astype(str)

    # Extract and flatten the `_id` field, then rename it to `brands_id`
    brands_df['_id'] = brands_df['_id'].apply(lambda x: x.get('$oid', None) if isinstance(x, dict) else None)
    brands_df.rename(columns={'_id': 'brands_id'}, inplace=True)

    # Drop the original nested `cpg` column if no longer needed
    if 'cpg' in brands_df.columns:
        brands_df = brands_df.drop(columns=['cpg'])

    # Removing Test Cases
    brands_df = brands_df[~brands_df['brandCode'].str.contains('test', case=False, na=False)]
    brands_df = brands_df[~brands_df['name'].str.contains('test', case=False, na=False)]

    # Remove rows where barcode is equal to brandCode
    brands_df = brands_df[~(brands_df['barcode'] == brands_df['brandCode'])]

    # Remove rows where 'brandCode' is NULL or whitespace
    brands_df = brands_df[
        brands_df['brandCode'].notna() &
        (brands_df['brandCode'].str.strip() != "")
    ]

    table_name = 'brands'
    brands_df.to_sql(table_name, db_engine, if_exists='replace', index=False)
    print(f"'{table_name}' table loaded to SQL.")

if __name__ == "__main__":
    engine = create_engine('sqlite:///fetch_rewards.db')
    process_brands(engine)