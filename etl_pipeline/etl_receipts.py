import pandas as pd
from sqlalchemy import create_engine

def process_receipts(db_engine):
    receipts_file_path = "data/receipts.json"
    receipts_df = pd.read_json(receipts_file_path, lines=True)

    date_fields = ['createDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate', 'purchaseDate']
    for field in date_fields:
        receipts_df[field] = receipts_df[field].apply(
            lambda x: pd.to_datetime(x.get('$date', None), unit='ms') if isinstance(x, dict) else None
        )

    receipts_df['_id'] = receipts_df['_id'].apply(lambda x: x.get('$oid', None) if isinstance(x, dict) else None)
    receipts_df.rename(columns={'_id': 'receipt_id'}, inplace=True)

    if 'rewardsReceiptItemList' in receipts_df.columns:
        receipts_df = receipts_df.drop(columns=['rewardsReceiptItemList'])

    receipts_df = receipts_df.drop_duplicates()

    table_name = 'receipts'
    receipts_df.to_sql(table_name, db_engine, if_exists='replace', index=False)
    print(f"'{table_name}' table loaded to SQL.")

if __name__ == "__main__":
    engine = create_engine('sqlite:///fetch_rewards.db')
    process_receipts(engine)