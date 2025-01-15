from fuzzywuzzy import process
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def process_items(db_engine):
    receipts_file_path = "data/receipts.json"
    receipts_df = pd.read_json(receipts_file_path, lines=True)

    # Function to flatten nested `rewardsReceiptItemList` field
    def flatten_nested_field_to_rows(df, nested_field, id_column):
        flattened_rows = []
        for _, row in df.iterrows():
            if isinstance(row[nested_field], list):
                for item in row[nested_field]:
                    flattened_row = {id_column: row['_id'].get('$oid', None) if isinstance(row['_id'], dict) else None}
                    flattened_row.update(item)
                    flattened_rows.append(flattened_row)
        return pd.DataFrame(flattened_rows)

    # Flatten the rewardsReceiptItemList field
    receipt_items_flattened = flatten_nested_field_to_rows(receipts_df, 'rewardsReceiptItemList', 'receipt_id')

    # Select necessary columns
    receipt_items_flattened = receipt_items_flattened[['receipt_id', 'barcode', 'brandCode', 'description', 'finalPrice', 'quantityPurchased']]

    # Replace "nan", empty strings, and whitespace with proper NaN
    receipt_items_flattened['brandCode'] = receipt_items_flattened['brandCode'].replace(['nan', ''], np.nan).str.strip()
    receipt_items_flattened['barcode'] = receipt_items_flattened['barcode'].replace(['nan', ''], np.nan).str.strip()

    # Filter for rows where both brandCode and barcode are not null
    receipt_items_flattened = receipt_items_flattened[~(receipt_items_flattened['brandCode'].isnull() & receipt_items_flattened['barcode'].isnull())]

    # Drop rows with specific invalid barcodes
    receipt_items_flattened = receipt_items_flattened[receipt_items_flattened['barcode'].ne('4011')]
    receipt_items_flattened = receipt_items_flattened[receipt_items_flattened['barcode'].ne('1234')]

    # Drop rows with invalid descriptions
    receipt_items_flattened = receipt_items_flattened[~receipt_items_flattened['description'].isin(['ITEM NOT FOUND', 'DELETED ITEM'])]

    # Analyze barcode lengths and filter out invalid ones
    receipt_items_flattened['barcode_len'] = receipt_items_flattened['barcode'].str.len()
    receipt_items_flattened = receipt_items_flattened[~receipt_items_flattened['barcode_len'].isin([4, 5])]

    # Identify rows with missing `brandCode` but valid `description`
    items_missing = receipt_items_flattened.query("brandCode.isnull() and description.notnull()")
    items_missing['new_description'] = items_missing['description'].str[:20]

    # Load brands list from brands table
    brands_list = pd.read_sql("SELECT DISTINCT brandCode FROM brands WHERE brandCode IS NOT NULL", db_engine)['brandCode'].tolist()

    # Define fuzzy matching function
    def match_brand_code(description, brand_codes, threshold=90):
        if pd.isnull(description):
            return None
        match, score = process.extractOne(description, brand_codes)
        return match if score >= threshold else None

    # Apply fuzzy matching to assign new `brandCode`
    items_missing['newBrandCode'] = items_missing['new_description'].apply(
        lambda desc: match_brand_code(desc, brands_list)
    )

    # Add `BrandCode2` column to the main items table
    receipt_items_flattened['BrandCode2'] = receipt_items_flattened['brandCode']  # Start with existing brandCode
    receipt_items_flattened.loc[
        receipt_items_flattened['brandCode'].isnull(), 'BrandCode2'
    ] = items_missing['newBrandCode']  # Update missing values with matched codes

    # Clean up: drop temporary columns
    receipt_items_flattened = receipt_items_flattened.drop(columns=['barcode_len'])

    table_name = 'items'
    receipt_items_flattened.to_sql(table_name, db_engine, if_exists='replace', index=False)
    print(f"'{table_name}' table loaded to SQL.")

if __name__ == "__main__":
    engine = create_engine('sqlite:///fetch_rewards.db')
    process_items(engine)
