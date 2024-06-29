import requests
import zipfile
import os
import pandas as pd
import numpy as np
import sqlite3
from io import BytesIO
from bs4 import BeautifulSoup

def get_download_link(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('openpowerlifting-latest.zip'):
            return href
    raise ValueError("openpowerlifting-latest.zip link not found on the page")

def download_zip(url):
    response = requests.get(url)
    return BytesIO(response.content)

def extract_zip(zip_file, extract_path):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

def csv_to_dataframe(csv_path):
    # Read the CSV in chunks to handle large files
    chunks = pd.read_csv(csv_path, chunksize=100000, low_memory=False)
    return pd.concat(chunks, ignore_index=True)

def dataframe_to_sqlite(df, db_path, table_name):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False, chunksize=100000)
    conn.close()

def main():
    url = "https://openpowerlifting.gitlab.io/opl-csv/bulk-csv.html"
    extract_folder = "data"
    output_csv = "powerlifting_data.csv"
    output_db = "powerlifting_data.sqlite"
    table_name = "powerlifting"
    
    os.makedirs(extract_folder, exist_ok=True)

    download_link = get_download_link(url)
    print(f"Download link: {download_link}")

    zip_file = download_zip(download_link)
    extract_zip(zip_file, extract_folder)

    csv_file = None
    for root, dirs, files in os.walk(extract_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_file = os.path.join(root, file)
                break
        if csv_file:
            break

    if not csv_file:
        raise FileNotFoundError("No CSV files found in the extracted folder or its subfolders")

    print(f"Reading CSV file: {csv_file}")
    df = csv_to_dataframe(csv_file)
    print(f"Original DataFrame shape: {df.shape}")

    queried_df = df.copy()

    # Check if 'Age' column exists
    if 'Age' in queried_df.columns:
        queried_df['Age'] = np.ceil(queried_df['Age']).astype('Int64')
    else:
        print("Warning: 'Age' column not found in the DataFrame")

    output_csv_path = os.path.join(extract_folder, output_csv)
    queried_df.to_csv(output_csv_path, index=False)
    
    print(f"\nExported {len(queried_df)} rows to {output_csv_path}")

    output_db_path = os.path.join(extract_folder, output_db)
    dataframe_to_sqlite(queried_df, output_db_path, table_name)
    
    print(f"Exported data to SQLite database: {output_db_path}")
    print(f"Table name in the database: {table_name}")

    print(f"\nFinal DataFrame shape: {queried_df.shape}")
    print("\nFirst few rows of the DataFrame:")
    print(queried_df.head())
    print("\nDataFrame Info:")
    print(queried_df.info())

if __name__ == "__main__":
    main()