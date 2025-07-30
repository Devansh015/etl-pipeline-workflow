import pandas as pd 
import sqlite3

def extract_data(file_path):

    data = pd.read_csv(file_path)
    return data

data = extract_data('data/source_data.csv')
print(data.head())

def transform_data(data):
    data = data.dropna()  

    data = data[data['0'] > 18]

    return data

transform_data = transform_data(data)
print(transform_data.head())

def load_data(data, database_path):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER
        )
    ''')        

    for _, row in data.iterrows():
        cursor.execute('''
            INSERT INTO users (name, age) VALUES (?, ?)
        ''', (row['1'], row['0']))

        conn.commit()   
        conn.close()

load_data(transform_data, 'data/destination.db')

def run_etl_pipeline():
    # Extract
    data = extract_data('data/source_data.csv')

    # Transform
    transformed_data = transform_data(data)

    #Load
    load_data(transformed_data, 'data/destination.db')

# Run the ETL pipeline
run_etl_pipeline()
