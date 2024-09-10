import os
import re
import sqlite3
import csv

def create_sql_table(db_path, query):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(query)
    conn.commit()
    conn.close()

def extract_channel_id(file_name):
    match = re.search(r'c(\d+)', file_name)
    if match:
        return int(match.group(1))
    return None

def insert_data_from_csv(cursor, experiment_batch, experiment_id, channel_id, file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            try:
                timestamp, temperature, humidity, pressure = map(float, row)
                cursor.execute('''
                INSERT INTO BME680Data (experiment_batch, experiment_id, channel_id, timestamp, temperature, humidity, pressure)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (experiment_batch, experiment_id, channel_id, int(timestamp), temperature, humidity, pressure))
            except ValueError:
                print(f"Skipping invalid row: {row}")

def process_folders(db_path, root_folder):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for batch_folder in os.listdir(root_folder):
        batch_folder_path = os.path.join(root_folder, batch_folder)
        if os.path.isdir(batch_folder_path):
            for csv_file in os.listdir(batch_folder_path):
                if csv_file.endswith('_BME680.csv'):
                    csv_file_path = os.path.join(batch_folder_path, csv_file)
                    experiment_id = os.path.splitext(csv_file)[0].replace('_BME680', '')
                    channel_id = extract_channel_id(csv_file)
                    if channel_id is not None:
                        insert_data_from_csv(cursor, batch_folder, experiment_id, channel_id, csv_file_path)

    conn.commit()
    conn.close()

if __name__ == '__main__':
    root_folder = 'D:\\code\\uom_explore\\raw_data\\2024_09_09'
    db_path = 'D:\\code\\uom_explore\\database\\voc_lab_2.db'

    query = '''
    CREATE TABLE IF NOT EXISTS BME680Data (
        experiment_batch TEXT NOT NULL,
        experiment_id TEXT NOT NULL,
        channel_id INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        temperature REAL NOT NULL,
        humidity REAL NOT NULL,
        pressure REAL NOT NULL
    )
    '''

    create_sql_table(db_path, query)
    process_folders(db_path, root_folder)

print("BME680 data import completed.")