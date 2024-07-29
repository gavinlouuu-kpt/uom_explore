import os
import re
import sqlite3
import csv

def create_sql_table(db_path, query):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Create table
    c.execute(query)
    
    conn.commit()
    conn.close()



# Function to read CSV and insert data into the database
def insert_data_from_csv(experiment_batch, experiment_id, channel_id, file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        first_row = next(reader)
        
        # Check if the first row contains non-numeric values
        try:
            int(first_row[0])
            int(first_row[1])
            float(first_row[2])
            # First row is valid data, process it
            process_row(experiment_batch, experiment_id, channel_id, first_row)
        except ValueError:
            # First row is header, skip it and process the rest
            pass
        
        for row in reader:
            process_row(experiment_batch, experiment_id, channel_id, row)

def process_row(experiment_batch, experiment_id, channel_id, cursor, row):
    heater_setting, timestamp, sensor_value = row
    cursor.execute('''
    INSERT INTO ExperimentData (experiment_batch, experiment_id, channel_id, heater_setting, timestamp, sensor_value)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (experiment_batch, experiment_id, channel_id, int(heater_setting), int(timestamp), float(sensor_value)))

# Function to extract channel_id from filename using regex
def extract_channel_id(file_name):
    match = re.search(r'c(\d+)', file_name)
    if match:
        return int(match.group(1))
    return None

def process_folders(root_folder):
    for batch_folder in os.listdir(root_folder):
        batch_folder_path = os.path.join(root_folder, batch_folder)
        if os.path.isdir(batch_folder_path):
            for csv_file in os.listdir(batch_folder_path):
                if csv_file.endswith('.csv') and '_BME680' not in csv_file:
                    csv_file_path = os.path.join(batch_folder_path, csv_file)
                    experiment_id = os.path.splitext(csv_file)[0]
                    channel_id = extract_channel_id(csv_file)
                    if channel_id is not None:
                        insert_data_from_csv(batch_folder, experiment_id, channel_id, csv_file_path)


if __name__ == '__main__':

    # Root folder containing all batch folders
    root_folder = 'D:\\code\\uom_explore\\raw_data\\2024_07_29'
    query = '''
    CREATE TABLE IF NOT EXISTS ExperimentData (
        experiment_batch TEXT NOT NULL,
        experiment_id TEXT NOT NULL,
        channel_id INTEGER NOT NULL,
        heater_setting INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        sensor_value REAL NOT NULL
    )
    '''

    # Path to your database file
    db_path = 'D:\\code\\uom_explore\\database\\voc_lab_2.db'
    create_sql_table(db_path, query)
    process_folders(root_folder)