import sqlite3
import pandas as pd

db_path = 'D:\\code\\uom_explore\\database\\voc_lab_2.db'

query = f"""
SELECT experiment_id, heater_setting, timestamp, sensor_value, channel_id
FROM ExperimentData
"""

def get_data(db_path, query):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    return df

df = get_data(db_path, query)
# group df by experiment_id and heater_setting
grouped = df.groupby(['experiment_id', 'heater_setting'], as_index=False, group_keys=False)

def normalize_timstamp(group):
    group['timestamp'] = group['timestamp'] - group['timestamp'].iloc[0]
    return group

target_channel = 'sensor_value'
window_size = 10

def apply_filter_and_concat(data, target_channel, window_size):
    """
    Applies a rolling mean filter to the target channel within each group and concatenates the result.

    Parameters:
    data (pd.DataFrame): The input DataFrame containing the data.
    target_channel (str): The name of the channel to which the filter should be applied.
    window_size (int): The window size for the rolling mean filter.

    Returns:
    pd.DataFrame: The concatenated DataFrame with the filtered values.
    """
    grouped = data.groupby('channel_id')
    filtered_data = []

    for name, group in grouped:
        filtered_group = group.copy()  # Avoid modifying the original data
        filtered_group['filtered'] = group[target_channel].rolling(window=window_size, center=False).mean()
        filtered_data.append(filtered_group)

    df_filtered = pd.concat(filtered_data, ignore_index=True)
    return df_filtered

def Vo2Resistance(group, target_channel, input_voltage, RL_2, bit_V, ):
    group['Vo'] = group[target_channel]*bit_V
    group['resistance'] = (RL_2/group['Vo'])*((input_voltage/group['Vo'])-1)
    return group

