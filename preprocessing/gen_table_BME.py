"""
Update Log:
- 2024-09-03: Implemented NaN value handling for both ExperimentData and BME680Data.
              Added update log.
- 2024-09-02: Added BME680 data to the processed data.
"""

import sqlite3
import pandas as pd
import os

def get_data(db_path, query, params):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def normalize_timestamp(group):
    group['timestamp'] = group['timestamp'] - group['timestamp'].min()
    return group

def apply_filter_and_concat(grouped, target_channel, window_size):
    filtered_data = []
    for _, group in grouped:
        filtered_group = group.copy()
        filtered_group['filtered'] = group[target_channel].rolling(window=window_size, center=False).mean()
        filtered_data.append(filtered_group)
    return pd.concat(filtered_data, ignore_index=True)

def Vo2Resistance(group, target_channel, input_voltage, RL_2, bit_V):
    group['Vo'] = group[target_channel] * bit_V
    # group['resistance'] = (RL_2 / group['Vo']) * ((input_voltage / group['Vo']) - 1) # when RL is in R1 position
    group['resistance'] = (group['Vo']*RL_2) / (input_voltage - group['Vo']) # when RL is in R2 position
    return group

def ratioCalculation(group):
    baseline_col = f"{group['heater_setting'].iloc[0]}_baseline"
    max_reaction_R_col = f"{group['heater_setting'].iloc[0]}_max_reaction_R"
    responsivity_col = f"{group['heater_setting'].iloc[0]}_responsivity"
    
    group[baseline_col] = group['resistance'].head(50).median()
    lowest_20_median = group['resistance'].nsmallest(20).median()
    group[max_reaction_R_col] = lowest_20_median
    group[responsivity_col] = (group[baseline_col] / group[max_reaction_R_col]) - 1
    
    return group

def pivot_metrics(df_full_feature):
    metric_cols = [col for col in df_full_feature.columns if any(metric in col for metric in ['_baseline', '_max_reaction_R', '_responsivity'])]
    df_metrics = df_full_feature[['experiment_id', 'channel_id'] + metric_cols].drop_duplicates()
    df_melted = df_metrics.melt(id_vars=['experiment_id', 'channel_id'], value_vars=metric_cols, var_name='metric', value_name='value')
    df_melted[['heater_setting', 'metric']] = df_melted['metric'].str.extract(r'(\d+)_(baseline|max_reaction_R|responsivity)')
    df_melted['heater_setting'] = df_melted['heater_setting'].astype(int)
    df_pivoted = df_melted.pivot_table(index=['experiment_id', 'channel_id'], columns=['metric', 'heater_setting'], values='value')
    df_pivoted.columns = [f'{metric}_{heater_setting}' for metric, heater_setting in df_pivoted.columns]
    df_pivoted = df_pivoted.reset_index()
    return df_pivoted

def process_experiment_batch(db_path, experiment_batch, output_path, start_date=None, end_date=None):
    target_channel = 'sensor_value'
    window_size = 10
    input_voltage = 3.3
    RL_2 = 10000  # 10kOhm
    adc_bit = 0.000125  # voltage of 1 bit in ADS1115
    channel_to_convert = 'filtered'

    # Modified queries for ExperimentData and BME680Data with date filtering
    exp_query = """
    SELECT experiment_id, experiment_batch, heater_setting, timestamp, sensor_value, channel_id
    FROM ExperimentData
    WHERE experiment_batch = ?
    AND substr(experiment_id, 1, 8) BETWEEN ? AND ?
    """
    
    bme_query = """
    SELECT experiment_id, experiment_batch, timestamp, temperature, humidity, pressure
    FROM BME680Data
    WHERE experiment_batch = ?
    AND substr(experiment_id, 1, 8) BETWEEN ? AND ?
    """
    
    # Fetch data separately with date filtering
    query_params = (experiment_batch, start_date or '00000000', end_date or '99999999')
    df_exp = get_data(db_path, exp_query, query_params)
    df_bme = get_data(db_path, bme_query, query_params)
    
    # Process ExperimentData
    grouped = df_exp.groupby(['experiment_id', 'channel_id', 'heater_setting'], as_index=False, group_keys=False)
    df_ts = grouped.apply(normalize_timestamp).reset_index(drop=True)
    df_filtered = apply_filter_and_concat(grouped, target_channel=target_channel, window_size=window_size)
    
    df_resistance = Vo2Resistance(df_filtered, channel_to_convert, input_voltage, RL_2, adc_bit)
    df_resistance = df_resistance.groupby(['experiment_id', 'channel_id', 'heater_setting'], as_index=False).apply(ratioCalculation)
    
    # Process BME680 data
    bme680_cols = ['temperature', 'humidity', 'pressure']
    df_bme680_avg = df_bme.groupby('experiment_id')[bme680_cols].agg(['mean', 'min', 'max', 'std']).reset_index()
    df_bme680_avg.columns = [f'{col}_{agg}' for col, agg in df_bme680_avg.columns]
    df_bme680_avg.rename(columns={'experiment_id_': 'experiment_id'}, inplace=True)
    
    # Merge BME680 data with metrics
    df_metrics = pivot_metrics(df_resistance)
    df_metrics = df_metrics.merge(df_bme680_avg, on='experiment_id', how='outer')
    
    # Drop rows with NaN values from the merged dataset
    df_metrics_clean = df_metrics.dropna()
    
    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # Save processed data
    df_resistance.to_csv(os.path.join(output_path, f'processed_data_{experiment_batch}.csv'), index=False)
    df_metrics_clean.to_csv(os.path.join(output_path, f'metrics_{experiment_batch}.csv'), index=False)
    
    return df_metrics_clean, df_exp

if __name__ == '__main__':
    db_path = 'D:\\code\\uom_explore\\database\\voc_lab_2.db'
    experiment_batch = 'exp_brujin_seq_1'  # Specify the experiment batch you want to process
    output_path = 'D:\\code\\uom_explore\\processed_data'  # Specify the output directory
    start_date = '20240829'  # Specify the start date in YYYYMMDD format
    end_date = '20240829'  # Specify the end date in YYYYMMDD format

    df_metrics, df_exp = process_experiment_batch(db_path, experiment_batch, output_path, start_date, end_date)
    print(f"Processed data and metrics for batch '{experiment_batch}' from {start_date} to {end_date} have been saved to {output_path}")
    print("\nSample of processed metrics (without NaN values):")
    print(df_metrics.head())

    # Print unique experiment_ids and their counts
    experiment_counts = df_metrics['experiment_id'].value_counts()
    print("\nUnique experiments processed (without NaN values):")
    print(experiment_counts)

    # Print the number of dropped experiments
    total_experiments = len(df_exp['experiment_id'].unique())
    processed_experiments = len(df_metrics['experiment_id'].unique())
    dropped_experiments = total_experiments - processed_experiments
    print(f"\nNumber of experiments dropped due to incomplete data: {dropped_experiments}")