import sqlite3
import pandas as pd



def get_data(db_path, query):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    return df



def normalize_timstamp(group):
    group['timestamp'] = group['timestamp'] - group['timestamp'].iloc[0]
    return group



def apply_filter_and_concat(group, target_channel, window_size):
    """
    Applies a rolling mean filter to the target channel within each group and concatenates the result.

    Parameters:
    data (pd.DataFrame): The input DataFrame containing the data.
    target_channel (str): The name of the channel to which the filter should be applied.
    window_size (int): The window size for the rolling mean filter.

    Returns:
    pd.DataFrame: The concatenated DataFrame with the filtered values.
    """
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

def ratioCalculation(group):
    # Calculate the baseline, max reaction resistance, and responsivity
    baseline_col = f"{group['heater_setting'].iloc[0]}_baseline"
    max_reaction_R_col = f"{group['heater_setting'].iloc[0]}_max_reaction_R"
    responsivity_col = f"{group['heater_setting'].iloc[0]}_responsivity"
    
    group[baseline_col] = group['resistance'].head(50).median()
    lowest_20_median = group['resistance'].nsmallest(20).median()
    group[max_reaction_R_col] = lowest_20_median
    # group[max_reaction_R_col] = group['resistance'].min()
    group[responsivity_col] = (group[baseline_col] / group[max_reaction_R_col]) - 1
    
    return group

def pivot_metrics(df_full_feature):
    """
    Pivot the DataFrame to have heater setting as columns and experiment_id as index.

    Parameters:
    df_full_feature (pd.DataFrame): The input DataFrame containing the full features.

    Returns:
    pd.DataFrame: The pivoted DataFrame with heater settings as columns and experiment_id as index.
    """
    # Extract relevant columns
    metric_cols = [col for col in df_full_feature.columns if any(metric in col for metric in ['_baseline', '_max_reaction_R', '_responsivity'])]
    df_metrics = df_full_feature[['experiment_id', 'channel_id'] + metric_cols].drop_duplicates()

    # Melt the DataFrame to long format
    df_melted = df_metrics.melt(id_vars=['experiment_id', 'channel_id'], value_vars=metric_cols, var_name='metric', value_name='value')

    # Extract heater_setting and metric from the column names
    df_melted[['heater_setting', 'metric']] = df_melted['metric'].str.extract(r'(\d+)_(baseline|max_reaction_R|responsivity)')
    df_melted['heater_setting'] = df_melted['heater_setting'].astype(int)

    # Pivot the DataFrame for each metric
    df_pivoted = df_melted.pivot_table(index=['experiment_id', 'channel_id'], columns=['metric', 'heater_setting'], values='value')

    # Flatten the MultiIndex columns
    df_pivoted.columns = [f'{metric}_{heater_setting}' for metric, heater_setting in df_pivoted.columns]
    df_pivoted = df_pivoted.reset_index()

    return df_pivoted

# run the script if it is main
if __name__ == '__main__':
    db_path = 'D:\\code\\uom_explore\\database\\voc_lab_2.db'

    query = f"""
    SELECT experiment_id, heater_setting, timestamp, sensor_value, channel_id
    FROM ExperimentData
    """
    target_channel = 'sensor_value'
    window_size = 10
    input_voltage = 3.3
    RL_2 = 10000 # 10kOhm
    adc_bit = 0.000125 # voltage of 1 bit in ADS1115
    channel_to_convert = 'filtered'

    df = get_data(db_path, query)
    # group df by experiment_id and heater_setting
    grouped = df.groupby(['experiment_id', 'heater_setting'], as_index=False, group_keys=False)

    df_ts = grouped.apply(normalize_timstamp).reset_index(drop=True)
    grouped_ts = df_ts.groupby(['experiment_id','heater_setting'], as_index=False, group_keys=False)
    df_filtered = apply_filter_and_concat(grouped_ts, target_channel=target_channel, window_size=window_size)
    
    df_resistance = Vo2Resistance(df_filtered, channel_to_convert, input_voltage, RL_2, adc_bit)
    df_resistance = df_resistance.groupby(['experiment_id', 'heater_setting'], as_index=False).apply(ratioCalculation)
    df_metrics = pivot_metrics(df_resistance)
    print(df_metrics.head())