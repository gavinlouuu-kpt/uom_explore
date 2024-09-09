import os
import pandas as pd
from ipywidgets import widgets, interact
from IPython.display import display

# Path to the data folder
folder_path = 'D:\\code\\uom_explore\\raw_data\\2024_09_03\\de_brujin_large_even_1'

# List all CSV files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Default column names
default_columns = ['Setting', 'Timestamp', 'sensor_value']

# Process each CSV file
for file in csv_files:
    file_path = os.path.join(folder_path, file)
    
    # Load the data with default column names
    df = pd.read_csv(file_path, sep=',', names=default_columns, header=0)
    
    # Display a random sample of the data
    sample_df = df.sample(5)
    display(sample_df)
    
    # Create widgets for manual ticking
    quality_checks = {index: widgets.Checkbox(description=f'Row {index}', value=False) for index in sample_df.index}
    quality_check_boxes = widgets.VBox(list(quality_checks.values()))
    display(quality_check_boxes)
    
    # Function to generate the report
    def generate_report():
        report = []
        for index, checkbox in quality_checks.items():
            status = 'Good' if checkbox.value else 'Bad'
            report.append(f'Row {index}: {status}')
        return '\n'.join(report)
    
    # Button to generate the report
    button = widgets.Button(description="Generate Report")
    output = widgets.Output()
    
    def on_button_clicked(b):
        with output:
            print(generate_report())
    
    button.on_click(on_button_clicked)
    display(button, output)