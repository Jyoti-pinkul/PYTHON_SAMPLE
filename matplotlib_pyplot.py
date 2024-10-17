import pandas as pd
import matplotlib.pyplot as plt
import os

# Read the data from a CSV file that contains paths
csv_file = '/path/to/your/data.csv'
df = pd.read_csv(csv_file)

# Define a function to extract year, month, and parent from the path
def extract_info_from_path(path):
    parts = path.split(os.sep)  # Split by the file separator
    if len(parts) >= 6:  # Ensure path is long enough
        year = parts[-5]
        month = parts[-4]
        parent = parts[-2]
        return parent, year, month
    return None, None, None

# Create new columns for Parent, Year, and Month
df[['Parent', 'Year', 'Month']] = df.apply(lambda row: pd.Series(extract_info_from_path(row['Path'])), axis=1)

# Drop rows with invalid paths
df = df.dropna(subset=['Parent', 'Year', 'Month'])

# Count the number of entries for each Parent, Year, and Month
df['Count'] = 1  # Add a count column
df_grouped = df.groupby(['Parent', 'Year', 'Month']).size().reset_index(name='Count')

# Plotting the data for each parent
for parent in df_grouped['Parent'].unique():
    parent_data = df_grouped[df_grouped['Parent'] == parent]
    
    # Pivoting data for easier plotting
    pivot_data = parent_data.pivot(index='Month', columns='Year', values='Count').fillna(0)
    
    # Plot
    pivot_data.plot(kind='bar', stacked=True)
    plt.title(f'Document Counts for Parent: {parent}')
    plt.xlabel('Month')
    plt.ylabel('Document Count')
    plt.xticks(rotation=45)
    plt.legend(title='Year')
    plt.tight_layout()
    
    # Show plot
    plt.show()
