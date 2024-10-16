import os
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

# Set your base directory
base_dir = '/path/to/root/directory'

# Dictionary to store counts of documents
data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

# Walk through the directory structure
for root, dirs, files in os.walk(base_dir):
    # Expecting the structure: /a/year/month/date/parent/child/uuid
    parts = root.split(os.sep)
    
    if len(parts) >= 6:  # Ensure it's deep enough in the directory structure
        year = parts[-5]    # Extract year
        month = parts[-4]   # Extract month
        parent = parts[-2]  # Extract parent category
        
        # Count the entries for each parent by year and month
        data[parent][year][month] += len(files)

# Convert the data into a DataFrame for plotting
records = []
for parent, years in data.items():
    for year, months in years.items():
        for month, count in months.items():
            records.append((parent, year, month, count))

df = pd.DataFrame(records, columns=['Parent', 'Year', 'Month', 'Count'])

# Plotting the data for each parent
for parent in df['Parent'].unique():
    parent_data = df[df['Parent'] == parent]
    
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
