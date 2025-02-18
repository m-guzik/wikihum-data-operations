"""
A simple script that merges data from two sources - elements that have deleted items as values of the P84 property and
list of elements that have been deleted during recent operations in WikiHum, along with the elements to which the data 
from those deleted elements have been moved.
"""

import pandas as pd 


neighborhood_data = pd.read_csv('data_2025/03.2_neighborhood_with_deleted_items.csv')
ahp_deleted_data = pd.read_csv('data_2025/03.3_AHP_PRNG_report.csv')

joined_tables = pd.merge(neighborhood_data, ahp_deleted_data, how="left", on="value")

print(joined_tables.head(10))

joined_tables.to_csv('data_2025/03.4_joined_data.csv')


