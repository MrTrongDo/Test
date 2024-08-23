Test 
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd
import json


# ----------------------GET DATA HISTORICAL 10VNBY

url = "https://www.worldgovernmentbonds.com/wp-admin/admin-ajax.php?action=jsonStoricoBond&area=58&dateRif=2099-12-31&durata=120&key=d1RHRzY4aE16dU9TaXRvSndPWVBEdz09"

response = requests.get(url).text
data = json.loads(response)  # Parse the JSON data directly
# print(data_dict)

# Convert to DataFrame
df = pd.DataFrame.from_dict(data['quote'], orient='index', columns=['date', 'bond_y'])

# Convert timestamp to readable date format
df['date'] = pd.to_datetime(df['date'], unit='ms')


# ----------------------GET DATA FORECAST 10VNBY


url = ('http://www.worldgovernmentbonds.com/bond-forecast/vietnam/10-years/')
r = requests.get(url)
# print(r.text)

# Get time forecast
web_content = BeautifulSoup(r.text, 'lxml')
web_content = web_content.find('div', {"class" : 'w3-responsive'})
web_content = web_content.find('thead').text
# print(web_content)

# Use regular expression to find lines with dates in MMM YYYY format
pattern = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}'
dates = re.findall(pattern, web_content)

# Convert month-year strings to datetime objects with day set to 1
date_objects = [datetime.strptime(date, '%b %Y') for date in dates]

date = []
# Print the converted date objects
for date_obj in date_objects:
    date.append(date_obj)

date = pd.DataFrame({'date': date})
# print(date)

# Get number forecast
web_number = BeautifulSoup(r.text, 'lxml')
web_number = web_number.find('div', {"class" : 'w3-responsive'})
web_number = web_number.find('tbody')
web_number = web_number.find('tr').text

# # Use regular expression to find lines with '%'
pattern = r'^\d+\.\d+%$'
lines_with_percent = re.findall(pattern, web_number, re.MULTILINE)

# Print the matching lines
bond_y = []
for line in lines_with_percent:
    bond_y.append(line)

bond_y = pd.DataFrame({'bond_y': bond_y})
# print(bond_y)

# Consolidate the two DataFrames into one
consolidated_df = pd.concat([date, bond_y], axis=1)

# Remove the '%' symbol
consolidated_df['bond_y'] = consolidated_df['bond_y'].str.replace('%', '')

# Convert the 'bond_y' column to numeric
consolidated_df['bond_y'] = pd.to_numeric(consolidated_df['bond_y'])


# --------------------COMBINE DATAFRAME

df_combined = pd.concat([df, consolidated_df], ignore_index=True).dropna(subset=['bond_y'])


# ----------------------GET DATA HISTORICAL 02VNBY

url = "https://www.worldgovernmentbonds.com/wp-admin/admin-ajax.php?action=jsonStoricoBond&area=58&dateRif=2099-12-31&durata=24&key=dENuNGVDdERQdUh6ZEhqZTBOWnN5Zz09"

response = requests.get(url).text
data = json.loads(response)  # Parse the JSON data directly
# print(data_dict)

# Convert to DataFrame
df = pd.DataFrame.from_dict(data['quote'], orient='index', columns=['date', 'bond_y'])

# Convert timestamp to readable date format
df['date'] = pd.to_datetime(df['date'], unit='ms')


# ----------------------GET DATA FORECAST 02VNBY


url = ('http://www.worldgovernmentbonds.com/bond-forecast/vietnam/2-years/')
r = requests.get(url)
# print(r.text)

# Get time forecast
web_content = BeautifulSoup(r.text, 'lxml')
web_content = web_content.find('div', {"class" : 'w3-responsive'})
web_content = web_content.find('thead').text
# print(web_content)

# Use regular expression to find lines with dates in MMM YYYY format
pattern = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}'
dates = re.findall(pattern, web_content)

# Convert month-year strings to datetime objects with day set to 1
date_objects = [datetime.strptime(date, '%b %Y') for date in dates]

date = []
# Print the converted date objects
for date_obj in date_objects:
    date.append(date_obj)

date = pd.DataFrame({'date': date})
# print(date)

# Get number forecast
web_number = BeautifulSoup(r.text, 'lxml')
web_number = web_number.find('div', {"class" : 'w3-responsive'})
web_number = web_number.find('tbody')
web_number = web_number.find('tr').text

# # Use regular expression to find lines with '%'
pattern = r'^\d+\.\d+%$'
lines_with_percent = re.findall(pattern, web_number, re.MULTILINE)

# Print the matching lines
bond_y = []
for line in lines_with_percent:
    bond_y.append(line)

bond_y = pd.DataFrame({'bond_y': bond_y})
# print(bond_y)

# Consolidate the two DataFrames into one
consolidated_df = pd.concat([date, bond_y], axis=1)

# Remove the '%' symbol
consolidated_df['bond_y'] = consolidated_df['bond_y'].str.replace('%', '')

# Convert the 'bond_y' column to numeric
consolidated_df['bond_y'] = pd.to_numeric(consolidated_df['bond_y'])


# --------------------COMBINE DATAFRAME

df_combined = pd.concat([df, consolidated_df], ignore_index=True).dropna(subset=['bond_y'])

# Print the consolidated DataFrame
print(df_combined)
