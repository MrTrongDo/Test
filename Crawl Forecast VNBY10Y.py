import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd

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

# Print the consolidated DataFrame
print(consolidated_df)
