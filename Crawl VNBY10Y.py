import json
import pandas as pd
import requests

url = "https://markets.tradingeconomics.com/chart?s=vnmgovbon10y:gov&interval=1d&span=10y&securify=new&url=/vietnam/government-bond-yield&AUTH=PGfuKhqIDQPL685yYekQYRQEUm%2F%2F9ND9s9l4rHECmvneJmhgaJxapKGQo7%2FRi92E&ohlc=0"

response = requests.get(url).text
data_dict = json.loads(response)  # Parse the JSON data directly
# print(data_dict)

# Extract 'data' from the 'series' key
data_list = data_dict['series'][0]['data']

# Create a DataFrame from the 'data' list
df = pd.DataFrame(data_list)
# print(df)

# Rename column "b" to "yield"
df.rename(columns={"y": "bond_y"}, inplace=True)

df = df[["date", "bond_y"]]
df = df.set_index('date')
print(df)
