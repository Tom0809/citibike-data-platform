import requests
import pandas as pd


# =====================================================
# 1. Read Citi Bike GBFS discovery API
# =====================================================

discovery_url = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"

response = requests.get(discovery_url, timeout=30)
response.raise_for_status()

data = response.json()


# =====================================================
# 2. Get English feeds
# =====================================================

feeds = data["data"]["en"]["feeds"]

feeds_df = pd.DataFrame(feeds)

print("\n===== AVAILABLE FEEDS =====")
print(feeds_df)


# =====================================================
# 3. Find station_status API URL
# =====================================================

station_status_url = feeds_df.loc[
    feeds_df["name"] == "station_status",
    "url"
].iloc[0]

print("\nStation Status URL:")
print(station_status_url)


# =====================================================
# 4. Read station_status API
# =====================================================

status_response = requests.get(station_status_url, timeout=30)
status_response.raise_for_status()

status_data = status_response.json()


# =====================================================
# 5. Extract station records
# =====================================================

stations = status_data["data"]["stations"]

station_status_df = pd.DataFrame(stations)


# =====================================================
# 6. Basic profiling
# =====================================================

print("\n===== STATION STATUS DF =====")

print("\nShape:")
print(station_status_df.shape)

print("\nColumns:")
print(station_status_df.columns.tolist())

print("\nFirst 5 rows:")
print(station_status_df.head())

print("\nData Types:")
print(station_status_df.dtypes)

print("\nNull Values:")
print(station_status_df.isnull().sum())

