import requests
import pandas as pd


url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"

response = requests.get(url, timeout=30)
response.raise_for_status()

data = response.json()

stations = data["data"]["stations"]

station_info_df = pd.DataFrame(stations)


print("\n===== STATION INFORMATION =====")

print("\nShape:")
print(station_info_df.shape)

print("\nColumns:")
print(station_info_df.columns.tolist())

print("\nFirst 5 rows:")
print(station_info_df.head())

print("\nData Types:")
print(station_info_df.dtypes)

print("\nNull Values:")
print(station_info_df.isnull().sum())

print("\nUnique station_id:")
print(station_info_df["station_id"].nunique())

print("\nDuplicate station_id:")
print(station_info_df["station_id"].duplicated().sum())

print("\nFeed last_updated:")
print(data["last_updated"])

print("\nTTL:")
print(data["ttl"])

print("\nVersion:")
print(data["version"])

# =====================================================
# CHECK JOIN WITH STATION STATUS
# =====================================================

status_url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"

status_response = requests.get(status_url, timeout=30)
status_response.raise_for_status()

status_data = status_response.json()

station_status_df = pd.DataFrame(
    status_data["data"]["stations"]
)


joined_df = station_status_df.merge(
    station_info_df,
    on="station_id",
    how="left"
)


print("\n===== JOIN CHECK =====")

print("\nStation status rows:")
print(len(station_status_df))

print("\nStation information rows:")
print(len(station_info_df))

print("\nJoined rows:")
print(len(joined_df))

print("\nMissing station names after join:")
print(joined_df["name"].isnull().sum())

print("\nExample:")
print(
    joined_df[
        [
            "station_id",
            "name",
            "lat",
            "lon",
            "capacity",
            "num_bikes_available",
            "num_docks_available"
        ]
    ].head(10)
)

print("\n===== STATUS SANITY CHECK =====")

print("\nBike / Dock Summary:")
print(
    station_status_df[
        [
            "num_bikes_available",
            "num_docks_available"
        ]
    ].describe()
)

print("\nStations with 0 docks available:")
print(
    (station_status_df["num_docks_available"] == 0).sum()
)

print("\nStations with 0 bikes available:")
print(
    (station_status_df["num_bikes_available"] == 0).sum()
)

print("\nFirst 10 stations with operational status:")
print(
    joined_df[
        [
            "name",
            "capacity",
            "num_bikes_available",
            "num_docks_available",
            "is_installed",
            "is_renting",
            "is_returning"
        ]
    ].head(10)
)

operational_df = station_status_df[
    (station_status_df["is_installed"] == 1)
    & (station_status_df["is_renting"] == 1)
    & (station_status_df["is_returning"] == 1)
]

print("Operational stations:")
print(len(operational_df))

print("Operational stations with 0 bikes:")
print(
    (operational_df["num_bikes_available"] == 0).sum()
)

print("Operational stations with 0 docks:")
print(
    (operational_df["num_docks_available"] == 0).sum()
)