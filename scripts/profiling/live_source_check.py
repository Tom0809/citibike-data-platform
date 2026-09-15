import requests
import pandas as pd
import time


url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"


def get_snapshot():
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()

    df = pd.DataFrame(data["data"]["stations"])

    return data, df


# =========================
# Snapshot 1
# =========================

data_1, df_1 = get_snapshot()

print("Snapshot 1 last_updated:")
print(data_1["last_updated"])

print(
    df_1[
        [
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "last_reported"
        ]
    ].head()
)


# =========================
# Wait
# =========================

print("\nWaiting 2 minutes...\n")
time.sleep(120)


# =========================
# Snapshot 2
# =========================

data_2, df_2 = get_snapshot()

print("Snapshot 2 last_updated:")
print(data_2["last_updated"])



compare_df = df_1[
    [
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "last_reported"
    ]
].merge(
    df_2[
        [
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "last_reported"
        ]
    ],
    on="station_id",
    suffixes=("_old", "_new")
)


changed_df = compare_df[
    (compare_df["num_bikes_available_old"]
     != compare_df["num_bikes_available_new"])
    |
    (compare_df["num_docks_available_old"]
     != compare_df["num_docks_available_new"])
]


print("\nNumber of changed stations:")
print(len(changed_df))

print("\nChanged station examples:")
print(
    changed_df[
        [
            "station_id",
            "num_bikes_available_old",
            "num_bikes_available_new",
            "num_docks_available_old",
            "num_docks_available_new"
        ]
    ].head(20)
)