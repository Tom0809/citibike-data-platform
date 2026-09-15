import requests


STATION_STATUS_URL = (
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"
)

STATION_INFORMATION_URL = (
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
)


def fetch_gbfs_feed(url: str) -> dict:
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    return response.json()

if __name__ == "__main__":
    status_data = fetch_gbfs_feed(STATION_STATUS_URL)

    print("Feed version:", status_data["version"])
    print("Last updated:", status_data["last_updated"])
    print("Number of stations:", len(status_data["data"]["stations"]))