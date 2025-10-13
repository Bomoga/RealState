import requests
from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path
import time

load_dotenv()
API_KEY = os.getenv("API_KEY")
city = "Miami"
state = "FL"

base_url = "https://api.rentcast.io/v1/properties"
params = {"city": city, "state": state, "limit": 500, "offset": 0, "includeTotalCount": "true"}
headers = {"Accept": "application/json", "X-Api-Key": API_KEY}

MAX_REQUESTS = 50

## Paginate records and return dataframe
def fetch_data(max_requests = MAX_REQUESTS):
    total_rows = []
    requests_made = 0

    while requests_made < MAX_REQUESTS:
        r = requests.get(base_url, params = params, headers = headers)
        r.raise_for_status()
        rows = r.json()
        requests_made += 1
        total_rows.extend(rows)

        if len(rows) < params["limit"]:
            break

        params["offset"] += params["limit"]
        time.sleep(0.06)

    all_data = pd.DataFrame.from_records(total_rows)

    return all_data

def main():
    ## Root directory & data output path
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    output_path = Path(os.path.join(f"{BASE_DIR}", "data", "raw", f"PROPERTY_RECORDS_{city}_{state}"))

    if output_path.exists():
        print(f"Data already exists at {output_path}.")
    else:
        data = fetch_data()

        print("Test rows:", len(data))
        print(data.head())
        print(data.shape)
        print(data.columns)

        data.to_csv(output_path, index=False)

if __name__ == "__main__":
    main()