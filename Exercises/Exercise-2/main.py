import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

def main():
    base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    target_date = "2024-01-19 15:33"

    print("Fetching webpage...")
    response = requests.get(base_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    rows = soup.find_all("tr")

    target_filename = None

    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            file_name = cols[0].text.strip()
            last_modified = cols[1].text.strip()

            if last_modified == target_date:
                target_filename = file_name
                break

    if not target_filename:
        print("Could not find file for the given timestamp.")
        return

    print(f"Found file: {target_filename}")

    file_url = base_url + target_filename

    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_file_path = os.path.join(script_dir, target_filename)

    print(f"Downloading file from {file_url} ...")
    file_resp = requests.get(file_url)
    file_resp.raise_for_status()

    with open(local_file_path, "wb") as f:
        f.write(file_resp.content)

    print(f"File downloaded: {local_file_path}")

    print("Loading data with pandas...")
    df = pd.read_csv(local_file_path)

    if "HourlyDryBulbTemperature" not in df.columns:
        print("Column 'HourlyDryBulbTemperature' not found.")
        return

    max_temp = df["HourlyDryBulbTemperature"].max()
    hottest_records = df[df["HourlyDryBulbTemperature"] == max_temp]

    print("Record(s) with highest HourlyDryBulbTemperature:")
    print(hottest_records)

if __name__ == "__main__":
    main()
