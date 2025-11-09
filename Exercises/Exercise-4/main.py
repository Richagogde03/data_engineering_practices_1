import os
import json
import pandas as pd
from pandas import json_normalize

def find_json_files(root_folder):
    """Walk through all subfolders and collect paths to .json files."""
    json_files = []
    for root, _, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".json"):
                json_files.append(os.path.join(root, file))
    return json_files


def flatten_json_to_csv(json_path, output_folder):
    """Read a JSON file, flatten nested fields, and save as CSV in output folder."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = [data]

    df = json_normalize(data)

    base_name = os.path.basename(json_path)
    csv_name = os.path.splitext(base_name)[0] + ".csv"
    output_path = os.path.join(output_folder, csv_name)

    df.to_csv(output_path, index=False)
    print(f"Converted: {json_path} → {output_path}")


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    output_dir = os.path.join(base_dir, "output")

    os.makedirs(output_dir, exist_ok=True)

    json_files = find_json_files(data_dir)
    if not json_files:
        print("No JSON files found in the 'data' folder.")
        return

    for json_file in json_files:
        flatten_json_to_csv(json_file, output_dir)

    print(f"\nAll CSV files saved in: {output_dir}")


if __name__ == "__main__":
    main()
