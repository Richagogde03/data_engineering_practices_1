import requests
import gzip
import os

def download_file(url, filename):
    """Download a file from a public HTTPS URL."""
    print(f"Downloading {url} ...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Saved to {filename}")
    return filename


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_url = "https://data.commoncrawl.org/"

    key_name = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    local_gz = os.path.join(script_dir, "wet.paths.gz")

    download_file(base_url + key_name, local_gz)

    print("Extracting wet.paths.gz ...")
    with gzip.open(local_gz, "rt") as gz_file:
        first_line = gz_file.readline().strip()
    print(f"First URI: {first_line}")

    if first_line.startswith("s3://commoncrawl/"):
        file_url = base_url + first_line.replace("s3://commoncrawl/", "")
    else:
        file_url = base_url + first_line

    local_file = os.path.join(script_dir, "first_file.wet.gz")

    download_file(file_url, local_file)

    print("\nReading and printing lines from the downloaded file...\n")
    with gzip.open(local_file, "rt", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, start=1):
            print(line.strip())
            if i >= 20:
                print("\n... (truncated output) ...")
                break


if __name__ == "__main__":
    main()
