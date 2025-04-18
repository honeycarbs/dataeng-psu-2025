import os
import urllib.request
import urllib.parse
import urllib.error
from json import load, dump, dumps, loads
from tqdm import tqdm

def read_IDs_from_file(file_path):
    IDs = []
    try:
        with open(file_path, 'r') as file:
            for i, line in enumerate(file):
                number = line.strip()
                IDs.append(number)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    
    return IDs

def fetchData(vehicleID, oFile):
    url =  f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={urllib.parse.quote(vehicleID)}"
    
    try:
        response = urllib.request.urlopen(url)
        data = response.read()
        parsed = loads(data)  # list of dicts

        existing_data = []

        if os.path.exists(oFile) and os.path.getsize(oFile) > 0:
            try:
                with open(oFile, 'r') as file:
                    existing_data = load(file)
                    if not isinstance(existing_data, list):
                        existing_data = [existing_data]
            except Exception:
                existing_data = []

        existing_data.extend(parsed)  # flatten, not append a list

        with open(oFile, 'w') as file:
            dump(existing_data, file, indent=4)

        return True 

    except urllib.error.HTTPError as err:
        print(f"HTTPError for vehicle ID {vehicleID}: {err.code} {err.reason}")
        return False
    except Exception as e:
        print(f"Error processing vehicle ID {vehicleID}: {e}")
        return False


def fetch_records(IDs_list, oFile, required_records=100):
    fetched_count = 0
    i = 0
    
    with tqdm(total=required_records, desc="Fetching records", dynamic_ncols=True) as progress_bar:
        while fetched_count < required_records and i < len(IDs_list):
            vehicleID = IDs_list[i]
            if fetchData(vehicleID, oFile):
                fetched_count += 1
                progress_bar.set_postfix({'Fetched': fetched_count})
                progress_bar.update(1)
            else:
                print(f"Skipping vehicle ID {vehicleID} due to error.")
            
            i += 1

        print(f"Finished fetching {fetched_count} records.")

# Example Usage
file_path = "IDs.txt"
IDs_list = read_IDs_from_file(file_path)
output_file = "output.json"

fetch_records(IDs_list, output_file, required_records=50)
