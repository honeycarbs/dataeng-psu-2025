import os
import urllib.request
import urllib.error
import json
from tqdm import tqdm

def read_IDs_from_file(file_path):
    IDs = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                number = line.strip()
                if number:
                    IDs.append(number)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    return IDs

def fetchData(vehicleID):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={urllib.parse.quote(vehicleID)}"
    try:
        response = urllib.request.urlopen(url)
        data = response.read()
        parsed = json.loads(data)
        return parsed
    except urllib.error.HTTPError as err:
        print(f"HTTPError for vehicle ID {vehicleID}: {err.code} {err.reason}")
    except Exception as e:
        print(f"Error processing vehicle ID {vehicleID}: {e}")
    return None

def fetch_records(IDs_list, oFile, required_records=100):
    fetched_count = 0
    i = 0
    first_entry = True

    with open(oFile, 'w') as f:
        f.write('[')

    with tqdm(total=required_records, desc="Fetching records", dynamic_ncols=True) as progress_bar:
        while fetched_count < required_records and i < len(IDs_list):
            vehicleID = IDs_list[i]
            parsed = fetchData(vehicleID)
            if parsed:
                with open(oFile, 'a') as f:
                    for record in parsed:
                        if not first_entry:
                            f.write(',')
                        else:
                            first_entry = False
                        json.dump(record, f, separators=(',', ':'))
                fetched_count += 1
                progress_bar.set_postfix({'Fetched': fetched_count})
                progress_bar.update(1)
            else:
                print(f"Skipping vehicle ID {vehicleID} due to error.")
            i += 1

    # Finalize the JSON array
    with open(oFile, 'a') as f:
        f.write(']')

file_path = "IDs.txt"
IDs_list = read_IDs_from_file(file_path)
output_file = "bc_sample.json"

fetch_records(IDs_list, output_file,required_records=2)