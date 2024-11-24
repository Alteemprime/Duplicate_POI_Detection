import json
import pandas as pd
import os 
import sys
from tqdm import tqdm
import helpers

#loading json data with limiting rows, it seems pandas are more robust to handle large dataset
def load_data(filename, row_limit=TRIAL_ROW_LIMIT):
    skipped_entries = []
    with open(filename, 'r', encoding='utf-8') as file:
        for i, line in enumerate(file):
            if i >= row_limit:
                break
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                print(f"Skipping invalid line {i + 1}")
                skipped_entries.append({"line_number": i + 1, "content": line.strip()})                
    return skipped_entries

#recursive function to convert nested json to flat dictionary
#recursively dig through dict structure, until it found list data as values
def flatten_json(data):
    out = {}

    def flatten(entry, name=''):#name as column name, initiated as '' and keep appending keys recursively as column names
        if type(entry) is dict:
            for a in entry:
                flatten(entry[a], name + a + '.')
        elif type(entry) is list:
            i = 0 #i is used as psudokeys, since each list values do not have unique keys
            for a in entry:
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = entry #if its not dict or list, it the value we want to extract

    flatten(data)
    return out

# First pass: collect all keys
def collect_all_keys(filename, row_limit=TRIAL_ROW_LIMIT):
    all_columns = set()
    for entry in tqdm(load_data(filename, row_limit), desc="Collecting all keys..."):
        flat_data = flatten_json(entry)
        all_columns.update(flat_data.keys())
    return all_columns

# Second pass: process and write the flattened data
def process_data(filename, file_out, all_columns, skipped_entries, row_limit=TRIAL_ROW_LIMIT):
    with open(file_out, 'w', newline='') as f_out:
        # Write all columns as the header once before processing
        pd.DataFrame(columns=all_columns).to_csv(f_out, mode='w', index=False, header=True)
        
        for entry in tqdm(load_data(filename, row_limit), desc="Processing data"):
            flat_data = flatten_json(entry)
            df = pd.DataFrame([flat_data], columns=all_columns)

            # Append the row data to the CSV without writing the header again
            df.to_csv(f_out, mode='a', index=False, header=False)

    # Save the skipped entries to a JSON file
    with open('skipped_entries.json', 'w') as file:
        json.dump(skipped_entries, file, indent=4)

if __name__ == "__main__":
    MAX_ROWS_PER_SHEET = 1000000
    TRIAL_ROW_LIMIT = 1446176
    
    #set 'next_file' to default of ''
    helpers.update_parameters('next_file' = '')

    param_file = 'parameters.json'
    #load parameters
    param = helpers.load_parameters(param_file)
    TRIAL_ROW_LIMIT = param['subset']
    row_limit = TRIAL_ROW_LIMIT // 1000
    
    file_in = 'datacheck.json'
    file_out = f's{row_limit}.csv'
    helpers.update_parameters('next_file' = file_out)
        
    # Step 1: Collect all unique columns from the entire dataset
    all_columns = list(collect_all_keys(file_in))
    for column in all_columns:
        print(column)

    # Step 2: Prepare to capture skipped entries
    skipped_entries = []

    # Step 3: Process the data and write to CSV using the full set of columns
    process_data(file_in, file_out, all_columns, skipped_entries)
                
    if os.stat(file_out).st_size == 0:
        print(f'{file_out} does not contain any rows')
        sys.exit(1)
    
    