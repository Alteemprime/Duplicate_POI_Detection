import os
from datetime import datetime
import json
import shutil
import re


#read json file skipping error lines and get file size,created and modified date, and rows
def get_file_info(file_path):
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)  # Convert bytes to megabytes
    created_date = datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
    modified_date = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
    total_rows = 0
    
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                data = json.loads(line)
                if "site_code" in data:
                    total_rows += 1
            except json.JSONDecodeError:
                continue

    return file_size_mb, total_rows, created_date, modified_date

def print_yellow_bold(message):
    print(f"\033[1;33m{message}\033[0m")
    
'''
#read input processed row on check and copy procedure and rewrite associated script file
def update_json_to_dataframe_conversion_script(row_limit):
    script_path = "json_to_dataframe_conversion.py"
    with open(script_path, 'r', encoding='utf-8') as file:
        script_content = file.read()
    
    # Update the TRIAL_ROW_LIMIT in the script
    new_script_content = re.sub(r'TRIAL_ROW_LIMIT\s*=\s*\d+', f'TRIAL_ROW_LIMIT = {row_limit}', script_content)
    
    with open(script_path, 'w', encoding='utf-8') as file:
        file.write(new_script_content)
    print(f"Updated {script_path} with TRIAL_ROW_LIMIT = {row_limit}")

'''
#check if delivery json file exist then copy it as datacheck file
def check_and_copy_file():
    delivery_file = "delivery.json"
    datacheck_file = "datacheck.json"
    
    if not os.path.exists(delivery_file):
        print_yellow_bold(f"Make sure the {delivery_file} exists and rerun the script.")
        return False, 0
    if os.path.getsize(delivery_file) == 0:
        print_yellow_bold(f"Make sure the {delivery_file} is not empty and rerun the script.")
        return False, 0
    
    file_size_mb, total_rows, created_date, modified_date = get_file_info(delivery_file)
    
    print(f"File: {delivery_file}")
    print(f"Size: {file_size_mb:.2f} MB")
    print(f"Total Rows: {total_rows}")
    print(f"Created Date: {created_date}")
    print(f"Last Modified Date: {modified_date}")
    
    proceed = input("Make sure you have the correct delivery.json file, do you want to continue? (yes/no): ").strip().lower()
    if proceed != 'yes':
        print_yellow_bold("Aborting script as per user request.")
        return False, 0
    
    while True:
        try:
            user_input = int(input(f"Enter number of rows to process (<= {total_rows}): "))
            if 0 <= user_input <= total_rows:
                break
            else:
                print(f"Please enter a number that is less than or equal to the total rows ({total_rows}).")
        except ValueError:
            print("Invalid input. Please enter a valid number.")
    
    shutil.copyfile(delivery_file, datacheck_file)
    return True, user_input

if __name__ == "__main__":
        
    proceed, row_limit = check_and_copy_file()
    if not proceed:
        exit(1)
    
    '''
    update_json_to_dataframe_conversion_script(row_limit)
    '''
    