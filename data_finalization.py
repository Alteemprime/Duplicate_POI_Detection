import pandas as pd
from tqdm import tqdm
from datetime import time
import re
import helpers

#hour_merging
DAYS_ORDER = ['Mo','Tu','We','Th','Fr','Sa','Su']
DAY_TO_INDEX = {day : idx for idx,day in enumerate(DAYS_ORDER)}
INDEX_TO_DAY = {idx : day for idx,day in enumerate(DAYS_ORDER)}

#defs for hour comparison
def parse_time_range(time_range):
    start_str, end_str = time_range.split('-')
    start_time = time.fromisoformat(start_str)
    end_time = time.fromisoformat(end_str)
    #print(f'parse time result : {start_time}, {end_time}')
    return (start_time, end_time)

def parse_row(row):
    skipped_rowprocess = None
    if pd.isna(row):
        return {}, None

    schedule = {}
    try:
        segments = row.split(";")
        
        for segment in segments:
            segment = segment.strip() #remove leading and trailing whitespaces
            
            # Match day range and time interval within each segment
            match = re.match(r'(\b\w{2}(?:-\w{2})?)\s+(\d{2}:\d{2}-\d{2}:\d{2})', segment)
            if not match:
                return {}, row 
        
            day_range, time_range = match.groups()
            start_time, end_time = parse_time_range(time_range)
            
            # Process day range (either single day or consecutive days)
            if '-' in day_range:  # Consecutive days like "Mo-We"
                start_day, end_day = day_range.split('-')
                if start_day not in DAY_TO_INDEX or end_day not in DAY_TO_INDEX:
                    return {}, row  # Invalid day names, skip row
                #assign start and end index
                start_idx = DAY_TO_INDEX[start_day]
                end_idx = DAY_TO_INDEX[end_day]
                
                # Determine all days in the specified range
                if start_idx <= end_idx:
                    days = DAYS_ORDER[start_idx:end_idx + 1]
                else:
                    days = DAYS_ORDER[start_idx:] + DAYS_ORDER[:end_idx + 1]#just in case if its was coded Sa-Tu
            else:  # Single day like "Mo"
                if day_range not in DAY_TO_INDEX:
                    return {}, row  # Invalid single day, skip row
                days = [day_range]
        
        # Assign time range to each day in the range
        for day in days:
            if day not in schedule:
                schedule[day] = (start_time, end_time)
            else:
                # Merge overlapping times if day already exists
                existing_start, existing_end = schedule[day]
                new_start = min(existing_start, start_time)
                new_end = max(existing_end, end_time)
                schedule[day] = (new_start, new_end)
    except ValueError as e:
        skipped_rowprocess = row
        print(f'error processsing {row} due to {e}')
        
    #print(f'result parse row: {schedule},{skipped_rowprocess}')
    
    return schedule, skipped_rowprocess

def merge_schedule(schedule1, schedule2):    
    if schedule1 is None and schedule2 is None:
        return None
    if schedule2 is None:
        return schedule1
    if schedule1 is None:
        return schedule2
    
    merged_schedule = schedule1.copy()

    for day,time_range in schedule2.items():
        if day in merged_schedule:
            #compare start and end time for the same day, vote min for start_time, vote max for end_time
            existing_start, existing_end = merged_schedule[day]
            new_start, new_end = time_range
            merged_start = min(existing_start, new_start)
            merged_end = max(existing_end, new_end)
            merged_schedule[day] = (merged_start, merged_end)
        else:
            #if day being compared non exist, add it with it time range
            merged_schedule[day] = time_range
    
    #print(f'result merged schecule : {merged_schedule}')

    return merged_schedule

def format_schedule(schedule):
    if not schedule:
        return None

    sorted_schedule = sorted(schedule.items(), key = lambda x: DAY_TO_INDEX[x[0]])
    
    formatted_schedule = []
    
    def add_schedule_entry(start_day, end_day, interval):
        if start_day == end_day:
            formatted_schedule.append(f"{start_day} {interval[0]}-{interval[1]}")
        else:
            formatted_schedule.append(f"{start_day}-{end_day} {interval[0]}-{interval[1]}")
            
    start_day, current_interval = sorted_schedule[0]
    end_day = start_day  # Initialize end_day as the first start_day
    for i in range(1, len(sorted_schedule)):
        next_day, next_interval = sorted_schedule[i]
        if next_interval == current_interval and DAY_TO_INDEX[next_day] == DAY_TO_INDEX[end_day] + 1:
            end_day = next_day
        else:
            add_schedule_entry(start_day, end_day, current_interval)
            start_day, current_interval = next_day, next_interval
            end_day = start_day
            
    add_schedule_entry(start_day, end_day, current_interval)

    return ';'.join(formatted_schedule)

def merge_hour(row1,row2):
    skipped_row=set()
    if row1 is None and row2 is None:
        return None
    if row1 is None:
        return row2
    if row2 is None:
        return row1

    schedule1,error = parse_row(row1)
    skipped_row.add(error)
    schedule2,error = parse_row(row2)
    skipped_row.add(error)

    merged_schedule = merge_schedule(schedule1, schedule2)

    return format_schedule(merged_schedule)

#defs for phone number merging
def merge_phonenumber(row1, row2):
    # Check if both are NaN or None, in which case return None
    if pd.isna(row1) and pd.isna(row2):
        return None
    elif pd.isna(row1):
        return str(int(row2))  # Convert row2 to an integer string if row1 is NaN
    elif pd.isna(row2):
        return str(int(row1))  # Convert row1 to an integer string if row2 is NaN

    # Convert both values to strings after checking they are not NaN
    row1_str = str(int(row1))
    row2_str = str(int(row2))

    # Split and merge numbers into a set to remove duplicates
    phone_numbers = set(row1_str.split(';') + row2_str.split(';'))

    # Join the sorted phone numbers with semicolons and return as a single string
    return ';'.join(sorted(phone_numbers))

def merge_poiname(df, name_column='names.0.name'):
    # Check if the number of rows in df is even
    if len(df) % 2 != 0:
        print("Warning: DataFrame length is odd. This function expects an even number of rows to process in pairs.")
        return None, None
    
    #this function actually select the rigt poi since it needs to enter the right hour and phone after merging
    selected_poiupdate = []
    selected_duplicateid = []
    merged_hour = None
    merged_phone = None
    for i in tqdm(range(0,len(df),2), desc = 'selecting duplicates...'):
        row1 = df.iloc[i]
        row2 = df.iloc[i+1]
        merged_hour = merge_hour(row1['hours'],row2['hours'])
        merged_phone = merge_phonenumber(row1['phone_number.number'], row2['phone_number.number'])
        if i+1 < len(df):
            if len(row1[name_column]) <= len(row2[name_column]):
                selected_row = row2.copy()
                selected_duplicateid.append(row1['site_code'])
            else:
                selected_row = row1.copy()
                selected_duplicateid.append(row2['site_code'])

        selected_row['hours'] = merged_hour
        selected_row['phone_number.number'] = merged_phone

        selected_poiupdate.append(selected_row)

        update_df = pd.DataFrame(selected_poiupdate)
        duplicate_df = pd.DataFrame(selected_duplicateid, columns=['site_code'])

    return update_df, duplicate_df

if __name__ == '__main__':
    param_file = 'parameters.json'
    #load parameters
    param = helpers.load_parameters(param_file)
    
    file_in = param['next_file']
    
    truedup_df = pd.read_csv(file_in).reset_index(drop=True)

    update_df, duplicate_df = merge_poiname(truedup_df)
    update_df.drop_duplicates(subset='site_code', inplace=True)
    duplicate_df.drop_duplicates(subset='site_code', inplace=True)
        
    fileduplicate_out = f'{os.path.splitext(file_in)[0]}_duplicateid_set.csv'
    duplicate_df.to_csv(fileduplicate_out, index=False)
    fileupdate_out = f'{os.path.splitext(file_in)[0]}_updatedid_set.csv'
    update_df.to_csv(fileupdate_out, index=False)
    print(f'{fileduplicate_out} created containing {len(duplicate_df)} duplicate ids')
    print(f'{fileupdate_out} created containing {len(duplicate_df)} ids that needs attribute updating')
