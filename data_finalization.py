import pandas as pd
from tqdm import tqdm

cluster_path = 'clustered_subset.csv'
falseduplicates_path = 'detected_falseduplicates.csv'
trueduplicates_path = 'detected_trueduplicates.csv'

truedup_df = pd.read_csv(trueduplicates_path).reset_index(drop=True)

if __name__ == '__main__':
    duplicateid_set = set()
    for i in tqdm(range(0,len(truedup_df),2), desc = 'selecting duplicates...'):
        if i+1 < len(truedup_df):
            if len(truedup_df.iloc[i]['names.0.name']) <= len(truedup_df.iloc[i+1]['names.0.name']):
                duplicateid_set.add(truedup_df.iloc[i]['site_code'])
            else:
                duplicateid_set.add(truedup_df.iloc[i+1]['site_code'])

    duplicateid_list = list(duplicateid_set)

    duplicate_df = pd.DataFrame(duplicateid_list,columns = ['site_code'])    

    file_out = 'duplicateid_set.csv'
    duplicate_df.to_csv(file_out, index=False)
    print(f'{file_out} created containing {len(duplicate_df)} duplicate ids')
