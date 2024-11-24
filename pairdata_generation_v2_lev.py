import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import Levenshtein as lev
from tqdm import tqdm
import time
import os
import sys
import helpers

def validate_thresholds(cos_threshold, lev_threshold):
    """
    Validates the cosine and Levenshtein thresholds.
    Raises ValueError if any threshold is invalid.
    """
    if cos_threshold is not None and (cos_threshold < 0.0 or cos_threshold > 1.0):
        raise ValueError("Cosine threshold must be between 0.0 and 1.0")
    if lev_threshold is not None and (lev_threshold <= 0.0 or lev_threshold > 1.0):
        raise ValueError("Levenshtein threshold must be greater than 0.0 and less than or equal to 1.0")

def create_similarity_pair_df(df, name_column = 'names.0.name', clusterid_column = 'cluster_id'):
    cos_enble = param['cosine']['enabled']
    lev_enble = param['levenshtein']['enabled']
    
    rows = []
    # Start the timer
    start_time = time.time()            
    for cluster_id, cluster_df in tqdm(df.groupby(clusterid_column),desc = 'creating pair data...'):
        #added this because i was slicing 1000 rows, last cluster_id might only have 1 data
        if len(cluster_df) < 2:
            print(f"Skipping cluster {cluster_id} - not enough members.")  # Debugging line
            continue
        try :
            for i in range(len(cluster_df)):
                for j in range(i+1, len(cluster_df)):
                    string1 = cluster_df.iloc[i][name_column]#init 2 strings to be compared
                    string2 = cluster_df.iloc[j][name_column]
                
                    if cos_enble and cosine_simratio(string1,string2) >= cos_thold :
                        # Store both rows as a tuple
                        rows.append((cluster_df.iloc[i], cluster_df.iloc[j]))
                    elif lev_enble and lev_ratio(string1,string2) >= lev_thold :
                        rows.append((cluster_df.iloc[i], cluster_df.iloc[j]))
            #print(f'rows in for block : {rows}')
        except ValueError as e:
            print(f"Error processing cluster {cluster_id}: {e}")
            continue  # Skip this cluster and move to the next one
    #print(f'rows after for block : {rows}')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"pairing completed in {elapsed_time:.2f} seconds.")                
    if rows:  # Check if rows have been collected
        result_df = pd.DataFrame([row for pair in rows for row in pair], columns=df.columns) #for pair in rows outer loop, for row in pair inner loop, since each row containing tuple of pair
    else:
        result_df = pd.DataFrame(columns=df.columns)
     
    return result_df

def assign_pair_id(df):
    df = df.reset_index(drop=True)
    df['pair_id'] = (df.index//2) + 1
    return df

def cosine_simratio(string1,string2):
    pair = [string1,string2]
    vectorizer = CountVectorizer()
    vectors = vectorizer.fit_transform(pair).toarray()
    #print(vectors)
    cos_sim = cosine_similarity(vectors)
    return cos_sim[0][1]

def lev_ratio(string1,string2):
    return lev.ratio(string1,string2)

if __name__ == "__main__":
    param_file = 'parameters.json'
    #load parameters
    param = helpers.load_parameters(param_file)
    #set default lev and cosine as true
    helpers.update_parameters(cosine['enabled'] = True)
    helpers.update_parameters(levenshtein['enabled'] = True)
    
    cos_thold = param['cosine']['threshold']
    lev_thold = param['levenshtein']['threshold']
       
    if cos_thold == 0:
        helpers.update_parameters(cosine['enabled'] = False)        
    if lev_thold == 0:
        helpers.update_parameters(levenshtein['enabled'] = False)

    file_in = param['next_file']
    try:
        validate_thresholds(cos_thold,lev_thold)
        clustered_potential_duplicates_df = pd.read_csv(file_in)
        similarity_pair_df = create_similarity_pair_df(clustered_potential_duplicates_df,name_column = 'names.0.name_preprocessed')
        similarity_pair_df = assign_pair_id(similarity_pair_df)
        #save relevant output
        file_out = f'{os.path.splitext(file_in)[0]}_c{cos_thold}_l{lev_thold}.csv'
        similarity_pair_df.to_csv(file_out, index=False)
        helpers.update_parameters('next_file' = file_out)
        print(f'pair output file saved as {file_output} containing {len(similarity_pair_df)/2} pairs')
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
    if os.stat(file_output).st_size == 0:
        print(f'{file_output} does not contain any rows')
        sys.exit(1)
    else:
        with open('fileoutputbuffer_info.txt', 'w') as f:
        f.write(file_output)