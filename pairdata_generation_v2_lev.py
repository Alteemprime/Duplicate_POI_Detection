import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import Levenshtein as lev
from tqdm import tqdm
import time

def validate_thresholds(cos_threshold, lev_threshold):
    """
    Validates the cosine and Levenshtein thresholds.
    Raises ValueError if any threshold is invalid.
    """
    if cos_threshold is not None and (cos_threshold < 0.0 or cos_threshold > 1.0):
        raise ValueError("Cosine threshold must be between 0.0 and 1.0")
    if lev_threshold is not None and (lev_threshold <= 0.0 or lev_threshold > 1.0):
        raise ValueError("Levenshtein threshold must be greater than 0.0 and less than or equal to 1.0")

def create_similarity_pair_df(df, name_column = 'names.0.name', clusterid_column = 'cluster_id', cos_threshold = 0.0, lev_threshold = 0.0 ):
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
                
                    if cosine_simratio(string1,string2) > cos_threshold :
                        # Store both rows as a tuple
                        rows.append((cluster_df.iloc[i], cluster_df.iloc[j]))
                    elif lev_ratio(string1,string2) > lev_threshold :
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

COSINE_THRESHOLD = None
LEV_THRESHOLD = None

if __name__ == "__main__":
    file_path = 'clustered_subset_preprocessed.csv'
    try:
        validate_thresholds(COSINE_THRESHOLD,LEV_THRESHOLD)
        clustered_potential_duplicates_df = pd.read_csv(file_path)
        similarity_pair_df = create_similarity_pair_df(clustered_potential_duplicates_df,name_column = 'names.0.name_preprocessed', cos_threshold = COSINE_THRESHOLD, lev_threshold = LEV_THRESHOLD)
        similarity_pair_df = assign_pair_id(similarity_pair_df)
        #save relevant output
        file_output = 'pairoutput_0,7Cosine_0,8Lev.csv'
        similarity_pair_df.to_csv(file_output, index=False)
        print(f'pair output file saved as {file_output} containing {len(similarity_pair_df)/2} pairs')
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")