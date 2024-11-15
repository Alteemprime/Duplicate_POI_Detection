import pandas as pd
import re

#this dict should focus only making impact on tokenization process
token_similarity_dict_indo ={'rm' : 'rumah makan',
                           'src' : 'kelontong',
                           'dr' : 'dokter',
                           'ggsp' : 'kelontong',
                           'wr' : 'warung',
                           'foto copy' : 'fotocopy',
                           'tb' : 'toko bangunan',
                           'ii' : '2',
                           'dental' : 'gigi',
                           #'cell' : 'phone',
                           #'cellular' : 'phone',
                           'drg' : 'dokter gigi',}

def safe_lowercase(row):
    try:
        return row['names.0.name'].lower()
    except AttributeError as e:
        cluster_id = row['cluster_id']
        name_value = row['names.0.name']
        print(f"Error lowercasing name '{name_value}' (Cluster ID: {cluster_id}) because of {e}")
        return name_value

def replace_locale_specific_tokens(name, locale):
    if locale == 'en-ID':
        # Replace based on the Indo dictionary
        for key, value in token_similarity_dict_indo.items():
            name = re.sub(rf'\b{re.escape(key)}\b', value, name)
    return name

def preprocess_data(df,name_column,locale_column='names.0.locale' ):    
    df[name_column + '_preprocessed'] = df.apply(safe_lowercase, axis=1)
    
    df[name_column + '_preprocessed'] = df[name_column + '_preprocessed'].apply(lambda x: re.sub(r"[.\-&'/]|'s", ' ', x))
    # Check locale for each row and apply token replacement if locale is 'en-ID'
    if locale_column in df.columns:
        df[name_column + '_preprocessed'] = df.apply(
            lambda row: replace_locale_specific_tokens(row[name_column + '_preprocessed'], row[locale_column]), 
            axis=1
        )    
    
    return df

file_in = 'clustered_subset.csv'
file_out = 'clustered_subset_preprocessed.csv'

if __name__ = "__main__":
    df = pd.read_csv(file_in)
    df.info()
    preprocessed_df = preprocess_data(df,'names.0.name')
    preprocessed_df['name.set'] = preprocessed_df['names.0.name_preprocessed'].apply(lambda name: set(name))
    preprocessed_df[['cluster_id','names.0.name_preprocessed','name.set']].head()
    preprocessed_df.to_csv(file_out)

