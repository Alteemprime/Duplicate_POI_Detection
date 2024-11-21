import pandas as pd
from math import ceil
import numpy as np
import os
import sys

FILE_IN = 'detected_trueduplicates.csv'

def sample_size_calculation_without_FPC(z=1.96,p=0.5,e=0.05):
    #confidence level 95% translate to 1.96 z score
    #p is estimated proportion of population has the attribute in question. 0.5 for maximum uncertainty (no prior knowledge)
    #e is margin of error (5%)
    result = ((z**2)*p*(1-p)) / (e**2)
    return ceil(result)

def finite_population_correction(N,n):
    #N is sample size without FPC
    #n is your population
    result = N / (1 + ((N-1)/n))
    return round(result)

if __name__ == "__main__" :

    df = pd.read_csv(FILE_IN)
    if os.stat(FILE_IN).st_size == 0:
        print(f'{FILE_IN} does not contain any rows')
        sys.exit(1)
        
    print(f'pair data has shape of {df.shape}')

    sample_set = []
    N = sample_size_calculation_without_FPC()
    n = df.shape[0] // 2
    n_sample = finite_population_correction(N,n)
    #use slicing from index 0 with step = 2
    odd_index_df = df.iloc[0::2]
    #get samples from odd rows
    sample_odd_rows = odd_index_df.sample(n = n_sample, random_state = 42)
    #row 1 pair would be row 2
    sample_even_rows = df.iloc[sample_odd_rows.index + 1]
    sample_set = pd.concat([sample_odd_rows, sample_even_rows], ignore_index = True).sort_values(by='cluster_id')
    sample_set['remark'] = np.nan

    print(f'sample set generated with shape of {sample_set.shape}')

    file_out = 'sample_set_0,7Cosine_0,8Lev.csv'
    sample_set.to_csv(file_out, index = False)

    print(f'{file_out} saved successfully')