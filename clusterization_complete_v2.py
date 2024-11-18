
import subprocess

scripts = ['trialdata_duplicating.py',
           'json_to_dataframe_conversion.py',
           'data_clusterization.py',
           'cluster_preprocessing.py'
           'pairdata_generation_v2_lev.py',
           'fp_minimalisation.py',
           'sample_generation.py',
           'data_finalization.py'
           ]

error_occurred = False

for script in scripts:
    print(f'running {script}')
    result = subprocess.run(["python", script], stderr=subprocess.STDOUT, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error running {script}: {result.stderr}")            
        error_occurred = True
        break
    else:
        print(f"Finished {script} successfully.")

if not error_occurred:
    print("All scripts ran successfully.")



