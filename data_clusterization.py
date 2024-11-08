import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from scipy.spatial import cKDTree
from tqdm import tqdm
import time

R = 6371000

def latlon_to_cartesian(lat, lon):
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon)
    
    x = R * np.cos(lat_rad) * np.cos(lon_rad)
    y = R * np.cos(lat_rad) * np.sin(lon_rad)
    z = R * np.sin(lat_rad)
    
    return np.array([x, y, z])

#clusterization
def find_cluster_on_assigned_idx(centroid_idx, gdf, tree, MAX_DISTANCE, MAX_CLUSTER_SIZE, clustered_points):
    centroid_cartesian = gdf.iloc[centroid_idx]['cartesian_coords']
    category = gdf.iloc[centroid_idx]['apple_categories.0']
    site_code = gdf.iloc[centroid_idx]['site_code']
    
    distances, indices = tree.query(centroid_cartesian, k=len(gdf), distance_upper_bound=MAX_DISTANCE) 

    has_neighbours = False #reset has_neigbour as False for every new cluster calculation
    valid_checks = 0 #counter to test max member per cluster
    
    sitecode_dict = {}
    sitecode_dict[centroid_idx] = [(centroid_idx, site_code)]
    
    for i, idx in enumerate(indices):
        if distances[i]==float('inf'): #if it found inf distance, distance returned by tree already sorted
            break
        if idx < len(gdf) and gdf.iloc[idx]['apple_categories.0'] == category and idx not in clustered_points: #safeguards against out of bound index produced by tree query
            sitecode_dict[centroid_idx].append((idx,gdf.iloc[idx]['site_code'])) #set default create new key if not exist, else use existing key
            has_neighbours = True
            valid_checks += 1
        if valid_checks >= MAX_CLUSTER_SIZE:
            break
    if not has_neighbours: #means condition will set to True (has no neighbour) so subsequent code will be processed
        sitecode_dict[centroid_idx] = [(centroid_idx, site_code)]
    
    return sitecode_dict

def cluster_points(gdf):
    #building spatial index
    # Start the timer
    start_time = time.time()
    print('building spatial index...')
    coordinates = np.vstack(gdf['cartesian_coords'].values)
    tree = cKDTree(coordinates)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Spatial index built in {elapsed_time:.2f} seconds.")
    
    clustered_points = set() #container to track points already clustered    
    sitecode_to_cluster_id = {} #container of sitecode to cluster_id mapping    
    cluster_counter = 0 #inizialisation of the first cluster id
    
    start_time = time.time()    
    for centroid_idx in tqdm(range(len(gdf)), desc='clustering data points...'):
        if centroid_idx in clustered_points :
            continue #skip ids already in clustered_points
        
        cluster_dict = find_cluster_on_assigned_idx(centroid_idx,gdf, tree, MAX_DISTANCE,
                                              MAX_CLUSTER_SIZE, clustered_points)
        #points marking
        for centroid_id, site_codes in cluster_dict.items():            
            for idx, site_code in site_codes:
                clustered_points.add(idx)  # Mark point as clustered
                sitecode_to_cluster_id[site_code] = cluster_counter  # Assign cluster ID to site codes
        
        #populate all resulting cluster, cluster counter referenced the index on which the cluster was being appended        
        cluster_counter += 1
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Clustering completed in {elapsed_time:.2f} seconds.")    
    return sitecode_to_cluster_id

if __name__ == "__main__":
    file_in = 'subset_data.csv'
    #flatten json can result bad data structure, if number of column is inconsistent
    #this is hard coded to 16 because resulting file 17th field which key are not detected on the json data
    df= pd.read_csv(file_in) 

    print(f'finished reading {file_in}')
    print(df.info())
    
    # Convert coordinates to numeric, coercing errors due to bad formatting, there are inconsistent column numbers in the data
    df['display_point.coordinates.longitude'] = pd.to_numeric(df['display_point.coordinates.longitude'], errors='coerce')
    df['display_point.coordinates.latitude'] = pd.to_numeric(df['display_point.coordinates.latitude'], errors='coerce')

    # Count and print the number of invalid rows (non-floats in latitude/longitude)
    invalid_rows = df[df['display_point.coordinates.longitude'].isna() | df['display_point.coordinates.latitude'].isna()]
    print(f'Number of invalid rows removed: {len(invalid_rows)}')
    invalid_rows.to_csv('invalid_rows.csv', index=False)

    # Filter the DataFrame to only include rows where latitude and longitude are valid floats
    df = df.dropna(subset=['display_point.coordinates.longitude', 'display_point.coordinates.latitude'])

    print(f'After filtering: {len(df)} rows remaining.')

    #convert df to gdf
    gdf = gpd.GeoDataFrame(df)
    gdf['geometry'] = gpd.points_from_xy(df['display_point.coordinates.longitude'], df['display_point.coordinates.latitude'])
    gdf.set_geometry('geometry', inplace=True)
    gdf.set_crs(epsg=4326, inplace=True)

    # Precompute Cartesian coordinates for all points and store in a new column
    gdf['cartesian_coords'] = gdf.apply(lambda row: latlon_to_cartesian(row.geometry.y, row.geometry.x), axis=1)

    MAX_DISTANCE = 100  # max distance in meters
    MAX_CLUSTER_SIZE = 3

    sitecode_to_cluster_id = cluster_points(gdf)

    #convert to dataframe
    sitecode_to_clusterid_df = pd.DataFrame(sitecode_to_cluster_id.items(), columns = ['site_code','cluster_id'])

    #check shape
    print('complete cluster information :')
    print(sitecode_to_clusterid_df.info())

    #Check those who have more than 1 cluster member 
    # Group by 'cluster_id' and filter those that have more than 1 'site_code'
    duplicate_clusters = sitecode_to_clusterid_df.groupby('cluster_id').filter(lambda x: len(x) > 1)

    print('duplicate cluster information :')
    print(duplicate_clusters.info())

    merged_df = pd.merge(duplicate_clusters,df, on = 'site_code', how = 'left')
    file_out = 'clustered_subset.csv'
    merged_df.to_csv(file_out, index=False)