import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from minio import Minio

# Initialize MinIO client
minio_client = Minio('localhost:9000',
                     access_key='admin',
                     secret_key='minio123',
                     secure=False)

bucket_name = 'de-challenge'
base_folder_path = 'fire_incidents/'

# Get a list of objects (Parquet files) in the bucket
objects = [obj for obj in minio_client.list_objects(bucket_name, prefix=base_folder_path, recursive=True) if obj.object_name.endswith('.parquet')]

print(objects)

dfs = []

# Iterate through the Parquet files and load them into a Pandas DataFrame
for obj in objects:
    # Read Parquet file from MinIO
    file_obj = minio_client.get_object(bucket_name, obj.object_name)
    parquet_file = BytesIO(file_obj.read())

    # # Read Parquet file into a Pandas DataFrame
    table = pq.read_table(parquet_file)
    df = table.to_pandas()

    # Extract partition details from the object name
    partition_details = obj.object_name.split('/')
    partition_values = [detail for detail in partition_details if '=' in detail]

    # Add partition columns to the DataFrame
    for partition_value in partition_values:
        partition_key, partition_val = partition_value.split('=')
        df[partition_key] = partition_val

    # Append the DataFrame to the list
    dfs.append(df)

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(dfs, ignore_index=True)