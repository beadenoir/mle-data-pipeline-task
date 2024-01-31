import os
import pandas as pd 
from sqlalchemy import create_engine
import pyarrow.parquet as pq

def extract():
    for month in range(1,4):
        local_path = f'data/green_tripdata_2021-{month:02}.parquet'
        url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-{month:02}.parquet"
        os.system(f'wget {url} -O {local_path}')
    df_green_taxi = pd.read_parquet(f'data/')
    return df_green_taxi

def load(df:pd.DataFrame, postgres_DB:str, table_name:str):
    engine = create_engine(postgres_DB)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    for month in range(1,4):
        local_path = f'data/green_tripdata_2021-{month:02}.parquet'
        parquet_file = pq.ParquetFile(local_path)
        for batch in parquet_file.iter_batches(batch_size=100000):
                batch_df = batch.to_pandas()
                batch_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print('all months processed!')

        

if __name__ == "__main__": 
    df_green_taxi = extract()
    postgres_DB = 'postgresql://postgres:green@172.21.0.2:5432/ny-green-taxi-DB'
    table_name = 'gtaxi_2021_qt1'
    load(df_green_taxi, postgres_DB, table_name) 
    