import os
import pandas as pd 
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from prefect import flow, task
from dotenv import load_dotenv
from prefect_dbt.cli.commands import DbtCoreOperation

@task(name="EXTRACT", retries=2, retry_delay_seconds=33, log_prints=True)
def extract():
    for month in range(1,2):
        local_path = f'data/green_tripdata_2021-{month:02}.parquet'
        url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-{month:02}.parquet"
        os.system(f'wget {url} -O {local_path}')
    df_green_taxi = pd.read_parquet(f'data/')
    return df_green_taxi

@task(name="LOAD", log_prints=True)
def load(df:pd.DataFrame , postgres_DB:str, table_name:str):
    engine = create_engine(postgres_DB)
    print(f'engine created for {postgres_DB}')
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    for month in range(1,2):
        local_path = f'data/green_tripdata_2021-{month:02}.parquet'
        parquet_file = pq.ParquetFile(local_path)
        for batch in parquet_file.iter_batches(batch_size=100000):
                batch_df = batch.to_pandas()
                batch_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print('all months processed!')

@task(name="TRANSFORM")
def dbt_transform():
    result = DbtCoreOperation(
        commands=["pwd", "dbt debug", "dbt run"],
        overwrite_profiles=False,
        profiles_path='profiles.yml',
        project_dir='.'
    ).run()
    return result     

@flow(name="ELT")
def elt_flow():
    df = extract()
    load_dotenv()
    postgres_DB = os.getenv("DB_CONN")
    table_name = 'green_taxi_2021_qt1'
    load(df, postgres_DB, table_name) 
    dbt_transform()          

if __name__ == "__main__": 
    elt_flow()

