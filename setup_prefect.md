# SETUP PREFECT
## Setup a virtual environment and install requirements
### Environment 
```sh
pyenv local 3.11.3
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

### or Miniconda Environment 
```sh
conda create -n .10venv python=3.11.3 pip
conda activate .10venv
pip install -r requirements.txt
```

## Make new Folders for this service
```sh
mkdir -p local_ELT_prefect/data
cd local_ELT_prefect
```

### create these files in the folder
+ ``requirements.txt``
````text
pandas==2.1.4
pyarrow==14.0.1
SQLAlchemy==2.0.23
psycopg2-binary==2.9.9
python-dotenv==1.0.0
prefect==2.14.9
prefect-dbt==0.4.1
dbt-postgres==1.7.6
````

+ `.env`
```text
DB_CONN= postgresql://postgres:green@postgres-green-con:5432/ny-green-taxi-DB
POSTGRES_USER='postgres'
POSTGRES_PASSWORD='green'
POSTGRES_HOST = 'postgres-green-con'
POSTGRES_DB='ny-green-taxi-DB'
DOCKER_NETWORK='green-taxi-nw'
PREFECT_API_URL=http://host.docker.internal:4200/api
DOCKER_NETWORK='green-taxi-nw'
```

+ `ELT_prefect.py`
```python
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
```
+ `profiles.yml`
```yml
postgres:
  outputs:
    prod:
        type: postgres
        threads: 4
        host: postgres-green-con
        port: 5432
        user: postgres
        pass: green
        dbname: ny-green-taxi-DB
        schema: prd

  target: prod
```

+ `Dockerfile`
```Dockerfile
FROM prefecthq/prefect:2-python3.10

WORKDIR /app

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY . /app

COPY profiles.yml /root/.dbt/

RUN pip install -r ELT_requirements.txt --no-cache-dir --trusted-host pypi.python.org

ENTRYPOINT ["python", "extract_and_load_prefect.py"]
```
### Copy folder/files from local_ELT_transform to local_ELT_prefect
+ `models`
+ `dbt_project.yml`

### Build Docker image
```
cd local_ELT_prefect
```

```sh
docker build -t prefect_elt .
```
### run docker container

```sh
prefect server start
```
```sh
docker run -it --rm  \
    --network=green-taxi-nw \
    -e PREFECT_API_URL=http://host.docker.internal:4200/api \
    prefect_elt
```
