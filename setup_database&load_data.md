# SETUP THE DATABASE

## Setup a docker network and container for the database
```sh
docker network create green-taxi-nw
```

```sh
docker run -d --name postgres-green-con \
    --network=green-taxi-nw \
    -e POSTGRES_PASSWORD=green \
    -e POSTGRES_DB=ny-green-taxi-DB \
    -p 5432:5432 \
    -v ./db-data:/var/lib/postgresql/data \
    postgres:13.2
```

## Created python files and a Dockercontainer for extract and load steps

### added new folders for extract and load task
```sh
mkdir -p local_ELT_extract-load/data
cd local_ELT_extract-load
```

+ ``extract_and_load.py``
```python
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
    postgres_DB = 'postgresql://postgres:green@postgres-green-con:5432/ny-green-taxi-DB'
    table_name = 'green_taxi_2021_qt1'
    load(df_green_taxi, postgres_DB, table_name) 
```
+ ``Dockerfile``
```docker
FROM python:3.10-slim-buster 

WORKDIR /app

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY . /app 

RUN pip install --no-cache-dir -r load_requirements.txt

ENTRYPOINT ["python", "extract_and_load.py"]
```


+ ``load_requirements.txt``
```text
pandas
pyarrow
sqlalchemy
psycopg2-binary
```

### built docker image
```sh
docker build -t extract_and_load .
```

### filled the database with data:
```sh
docker run --rm -it --network=green-taxi-nw extract_and_load
```
## Connected to the database
```sh
docker exec -it postgres-green-con psql -U postgres 
```
postgres=#
```sh
\c ny-green-taxi-DB
```
ny-green-taxi-DB=#

### checked for loaded data
```sh
\dt
```
&rarr; green_taxi_2021_qt1

ny-green-taxi-DB=#

```sh
SELECT COUNT(*) FROM green_taxi_2021_qt1;
```

&rarr; 224917 rows
