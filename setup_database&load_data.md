# SETUP THE DATABASE

## setup docker container for database
```sh
docker network create green-taxi-nw
```

```sh
docker run -d --name postgres-green-con \
    --network=green-taxi-nw \
    -e POSTGRES_PASSWORD=green \
    -e POSTGRES_DB=ny-green-taxi-DB \
    -p 5432:5432 \
    postgres:13.2
```
    -v $(pwd)/db-data:/var/lib/postgresql/data \
```sh
docker ps
```
```sh
docker network connect green-taxi-nw <CONTAINER_ID>
```
```sh
docker exec -it postgres-green-con psql -U postgres 
```
```sh
docker inspect -f "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}" <CONTAINER_ID>
```

## write files for extract and load steps

### new folder for extract and load task
```sh
mkdir -p local-ELT_extract-load/data
cd local-ELT_extract-load
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
    postgres_DB = 'postgresql://postgres:green@172.21.0.2:5432/ny-green-taxi-DB'
    table_name = 'rides_jan-feb-mar'
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

build docker image
```sh 
cd local-ELT_extract-load
```

```sh
docker build -t extract_and_load .
```

fill database with data:
```sh
docker run --rm -it --network=green-taxi-nw extract_and_load
```

postgres=#
```sh
\c ny-green-taxi-DB
```

ny-green-taxi-DB=#
```sh
\dt
```
=> rides_jan-feb-mar

ny-green-taxi-DB=#
```sh
SELECT COUNT(*) FROM gtaxi_2021_qt1;
```

=> 224917 rows

 0.0.0.0:5432->5432/tcp 

 sudo rm -r db-data

 POSTGRES_HOST_AUTH_METHOD=trust

 rides_jan_feb_mar