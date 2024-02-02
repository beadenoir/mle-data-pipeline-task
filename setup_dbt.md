# SETUP DBT PROJECT
## Setup a virtual environment and install dbt with postgres adapter
### Environment 
```sh
pyenv local 3.11.3
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install dbt-postgres
```

### or Miniconda Environment 
```sh
conda create -n .10venv python=3.11.3 pip
conda activate .10venv
pip install dbt-postgres
```

## Create local dbt project
in root folder of repository:
```sh
dbt init local_ELT_transform
```

inside local_ELT_transform folder:

+ create ``profiles.yml`` 
```yml
postgres:
  outputs:
    prod:
        type: postgres
        threads: 4
        host: "{{ env_var('POSTGRES_HOST') }}"
        port: 5432
        user: "{{ env_var('POSTGRES_USER') }}"
        pass: "{{ env_var('POSTGRES_PASSWORD') }}"
        dbname: "{{ env_var('POSTGRES_DB') }}"
        schema: prd

  target: prod
```


+ alter `dbt_project.yml`

    - change name: 'green_taxi_trips' and match  in models section
    - change profile: 'postgres' 
    - change models folder tree

```yml
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'green_taxi_trips'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: 'postgres'

.
.
.

# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

models:
  green_taxi_trips:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
    staging:
      +materialized: table
      +schema: staging
    mart:
      +materialized: table
      finance:
        +schema: mart
```

## Create dbt models
```sh
cd local_ELT_transform/models && mkdir -p staging mart/finance
```

in the staging models folder:

+ `src_gtaxi.yml` 

```yaml
version: 2

sources:
  - name: src_gtaxi
    schema: public
    tables:
      - name: green_taxi_2021_qt1
```

+ `stg_gtaxi.sql` 

```sql
{{ config(materialized='table', schema='stg') }}

SELECT 
    "VendorID" AS vendor_id,
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
    store_and_fwd_flag,
    "RatecodeID" AS rate_code_id,
    "PULocationID" AS pickup_location_id,
    "DOLocationID" AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge

FROM {{ source('src_gtaxi','grenn_taxi_2021_qt1') }}
```


In the mart/finance folder:
+ `fct_revenue_per_day.sql`

```sql
{{ config(materialized='table', schema='finance') }}

SELECT date(pickup_datetime) AS day, 
SUM(fare_amount
    + extra
    + mta_tax
    + tolls_amount
    + improvement_surcharge 
    + congestion_surcharge) AS total_revenue
FROM {{ ref('stg_gtaxi') }}
GROUP BY day
ORDER BY day ASC
```

inside local_ELT_transform:
+ ``Dockerfile`` 

```dockerfile
FROM python:3.10-slim-buster

RUN apt-get update 

WORKDIR /app

COPY . /app

COPY profiles.yml /root/.dbt/

RUN pip install --no-cache-dir dbt-postgres

ENTRYPOINT [ "dbt" ]
```

## Build the docker image

```sh
docker build -t dbt-gtaxi .
```

## Run the docker image

```sh
docker run -it --rm -e POSTGRES_USER='postgres' \
    --network=green-taxi-nw \
    -e POSTGRES_PASSWORD='green' \
    -e POSTGRES_HOST='postgres-green-con' \
    -e POSTGRES_DB='ny-green-taxi-DB' \
    --name dbt_gtaxi_daily_revenue \
    dbt-gtaxi run
```

