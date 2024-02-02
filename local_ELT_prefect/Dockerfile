FROM prefecthq/prefect:2-python3.10

WORKDIR /app

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY . /app

COPY profiles.yml /root/.dbt/

RUN pip install -r ELT_requirements.txt --no-cache-dir --trusted-host pypi.python.org

ENTRYPOINT ["python", "extract_and_load_prefect.py"]