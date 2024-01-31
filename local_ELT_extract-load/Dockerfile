FROM python:3.10-slim-buster 

WORKDIR /app

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY . /app 

RUN pip install --no-cache-dir -r load_requirements.txt

ENTRYPOINT ["python", "extract_and_load.py"]