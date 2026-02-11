
# User Guide

## Requirements
- Docker
- Docker Compose
- Python 3 (for running the event generator locally)

## Project Setup
### Setup Instructions:

1. **Clone the repository**  
   First, clone the repository to your local machine:
   ```bash
   git clone https://github.com/GEssuman/data-engineering-labs.git
   cd data-engineering-labs


3. **Create `.env` file**
    Before running the project, creat a `.env` file from the provided example:
    ```bash
    cp .env.example .env
    ```
    Then edit the `.env` with your environment-specific values. 
    This will configure credentials, database settings, and other envrionment variable. 

4. **Build Docker Image**
    This projject requires a custome spark image. Build it using the Dockefile in the repository.
    ```bash
    docker build -t my-spark-image:1.2 .
    ```
5. **Run Docker Compose**
    Start the services defined in the docker-compose.yml:
    ```
    docker-compose up -d
    ```

6. **Verify that the services are running**
    After running the command, the services (PostgreSQL and Spark) will be up and running in your Docker environment. You can check the status of the containers by running:
    ```
    docker ps
    ```

    ## Services Running:
    ### Running Services

| Service           | Description                           | Access URL / Port         |
|------------------|---------------------------------------|---------------------------|
| PostgreSQL DB     | PostgreSQL container for event storage | `localhost:5433`          |
| Spark Master      | Spark master node for cluster management | `localhost:8981`          |
| Spark Worker      | Spark worker node for executing tasks | Internal only             |
| Spark Driver UI   | Spark streaming query monitoring       | `localhost:4040`          |
| Prometheus        | Metrics collection server              | `localhost:9090`          |
| Grafana           | Dashboard for visualizing metrics      | `localhost:3000`          |



## Running Event Generator

```bash
cd scripts/python
python data_generator.py
```

## Running Spark Structured Streaming

1. Access Spark master container
```bash
docker exec -it spark-master bash
cd /opt/real-time-spark/spark/apps
```

2. Run Spark application
```bash
spark-submit \
  --jars /opt/real-time-spark/spark/resources/postgresql-42.7.2.jar \
  spark_streaming_to_postgres.py
```
OR
```bash
spark-submit \
  --jars /opt/real-time-spark/spark/resources/postgresql-42.7.2.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.6.1 \
  spark_streaming_to_postgres_from_kafka.py

```


*Notes:*
-
- If you modify the streaming script, stop the current Spark query before restarting.

- Metrics are collected via Prometheus; Grafana dashboards can visualize input rows, processed rows, batch durations, and backlog metrics.

- Checkpoint locations are configured in the script — deleting them resets stream offsets.


## Grafana Dashboard

A ready-made Grafana dashboard is available to visualize Spark Streaming metrics.  

### Import Steps
## Step 1: Create Prometheus Data Source
1. Open Grafana at `http://localhost:3000`  
2. Go to Connections → Data Sources
3. Click Add data source
4. Select Prometheus
5. Set the URL to:
    ```code
        http://prometheus:9000
    ```

6. Click Save & Test
    You should see:
        Data source is working

## Step 2: Import the Dashboard
2. Go to **Dashboard → Import**  
3. Paste the JSON snippet provided in this repository. `grafana_dashboard.json`
4. Click **Import**  

## Step 3: Update Data Source in Dashboard Panels
After importing:

1. Open the imported dashboard
2. Click **Edit** on any panel
3. Under **Query → Data Source**, select: `Prometheus`
4. Repeat this for all panels (if not automatically set)
5. Click **Save Dashboard**


The dashboard includes:

- **Input Rows/sec** – streaming ingestion rate  
- **Processed Rows/sec** – processing throughput  
- **Batch Duration / Falling Behind** – stat panel showing lag  

---

## What This Helps You Monitor

- Whether Spark is keeping up with incoming data  
- If batch duration exceeds trigger interval  
- Throughput trends over time  
- Streaming backpressure issues 

