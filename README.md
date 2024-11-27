# Real-time Stock Data ETL Pipeline with Dagster

This project implements an ETL (Extract, Transform, Load) pipeline for processing real-time stock data from Kafka and loading it into a PostgreSQL database. It leverages Dagster for orchestration, asset management, and scheduling.

## Overview

The pipeline performs the following steps:

1. **Extract:** Consumes stock price and volume data from separate Kafka topics.
2. **Transform:**
    * Converts timestamps to datetime objects.
    * Calculates moving averages for price and volume.
    * Joins price and volume data based on symbol and timestamp.
    * Calculates derived metrics (e.g., price-volume ratio).
3. **Load:** Inserts the processed data into PostgreSQL tables, including hourly partitioned tables for stock prices and volume.

## Architecture

The pipeline uses the following components:

* **Kafka:**  Message broker for streaming real-time stock price and volume data.
* **PostgreSQL:** Relational database for storing the processed data.
* **Dagster:** Orchestration tool for managing the ETL process, defining assets, and scheduling jobs.
* **Python:** Programming language used for implementing the ETL logic. Libraries used include `pandas`, `sqlalchemy`, `confluent-kafka`, and `dagster`.

## Implementation Details

### Assets

The pipeline is defined using Dagster assets:

* **`stock_prices_asset`:** Extracts stock price data from Kafka, calculates the price moving average, and loads it into a PostgreSQL table. The table is partitioned hourly based on the timestamp.
* **`stock_volume`:** Extracts stock volume data from Kafka, calculates the volume moving average, and loads it into a PostgreSQL table.
* **`joined_stock_data`:** Joins the `stock_prices_asset` and `stock_volume` data, calculates the price-volume ratio, and loads the combined data into a PostgreSQL table.

### Kafka Client

A custom `KafkaClient` class handles consuming messages from Kafka in batches. It manages offset tracking and commits offsets after successful processing and database insertion. The client also includes error handling and logging.

### PostgreSQL Client

A custom `DBClient` class manages interactions with the PostgreSQL database. It uses SQLAlchemy for database operations and implements connection pooling. The client also includes logic for creating hourly partitions for stock price and volume data. It supports different partition strategies (hourly, daily, monthly) for flexibility. Error handling and retry logic are implemented for robust data loading.

### Dagster Job and Schedule

A Dagster job (`stock_data_job`) defines the dependencies between the assets. It ensures that the `joined_stock_data` asset runs after `stock_prices_asset` and `stock_volume`.

A Dagster schedule (`stock_data_schedule`) runs the `stock_data_job` every 20 minutes. The schedule is configured with an execution timezone of UTC.

## Repo Structure
```
etl-pipeline/  <-  **This is where you have to be, to run this project**
├── etl_pipeline/
│   ├── assets/
│   │   ├── __init__.py
│   │   └── assets.py         # Contains all asset definitions
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── kafka_client.py   # Kafka resource implementation
│   │   └── sql_client.py     # Database resource implementation
│   ├── utils/
│   │   ├── __init__.py
│   │   └── constants.py      # Configuration constants
│   ├── __init__.py
│   └── definitions.py        # Dagster definitions
├── tests/
│   ├── __init__.py
│   ├── test_assets.py
│   ├── test_kafka_client.py
│   └── test_sql_client.py
├── setup.py                  # Package installation
├── requirements.txt          # Dependencies
├── pyproject.toml           # Build configuration
├── README.md                # Documentation
└── docker-compose.yml       # Service configuration
```
## Prerequisites

1. **Python**: Ensure Python 3.7+ is installed.
2. **Docker and Docker Compose**: Install Docker to manage containers.
3. **PostgreSQL Client**: Use tools like [DBeaver](https://dbeaver.io/) or [PgAdmin](https://www.pgadmin.org/) for database interactions.

## Tests

```python
# To run tests execute this-
pytest etl_pipeline_tests
```
---

## Setup Instructions(Follow in the order they are mentioned)

### 1. Create and Activate a Virtual Environment
```bash
# You should be in the parent etl-pipeline directory
cd etl-pipeline
# Create the virtual environment
python -m venv {virtual_env_name}

# Activate the virtual environment
source {virtual_env_name}/bin/activate
## Usage (Follow in the order they are mentioned)
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Start Docker Containers
```bash
docker-compose up -d
```

### 4. Start the Data Source
##### Run the stock data generator script to produce stock data into Kafka topics:
```bash
python3 kafka_producer/stock_data_generator.py
```
##### You can monitor the Kafka topic data by visiting http://localhost:8080/topics.


### 5. Start Dagster
##### Open a new terminal instance, activate the virtual environment, and start Dagster:
```bash
pip install -e ".[dev]" && dagster dev
```
---
## Running the Pipeline
1. Launch the Pipeline
Use the Dagster UI to run the pipeline (stock_data_job).

2. View Processed Data
Processed stock data is stored in the PostgreSQL database. You can query the following tables:
``` 
* stock_prices: Hourly partitioned stock price data.
* stock_volumes: Unpartitioned stock volume data.
* stock_analytics_data: Processed and joined data from stock prices and volumes.
```
---

## Database Configuration and Queries
### Connection String
To connect to the PostgreSQL database:

```sql
-- If the configurations are not tweaked
postgresql://myuser:mypassword@localhost:5432/mydatabase
```
### Sample Queries
View stock prices:

```sql

--View Stock Prices
SELECT * FROM stock_prices;
View stock volumes:

--View Stock volumes traded
SELECT * FROM stock_volumes;
View joined analytics data:

--View Stock joined data
SELECT * FROM stock_analytics_data;
Check table partitions:

--View Stock Prices partitions
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    pg_partition_tree(c.oid) AS partition_tree
FROM
    pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE
    c.relname = 'stock_prices';

```

## Future Improvements

* **Data Quality:** Implement data quality checks to ensure data integrity.
* **Bundle Spark:** Implement data quality checks to ensure data integrity.
* **Alerting:**  Integrate alerting for pipeline failures.
* **Backfilling:** Implement a mechanism for backfilling historical data.
* **Testing:** Add more comprehensive unit and integration tests.
* **Dockerization:** Containerize the pipeline for easier deployment.
* **Scalability:** Explore options for scaling the pipeline to handle larger data volumes.


## Contributing

Contributions are welcome! Please open an issue or submit a pull request. 
