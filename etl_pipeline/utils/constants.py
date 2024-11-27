from sqlalchemy import text
# Database configuration
STOCK_PRICE_TABLE = 'stock_prices'
STOCK_VOLUME_TABLE = 'stock_volumes'
STOCK_ANALYTICS_DATA = 'stock_analytics_data'
# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092'
KAFKA_BATCH_SIZE = 10000
KAFKA_BATCH_TIMEOUT = 5.0
STOCK_PRICE_TOPIC = 'stock_prices'
STOCK_VOLUME_TOPIC = 'stock_volume'

# Database queries
stock_prices_create_query = text(f"""
        CREATE TABLE IF NOT EXISTS {STOCK_PRICE_TABLE} (
            id BIGSERIAL,
            symbol VARCHAR(6) NOT NULL,
            price DOUBLE PRECISION,
            open_price DOUBLE PRECISION,
            high_price DOUBLE PRECISION,
            low_price DOUBLE PRECISION,
            timestamp TIMESTAMP NOT NULL,
            processed_at TIMESTAMP NOT NULL,
            price_moving_avg DOUBLE PRECISION,
            PRIMARY KEY (symbol, id, timestamp)
        ) PARTITION BY RANGE (timestamp);
        """)

stock_volume_create_query = text(f"""
          CREATE TABLE IF NOT EXISTS {STOCK_VOLUME_TABLE} (
            id BIGSERIAL,
            symbol VARCHAR(6) NOT NULL,
            volume DOUBLE PRECISION,
            trading_volume_moving_avg DOUBLE PRECISION,
            timestamp TIMESTAMP NOT NULL,
            processed_at TIMESTAMP NOT NULL,
            PRIMARY KEY (symbol, id)
        );
        """)      

create_joined_table_query = text(f"""
                                 CREATE TABLE IF NOT EXISTS {STOCK_ANALYTICS_DATA} (
            id BIGSERIAL,
            symbol VARCHAR(6) NOT NULL,
            price DOUBLE PRECISION,
            open_price DOUBLE PRECISION,
            high_price DOUBLE PRECISION,
            low_price DOUBLE PRECISION,
            timestamp TIMESTAMP NOT NULL,
            price_moving_avg DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            trading_volume_moving_avg DOUBLE PRECISION,
            price_volume_ratio DOUBLE PRECISION,
            processed_at TIMESTAMP NOT NULL,
            PRIMARY KEY (symbol, timestamp)
        );
        """)
