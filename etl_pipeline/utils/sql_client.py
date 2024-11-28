from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
import pandas as pd
from datetime import datetime
from typing import Optional, List
from dagster import AssetExecutionContext
from enum import Enum
from contextlib import contextmanager
from etl_pipeline.utils.utils import get_table_query
from etl_pipeline.utils.constants import *

class PartitionStrategy(Enum):
    HOURLY = 'hourly'
    DAILY = 'daily'
    MONTHLY = 'monthly'

class DBError(Exception):
    """Base exception for database operations"""
    pass

class DBClient:
    """Database client for handling PostgreSQL operations with connection pooling"""

    def __init__(
        self,
        context: Optional[AssetExecutionContext] = None,
        host: str = DB_HOST,
        port: int = DB_PORT,
        dbname: str = DB_NAME,
        user: str = DB_USER,
        password: str = DB_PASSWORD,
        max_retries: int = 3
    ) -> None:
        """Initialize database client with connection parameters"""
        self.context = context
        self.url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        self.max_retries = max_retries
        self._init_engine()

    def _init_engine(self) -> None:
        """Initialize SQLAlchemy engine with connection pooling"""
        try:
            self.engine = create_engine(
                self.url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30
            )
        except Exception as e:
            self._log_error(f"Failed to initialize database engine: {e}")
            raise DBError(f"Database initialization failed: {e}")

    def _log_info(self, message: str) -> None:
        """Log info message using context if available"""
        if self.context:
            self.context.log.info(message)

    def _log_error(self, message: str) -> None:
        """Log error message using context if available"""
        if self.context:
            self.context.log.error(message)

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        try:
            with self.engine.begin() as conn:
                yield conn
        except SQLAlchemyError as e:
            self._log_error(f"Database connection error: {e}")
            raise DBError(f"Database connection failed: {e}")

    def execute_query(self, query: str) -> None:
        """Execute SQL query with retry logic"""
        try:
            with self.get_connection() as conn:
                # Create base table if not exists
                conn.execute(query)
        except Exception as e:
            self._log_error(f"Failed to execute query: {e}")

    def create_partition_hourly(
        self, 
        timestamp: datetime,
        table_name: str = 'stock_prices'
    ) -> None:
        """Create hourly partition for specified table"""
        try:
            
            partition_date = timestamp.strftime('%Y_%m_%d_%H')
            next_hour = timestamp.replace(minute=59, second=59, microsecond=999999)
            
            sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name}_{partition_date}
            PARTITION OF {table_name}
            FOR VALUES FROM ('{timestamp}') TO ('{next_hour}');
            """)
            self.execute_query(sql)
            self._log_info(f"Created partition {table_name}_{partition_date}")
                
        except Exception as e:
            self._log_error(f"Failed to create partition: {e}")
            raise DBError(f"Partition creation failed: {e}")

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_strategy: str = PartitionStrategy.HOURLY.value
    ) -> None:
        """Insert DataFrame into partitioned/normal table with retry logic"""
        if df.empty:
            self._log_info("Empty DataFrame received, skipping insert")
            return
        try:
            self.execute_query(get_table_query(table_name))
        except Exception as e:
            self._log_error(f"Failed to create table: {e}")
            raise DBError(f"Table creation failed: {e}")
        retries = 0
        while retries < self.max_retries:
            try:
                if partition_strategy == PartitionStrategy.HOURLY.value:
                    # Get unique hours from timestamp column
                    hours = pd.to_datetime(df['timestamp']).dt.floor('h').unique()
                    # Create partitions for each hour
                    for hour in hours:
                        self.create_partition_hourly(hour, table_name)
                    
                # Insert data using pandas to_sql
                df.to_sql(
                    table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                
                self._log_info(f"Successfully inserted {len(df)} rows into {table_name}")
                break
                    
            except SQLAlchemyError as e:
                retries += 1
                self._log_error(f"Insert attempt {retries} failed: {e}")
                if retries == self.max_retries:
                    raise DBError(f"Failed to insert data after {self.max_retries} attempts: {e}")

    def close(self) -> None:
        """Close database connections"""
        try:
            self.engine.dispose()
            self._log_info("Database connections closed")
        except Exception as e:
            self._log_error(f"Error closing database connections: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()