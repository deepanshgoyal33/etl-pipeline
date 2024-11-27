from dagster import resource
from etl_pipeline.utils.kafka_client import KafkaClient
from etl_pipeline.utils.sql_client import DBClient

@resource
def db_client():
    return DBClient()