from etl_pipeline.utils.constants import *
import logging

logging.basicConfig(level=logging.INFO)
def get_table_query(table_name:str)-> str:
    if table_name == STOCK_PRICE_TABLE:
        return stock_prices_create_query
    elif table_name == STOCK_VOLUME_TABLE:
        return stock_volume_create_query
    elif table_name == STOCK_ANALYTICS_DATA:  
        return create_joined_table_query
    else:
        raise ValueError(f"Table {table_name} not found")
