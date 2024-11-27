from dagster import asset, AssetExecutionContext,job, schedule
import json
import pandas as pd
from datetime import datetime
import time
from etl_pipeline.utils.kafka_client import KafkaClient
from etl_pipeline.utils.sql_client import DBClient
from etl_pipeline.utils.constants import *

@asset(required_resource_keys={"db_client"})
def stock_prices_asset(context: AssetExecutionContext) -> pd.DataFrame:
    client = KafkaClient(
        group_id='dagster_stock_prices',
        topic=STOCK_PRICE_TOPIC, 
        context=context
    )
    
    try:
        messages = client.consume_batch()
        if not messages:
            context.log.warning("No messages received from Kafka, returning an empty DataFrame")
            ##datadog metric for telemetry, return empty dataframe to avoid error
            return pd.DataFrame()
            
        df = pd.DataFrame(messages)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['processed_at'] = datetime.now()
        if 'price' in df.columns and len(df) > 0:
            df = df.sort_values('timestamp')
            df['price_moving_avg'] = df.groupby('symbol')['price'].rolling(
                window=5,
                min_periods=1
            ).mean().reset_index(0, drop=True)
            
        try:
            if not df.empty:
                context.resources.db_client.insert_dataframe(df, STOCK_PRICE_TABLE)
                context.log.info(f"Successfully loaded {len(df)} records")
                client.commit()
            else:
                context.log.warning("No data to load")
        except Exception as e:
            context.log.error(f"Failed to load data: {e}")
            return df
        
        context.log.info(f"Processed {len(df)} records")
        return df
        
    except Exception as e:
        context.log.error(f"Error processing batch: {e}")
        ## Return empty DataFrame on error and send the log to datadog
        return pd.DataFrame()
    
    finally:
        client.close()


@asset(required_resource_keys={"db_client"})
def stock_volume(context: AssetExecutionContext) -> pd.DataFrame:
    client = KafkaClient(
        group_id='dagster_stocks_volume',
        topic=STOCK_VOLUME_TOPIC,
        context=context
    )
    
    try:
        messages = client.consume_batch()
        
        if not messages:
            context.log.warning("No messages received from Kafka")
            return pd.DataFrame()
            
        df = pd.DataFrame(messages)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['processed_at'] = datetime.now()
        if 'volume' in df.columns and len(df) > 0:
            df = df.sort_values('timestamp')
            df['trading_volume_moving_avg'] = df.groupby('symbol')['volume'].rolling(
                window=5,
                min_periods=1
            ).mean().reset_index(0, drop=True)
            
        try:
            if not df.empty:
                context.resources.db_client.insert_dataframe(df, STOCK_VOLUME_TABLE,partition_strategy=None)
                context.log.info(f"Successfully loaded {len(df)} records")
                client.commit()
            else:
                context.log.warning("No data to load")
        except Exception as e:
            context.log.error(f"Failed to load data: {e}")
            # Return DataFrame even if DB insert fails
            return df
        
        context.log.info(f"Processed {len(df)} records")
        return df
        
    except Exception as e:
        context.log.error(f"Error processing batch: {e}")
        return pd.DataFrame()
    
    finally:
        client.close()

@asset
def joined_stock_data(context: AssetExecutionContext, stock_prices_asset: pd.DataFrame, stock_volume: pd.DataFrame) -> pd.DataFrame:
    """Join stock prices and volumes data"""
    try:
        if stock_prices_asset.empty or stock_volume.empty:
            context.log.warning("One or both input DataFrames are empty")
            return pd.DataFrame()

        # Join on symbol and timestamp
        joined_df = pd.merge(
            stock_prices_asset,
            stock_volume,
            on=['symbol','timestamp'],
            how='inner',
            suffixes=('_price', '_volume')
        )
        context.log.info(f"Joined {len(joined_df)} records")
        ## Add derived metrics
        joined_df['price_volume_ratio'] = joined_df['price'] / joined_df['volume']
        joined_df['processed_at'] = datetime.now()
        joined_df.drop(['processed_at_price', 'processed_at_volume'], axis=1, inplace=True)
        try:
            with DBClient(context=context) as db:
                if not joined_df.empty:
                    db.insert_dataframe(joined_df, STOCK_ANALYTICS_DATA, partition_strategy=None)
                    context.log.info(f"Successfully loaded {len(joined_df)} joined records")
                else:
                    context.log.warning("No joined data to load")
        except Exception as e:
            context.log.error(f"Failed to load joined data: {e}")
            return joined_df

        return joined_df

    except Exception as e:
        context.log.error(f"Error joining data: {e}")
        return pd.DataFrame()

@job
def stock_data_job():
    # Define asset dependencies
    joined_stock_data(stock_prices_asset(), stock_volume())

# Schedule job to run every 5 minutes
@schedule(
    job=stock_data_job,
    cron_schedule="*/20 * * * *",  # Every 20 minutes
    execution_timezone="UTC"
)
def stock_data_schedule():
    return {}