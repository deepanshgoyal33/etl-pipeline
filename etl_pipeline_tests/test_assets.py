import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch
from dagster import build_asset_context
from etl_pipeline.assets.assets import stock_prices_asset, stock_volume
from etl_pipeline.utils.constants import STOCK_PRICE_TABLE, STOCK_VOLUME_TABLE

@pytest.fixture
def mock_kafka_messages():
    return [
        {
            'symbol': 'AAPL',
            'price': 150.0,
            'open_price': 149.0,
            'high_price': 151.0,
            'low_price': 148.0,
            'timestamp': '2024-03-26T10:00:00'
        },
        {
            'symbol': 'AAPL',
            'price': 151.0,
            'open_price': 150.0,
            'high_price': 152.0,
            'low_price': 149.0,
            'timestamp': '2024-03-26T10:01:00'
        }
    ]

@pytest.fixture
def mock_volume_messages():
    return [
        {
            'symbol': 'AAPL',
            'volume': 1000,
            'timestamp': '2024-03-26T10:00:00'
        },
        {
            'symbol': 'AAPL',
            'volume': 1200,
            'timestamp': '2024-03-26T10:01:00'
        }
    ]

@pytest.fixture
def mock_context():
    mock_db_client = Mock()
    return build_asset_context(resources={"db_client": mock_db_client})

class TestStockPricesAsset:
    @patch('etl_pipeline.assets.assets.KafkaClient')
    @patch('etl_pipeline.assets.assets.DBClient')
    def test_stock_prices_success(self, mock_db, mock_kafka, mock_context, mock_kafka_messages):
        mock_kafka_instance = Mock()
        mock_kafka_instance.consume_batch.return_value = mock_kafka_messages
        mock_kafka.return_value = mock_kafka_instance

        mock_db_instance = Mock()
        mock_db.return_value.__enter__.return_value = mock_db_instance
        result = stock_prices_asset(mock_context)

        # Verify results
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'price_moving_avg' in result.columns
        assert 'processed_at' in result.columns
        
        mock_kafka_instance.consume_batch.assert_called_once()
        mock_kafka_instance.commit.assert_called_once()
        mock_db_instance.insert_dataframe_partitioned.assert_not_called()

    @patch('etl_pipeline.assets.assets.KafkaClient')
    def test_stock_prices_no_messages(self, mock_kafka, mock_context):
        # Setup mock
        mock_kafka_instance = Mock()
        mock_kafka_instance.consume_batch.return_value = []
        mock_kafka.return_value = mock_kafka_instance

        result = stock_prices_asset(mock_context)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch('etl_pipeline.assets.assets.KafkaClient')
    @patch('etl_pipeline.assets.assets.DBClient')
    def test_stock_prices_db_error(self, mock_db, mock_kafka, mock_context, mock_kafka_messages):
        mock_kafka_instance = Mock()
        mock_kafka_instance.consume_batch.return_value = mock_kafka_messages
        mock_kafka.return_value = mock_kafka_instance

        mock_db_instance = Mock()
        mock_db_instance.insert_dataframe_partitioned.side_effect = Exception("DB Error")
        mock_db.return_value.__enter__.return_value = mock_db_instance
        result = stock_prices_asset(mock_context)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert not result.empty

class TestStockVolumeAsset:
    @patch('etl_pipeline.assets.assets.KafkaClient')
    @patch('etl_pipeline.assets.assets.DBClient')
    def test_stock_volume_success(self, mock_db, mock_kafka, mock_context, mock_volume_messages):
        mock_kafka_instance = Mock()
        mock_kafka_instance.consume_batch.return_value = mock_volume_messages
        mock_kafka.return_value = mock_kafka_instance

        mock_db_instance = Mock()
        mock_db.return_value.__enter__.return_value = mock_db_instance

        result = stock_volume(mock_context)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'volume' in result.columns
        assert 'processed_at' in result.columns

        mock_kafka_instance.consume_batch.assert_called_once()
        mock_kafka_instance.commit.assert_called_once()

    @patch('etl_pipeline.assets.assets.KafkaClient')
    def test_stock_volume_no_messages(self, mock_kafka, mock_context):
        mock_kafka_instance = Mock()
        mock_kafka_instance.consume_batch.return_value = []
        mock_kafka.return_value = mock_kafka_instance

        result = stock_volume(mock_context)

        assert isinstance(result, pd.DataFrame)
        assert result.empty

def test_data_transformations():
    # Test price moving average calculation
    data = pd.DataFrame({
        'symbol': ['AAPL', 'AAPL', 'AAPL'],
        'price': [100, 110, 120],
        'timestamp': pd.date_range(start='2024-03-26', periods=3, freq='h')
    })
    
    data['price_moving_avg'] = data.groupby('symbol')['price'].rolling(
        window=5,
        min_periods=1
    ).mean().reset_index(0, drop=True)
    
    assert len(data) == 3
    assert data['price_moving_avg'].iloc[0] == 100
    assert data['price_moving_avg'].iloc[-1] == 110
