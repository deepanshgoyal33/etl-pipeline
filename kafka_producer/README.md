# Data Generation for Stock Prices and Volumes

## Overview
This script generates synthetic stock price and volume data for a large number of symbols. The data is published to Kafka topics (`stock_prices` and `stock_volumes`) in two phases:
1. **Initial Batch**: Produces a set of historical data spanning the last 10 days.
2. **Real-Time Updates**: Continuously generates live data for stock prices and volumes.

## Data Description
### Stock Price Data
Each stock price record contains the following fields:
- **`symbol`**: A unique stock ticker symbol, generated randomly using a combination of consonants and vowels (e.g., `BCA`, `TRND`).
- **`price`**: The current stock price, simulated with a trend and noise component.
- **`open_price`**: The opening price for the stock.
- **`high_price`**: The highest recorded price for the stock during the current simulation.
- **`low_price`**: The lowest recorded price for the stock during the current simulation.
- **`timestamp`**: The ISO 8601 formatted timestamp for when the data was generated.

### Stock Volume Data
Each stock volume record contains the following fields:
- **`symbol`**: The stock ticker symbol corresponding to the volume data.
- **`volume`**: The simulated trade volume, which correlates with the price movement of the stock.
- **`timestamp`**: The ISO 8601 formatted timestamp for when the data was generated.

## Data Generation
### Stock Symbols
Stock symbols are generated randomly using a combination of consonants and vowels to create unique 3- or 4-letter symbols. Up to 5000 unique symbols are generated.

### Stock Price Simulation
A `StockPriceSimulator` class is used to manage price trends and simulate realistic stock price movements:
- **Trend**: Simulates upward or downward movement over time with occasional reversals.
- **Noise**: Adds small random fluctuations to the price.
- **High/Low Tracking**: Keeps track of the highest and lowest prices observed for each symbol.

### Stock Volume Simulation
Volume is correlated with price changes:
- A base volume is generated randomly.
- A modifier is applied based on the magnitude of price changes.

## Kafka Integration
- Data is sent to Kafka topics using the `KafkaProducer`.
- Each record is serialized into JSON format before being published.

### Topics
1. **`stock_prices`**: Stores stock price data.
2. **`stock_volumes`**: Stores stock volume data.

### Initial Batch Production
- Simulates 10 days of historical data.
- Divides the time range into intervals to spread out the generated records evenly.

### Real-Time Data Production
- Generates stock price and volume data every second.
- Continuously sends data to the Kafka topics in real-time.

## Script Execution
Run the script using:
```python
python stock_data_generator.py
```
