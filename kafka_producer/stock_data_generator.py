from kafka import KafkaProducer
from faker import Faker
import json
import time
from datetime import datetime, timedelta
import random
from collections import defaultdict

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:19092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_stock_symbols(num_stocks=5000):
    symbols = []
    consonants = 'BCDFGHJKLMNPQRSTVWXYZ'
    vowels = 'AEIOU'
    
    while len(symbols) < num_stocks:
        length = random.choice([3, 4])
        if length == 3:
            symbol = (
                random.choice(consonants) +
                random.choice(vowels) +
                random.choice(consonants)
            )
        else:
            symbol = (
                random.choice(consonants) +
                random.choice(vowels) +
                random.choice(consonants) +
                random.choice(consonants)
            )
        if symbol not in symbols:
            symbols.append(symbol)
    
    return symbols

STOCK_SYMBOLS = generate_stock_symbols()


class StockPriceSimulator:
    def __init__(self):
        self.price_memory = defaultdict(dict)
        self.trend_direction = defaultdict(lambda: 1)  # 1 for up, -1 for down
        self.trend_duration = defaultdict(int)
        
    def initialize_stock(self, symbol, base_price=None):
        if base_price is None:
            base_price = random.uniform(10.0, 1000.0)
        
        self.price_memory[symbol] = {
            'last_price': base_price,
            'high_price': base_price,
            'low_price': base_price
        }
        return base_price
        
    def get_next_price(self, symbol):
        if symbol not in self.price_memory:
            return self.initialize_stock(symbol)
            
        last_price = self.price_memory[symbol]['last_price']
        
        # Change trend direction occasionally
        self.trend_duration[symbol] += 1
        if self.trend_duration[symbol] > random.randint(5, 15):
            self.trend_direction[symbol] *= -1
            self.trend_duration[symbol] = 0
            
        # Calculate price movement
        trend = self.trend_direction[symbol] * random.uniform(0.001, 0.03)
        noise = random.uniform(-0.005, 0.005)
        
        # New price with trend and noise
        new_price = last_price * (1 + trend + noise)
        
        # Update price memory
        self.price_memory[symbol]['last_price'] = new_price
        self.price_memory[symbol]['high_price'] = max(
            self.price_memory[symbol]['high_price'],
            new_price
        )
        self.price_memory[symbol]['low_price'] = min(
            self.price_memory[symbol]['low_price'],
            new_price
        )
        
        return new_price

simulator = StockPriceSimulator()

def generate_stock_data(timestamp=None):
    symbol = random.choice(STOCK_SYMBOLS)
    
    if timestamp is None:
        timestamp = datetime.now()
        
    current_price = simulator.get_next_price(symbol)
    
    stock_price_data = {
        'symbol': symbol,
        'price': round(current_price, 2),
        'open_price': round(simulator.price_memory[symbol]['last_price'], 2),
        'high_price': round(simulator.price_memory[symbol]['high_price'], 2),
        'low_price': round(simulator.price_memory[symbol]['low_price'], 2),
        'timestamp': timestamp.isoformat()
    }
    
    # Volume data with correlation to price movement
    price_change = abs(stock_price_data['price'] - stock_price_data['open_price'])
    base_volume = random.randint(1000, 10000)
    volume_modifier = 1 + (price_change / stock_price_data['open_price']) * 10
    
    stock_volume_data = {
        'symbol': symbol,
        'volume': int(base_volume * volume_modifier),
        'timestamp': timestamp.isoformat()
    }
    
    return stock_price_data, stock_volume_data

def produce_initial_batch(num_records=10000):
    """Produce initial batch of records for last 10 days"""
    now = datetime.now()
    ten_days_ago = now - timedelta(days=10)
    
    # Calculate time interval for spreading records
    total_seconds = (now - ten_days_ago).total_seconds()
    interval = timedelta(seconds=total_seconds / num_records)
    
    # Generate and send messages in batch
    stock_price_messages,stock_volume_data_messages = [],[]
    for i in range(num_records):
        # Generate timestamp between 10 days ago and now
        timestamp = ten_days_ago + (interval * i)
        stock_price_data, stock_volume_data = generate_stock_data(timestamp)
        stock_price_messages.append(stock_price_data)
        stock_volume_data_messages.append(stock_volume_data)
    
    # Send messages
    for msg in stock_price_messages:
        producer.send('stock_prices', msg)
    for msg in stock_volume_data_messages:
        producer.send('stock_volume', msg)
        
    producer.flush()
    print(f"Produced initial batch of {num_records} records spanning last 10 days")
    print(f"Time range: {ten_days_ago} to {now}")

def produce_realtime_data():
    """Continue producing real-time data after initial batch"""
    while True:
        stock_price_data, stock_volume_data = generate_stock_data()
        producer.send('stock_prices', stock_price_data)
        producer.send('stock_volume', stock_volume_data)
        print(f"Sent: {stock_price_data} and {stock_volume_data}")
        time.sleep(1)

if __name__ == "__main__":
    try:
        # First produce initial batch
        produce_initial_batch(10000)
        
        # Then continue with real-time updates
        produce_realtime_data()
    except KeyboardInterrupt:
        print("Shutting down producer...")
        producer.close()