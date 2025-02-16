import pandas as pd
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json

def iterate_traffic(producer, csv_path, speed_factor):
    df = pd.read_csv(csv_path)

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    start_time = df['timestamp'].min()
    prev_time = start_time

    for _, row in df.iterrows():
        current_time = row['timestamp']
        delay = (current_time - prev_time).total_seconds()/speed_factor
        print(f'Highway {row["highway_id"]}: {row["plate"]}: {row["speed"]}')
        row_dict = row.to_dict()
        row_dict['timestamp'] = str(row_dict['timestamp'])
        producer.send('traffic-events', value=row_dict)
        time.sleep(delay)
        prev_time = current_time
        producer.flush()


def create_producer(bootstrap_servers, retries=10, delay=5):
    # trying to connect to the Kafka brokers until they are finally available
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka broker...")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/{retries}: Kafka broker not available. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Could not connect to Kafka broker after multiple retries.")


if __name__ == "__main__":
    producer = create_producer(
        bootstrap_servers='broker-1:9092'
                           #'broker-2:9093','broker-3:9094'
                           )
    iterate_traffic(producer=producer, 
                    csv_path='./csv_files/multi_road_traffic.csv', 
                    speed_factor=2)
