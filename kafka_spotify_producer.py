import time
import json
import random
from kafka import KafkaProducer

artists = ["Taylor Swift", "The Weeknd", "Bad Bunny", "Ed Sheeran", "Dua Lipa"]
genres = ["Pop", "K-pop", "R&B", "Hip Hop", "Rock"]
countries = ["US", "BR", "MX", "DE", "FR", "UK", "JP"]

def generate_spotify_data():
    return {
        "timestamp": int(time.time()),
        "artist": random.choice(artists),
        "track": f"Track_{random.randint(1000,9999)}",
        "genre": random.choice(genres),
        "country": random.choice(countries),
        "streams": random.randint(10000, 100000),
        "duration_ms": random.randint(180000, 300000)
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    data = generate_spotify_data()
    producer.send('spotify_streaming', value=data)
    print(f"Sent: {data}")
    time.sleep(random.uniform(0.1, 0.5))
