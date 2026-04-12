from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx["store"]
    amount = tx["amount"]

    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- PODSUMOWANIE ---")
        print("Sklep | Liczba | Suma | Średnia")

        for store in store_counts:
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count

            print(f"{store} | {count} | {round(total,2)} | {round(avg,2)}")