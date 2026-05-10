from kafka import KafkaConsumer
import json
from datetime import datetime
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(list)

print("Nasłuchuję anomalii prędkości...")

for message in consumer:
    transaction = message.value

    user_id = transaction['user_id']
    transaction_time = datetime.fromisoformat(transaction['timestamp'])

    user_transactions[user_id].append(transaction_time)

    user_transactions[user_id] = [
        t for t in user_transactions[user_id]
        if (transaction_time - t).total_seconds() <= 60
    ]

    if len(user_transactions[user_id]) > 3:
        print(
            f"ALERT: {user_id} wykonał "
            f"{len(user_transactions[user_id])} transakcje w ciągu 60 sekund"
        )
