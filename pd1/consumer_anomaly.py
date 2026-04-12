from kafka import KafkaConsumer
from collections import defaultdict
import json
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(list)

print("Wykrywanie anomalii...")

for message in consumer:
    tx = message.value
    user = tx["user_id"]
    timestamp = datetime.fromisoformat(tx["timestamp"])

    user_transactions[user].append(timestamp)

    one_minute_ago = timestamp - timedelta(seconds=60)
    user_transactions[user] = [
        t for t in user_transactions[user] if t > one_minute_ago
    ]

    if len(user_transactions[user]) > 3:
        print(f"ANOMALY ALERT: {user} >3 transakcji w 60s ({len(user_transactions[user])})")
