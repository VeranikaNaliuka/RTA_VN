from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

stores = ["Warszawa", "Kraków", "Gdańsk", "Wrocław"]
categories = ["elektronika", "odzież", "żywność", "książki"]

tx_number = 1

def generate_transaction():
    global tx_number

    transaction = {
        "tx_id": f"TX{tx_number:04d}",
        "user_id": f"u{random.randint(1, 20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(stores),
        "category": random.choice(categories),
        "timestamp": datetime.now().isoformat()
    }

    tx_number += 1
    return transaction

while True:
    tx = generate_transaction()
    producer.send('transactions', value=tx)
    producer.flush()

    print(
        f"SENT: {tx['tx_id']} | {tx['user_id']} | "
        f"{tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}"
    )

    time.sleep(1)
