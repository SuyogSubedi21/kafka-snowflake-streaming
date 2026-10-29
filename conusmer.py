import os
import time

import snowflake.connector
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

BATCH_SIZE = 50
BATCH_SECONDS = 10

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "snowflake-loader",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["api_orders"])

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    authenticator="SNOWFLAKE_JWT",
    private_key_file=os.path.expanduser(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="RAW",
)


def insert_batch(rows):
    placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(rows))
    sql = f"""
        insert into api_orders_raw (data, kafka_key, kafka_partition, kafka_offset, loaded_at)
        select parse_json(column1), column2, column3, column4, current_timestamp()
        from values {placeholders}
    """
    values = [v for row in rows for v in row]
    conn.cursor().execute(sql, values)


def main():
    batch = []
    last_flush = time.time()
    print("Consumer started. Press Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                else:
                    batch.append((
                        msg.value().decode(),
                        msg.key().decode() if msg.key() else None,
                        msg.partition(),
                        msg.offset(),
                    ))

            if batch and (len(batch) >= BATCH_SIZE or time.time() - last_flush >= BATCH_SECONDS):
                insert_batch(batch)
                consumer.commit(asynchronous=False)
                print(f"Inserted {len(batch)} rows and committed offsets")
                batch = []
                last_flush = time.time()
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()