import json
from kafka import KafkaConsumer
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


BOOTSTRAP_SERVERS="host.docker.internal:29092"
TOPIC_NAME="clean_events"
GROUP_ID = "snowflake-loader"

SNOWFLAKE_CONFIG = {
    "user" : os.getenv("SNOWFLAKE_USER"),
    "password" : os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse" : os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database" : os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

BATCH_SIZE = 10

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

print("Connected to Snowflake")
print("Starting Kafka -> Snowflake Loader...")

buffer = []

def flush_to_snowflake(records):
    df = pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn=sf_conn,
        df=df,
        table_name="KAFKA_EVENTS_SILVER"
    )

    if not success:
        raise Exception("Snowflake insert failed")
    
    print(f"Inserted {nrows} rows into Snowflake")

for message in consumer:
    event = message.value
    buffer.append({
        "event_id": event["event_id"],
        "customer_id" : event["customer_id"],
        "event_type" : event["event_type"],
        "amount" : event["amount"],
        "currency" : event["currency"],
        "event_timestamp" : event["event_timestamp"]
    })

    if len(buffer) >= BATCH_SIZE:
        try:
            flush_to_snowflake(buffer)
            consumer.commit()
            buffer.clear()
        except Exception as e:
            print(f"ERROR inserting batch: {e}")