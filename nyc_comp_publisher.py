import time
import json
import logging
from google.cloud import pubsub_v1
import pandas as pd

logging.basicConfig(level=logging.INFO)

publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/your-project-id/topics/your-topic-name'
csv_file_path = 'your-path/nyc_civilians_complaints.csv'

def process_and_publish_csv():
    chunk_size = 5000
    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, delimiter=";"):
        for row in chunk.to_dict(orient="records"):
            message = json.dumps(row)
            publisher.publish(topic_name, message.encode('utf-8'))
        logging.info("Batch published.")
        time.sleep(10)

if __name__ == "__main__":
    process_and_publish_csv()