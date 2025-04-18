import time 
from google.cloud import pubsub_v1
from json import load, dumps
from concurrent import futures

from tqdm import tqdm


project_id = "dataeng-2025-transport"
topic_id = "sample-topic"
file_path = "bcsample.json"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open(file_path, 'r') as file:
    records = load(file)

start_time = time.time()

with tqdm(total=len(records), desc="Publishing messages", unit="msg") as pbar:
    publish_futures = []

    # Publish messages asynchronously
    for record in records:
        message_data = dumps(record).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        publish_futures.append(future)
        future.add_done_callback(lambda fut: pbar.update(1))

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

execution_time = time.time() - start_time
print(f"\nPublished {len(records)} messages to {topic_path}.")
print(f"Publisher execution time: {execution_time:.2f} seconds.")
