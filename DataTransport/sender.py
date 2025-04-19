import time
from google.cloud import pubsub_v1
from json import load, dumps
from concurrent import futures

project_id = "dataeng-2025-transport"
topic_id = "sample-topic"
file_path = "bc_data.json"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open(file_path, 'r') as file:
    records = load(file)

start_time = time.time()
future_list = []
count = 0

def future_callback(future):
    global count
    

def publish_message(record):
    message_data = dumps(record).encode("utf-8")
    return publisher.publish(topic_path, message_data)


for record in records:
    future = publish_message(record)
    future.add_done_callback(future_callback)
    future_list.append(future)
    count += 1
    
    
    if count % 10000 == 0:
        print(f"Submitted {count} messages for publishing")

print(f"All {len(records)} messages submitted. Waiting for completion...")


completed_count = 0
for future in futures.as_completed(future_list):
    completed_count += 1
    if completed_count % 10000 == 0:
        print(f"Completed {completed_count} messages")

execution_time = time.time() - start_time
print(f"\nPublished {len(records)} messages to {topic_path}.")
print(f"Publisher execution time: {execution_time:.2f} seconds.")