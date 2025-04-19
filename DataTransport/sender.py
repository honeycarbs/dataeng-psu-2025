import time
from google.cloud import pubsub_v1
from json import load, dumps
from concurrent import futures

project_id = "dataeng-2025-transport"
topic_id = "sample-topic"
file_path = "bc_data.json"
MAX_WORKERS = 10

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open(file_path, 'r') as file:
    records = load(file)

start_time = time.time()
publish_count = 0
completed_count = 0

def future_callback(future):
    global completed_count
    completed_count += 1
    if completed_count % 10000 == 0:
        print(f"Completed {completed_count} messages")

def publish_message(record):
    message_data = dumps(record).encode("utf-8")
    return publisher.publish(topic_path, message_data)

with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_list = []
    
    for record in records:
        future = executor.submit(publish_message, record)
        future.add_done_callback(future_callback)
        future_list.append(future)
        publish_count += 1
        
        if publish_count % 10000 == 0:
            print(f"Submitted {publish_count} messages for publishing")

    print(f"All {len(records)} messages submitted. Waiting for completion...")
    
    for future in futures.as_completed(future_list):
        pass 

execution_time = time.time() - start_time
print(f"\nPublished {len(records)} messages to {topic_path}.")
print(f"Publisher execution time: {execution_time:.2f} seconds.")