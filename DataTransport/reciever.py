from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import time

project_id = "dataeng-2025-transport"
subscription_id = "sample-sub"

timeout = .0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

total_messages = 0
message_threshold = 10000

start_time = time.time()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global total_messages
    total_messages += 1
    message.ack()

    # Print total count every 10,000 messages
    if total_messages % message_threshold == 0:
        print(f"Received {total_messages} messages.")

# Start streaming pull
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

# Calculate and print execution time
execution_time = time.time() - start_time
print(f"Total messages received: {total_messages}.")
print(f"Subscriber execution time: {execution_time:.2f} seconds.")

