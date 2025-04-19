from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import time
import threading

# Configuration
project_id = "your-project-id"  # Replace with your actual project ID
subscription_id = "your-subscription-id"  # Replace with your subscription ID
timeout = 300.0  # Increased timeout for large message volumes
message_threshold = 10000  # Print progress every N messages

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

class MessageCounter:
    def __init__(self):
        self.count = 0
        self.lock = threading.Lock()
        self.last_print_count = 0
        self.start_time = time.time()
        self.active = True

    def increment(self):
        with self.lock:
            self.count += 1
            if self.count % message_threshold == 0:
                current_time = time.time()
                elapsed = current_time - self.start_time
                rate = message_threshold / (current_time - self.last_print_time) if hasattr(self, 'last_print_time') else 0
                print(f"Received {self.count} messages ({rate:.1f} msg/sec)")
                self.last_print_time = current_time

counter = MessageCounter()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    counter.increment()
    message.ack()

def listen_for_messages():
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
        flow_control=pubsub_v1.types.FlowControl(
            max_messages=1000,  # Adjust based on your memory constraints
            max_bytes=100 * 1024 * 1024,  # 100MB
        )
    )
    
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        counter.active = False

# Start the subscriber in a separate thread
listener_thread = threading.Thread(target=listen_for_messages)
listener_thread.start()

print(f"Listening for messages on {subscription_path}...")

try:
    # Monitor progress while messages are being received
    while counter.active:
        time.sleep(1)
        # You could add additional monitoring here if needed
except KeyboardInterrupt:
    print("\nShutting down subscriber...")
    counter.active = False

listener_thread.join()

# Final statistics
execution_time = time.time() - counter.start_time
print(f"\nTotal messages received: {counter.count}")
print(f"Average rate: {counter.count/execution_time:.1f} messages/sec")
print(f"Total execution time: {execution_time:.2f} seconds")