import functions_framework
from google.cloud import pubsub_v1
import base64
import json
import os

# Getting project ID and Pub/Sub topic from environment variables
PROJECT_ID = "true-record-462618-h8"
TOPIC_ID = "my-topic-1"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@functions_framework.cloud_event
def process_storage_event(cloud_event):
    """
    This function is triggered by a Cloud Storage event.
    """
    # Getting file data from event
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]
    size = data["size"]

    # Getting file format
    file_format = name.split('.')[-1] if '.' in name else 'unknown'

    print(f"File {name} uploaded to bucket {bucket}.")
    print(f"Size: {size} bytes, Format: {file_format}")

    # Data for Pub/Sub Msg
    message_data = {
        "fileName": name,
        "fileSize": size,
        "fileFormat": file_format
    }

    # Publishing the message
    future = publisher.publish(topic_path, data=json.dumps(message_data).encode("utf-8"))
    print(f"Published message to {topic_path}: {future.result()}")