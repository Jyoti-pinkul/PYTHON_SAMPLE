import boto3
import multiprocessing
import time

# Configure SQS
sqs_client = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue'
max_processes = 5
processes = []

def process_message(message):
    try:
        print(f"Processing Message: {message['Body']} in PID {multiprocessing.current_process().pid}")
        time.sleep(2)  # Simulate processing time

        # Delete the message from the queue once processed
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
        print(f"Deleted message with ReceiptHandle: {message['ReceiptHandle']}")
    except Exception as e:
        print(f"Error processing message: {e}")

def poll_and_process():
    while True:
        # Clean up finished processes
        for p in processes[:]:
            if not p.is_alive():
                p.join()
                processes.remove(p)

        # Control the number of processes
        if len(processes) >= max_processes:
            time.sleep(1)
            continue

        # Poll message from SQS
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )

        messages = response.get('Messages', [])
        if not messages:
            print("No messages found. Waiting...")
            continue

        message = messages[0]
        print("Received message. Starting process.")

        # Start a new process for the message
        p = multiprocessing.Process(target=process_message, args=(message,))
        p.start()
        processes.append(p)

if __name__ == "__main__":
    poll_and_process()