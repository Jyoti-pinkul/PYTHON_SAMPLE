import boto3
import multiprocessing
from botocore.config import Config
from botocore.exceptions import ClientError

# Configure S3 client with retries
custom_config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'adaptive'
    }
)
s3_client = boto3.client('s3', config=custom_config)

SOURCE_BUCKET = "your-source-bucket"
DEST_BUCKET = "your-destination-bucket"

def copy_object_worker(object_key):
    """Worker function to copy a single object."""
    try:
        copy_source = {'Bucket': SOURCE_BUCKET, 'Key': object_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=DEST_BUCKET, Key=object_key)
        print(f"Copied: {object_key}")
    except ClientError as e:
        print(f"Failed to copy {object_key}: {e}")

def process_page(page):
    """Process a single page of objects."""
    object_keys = [obj['Key'] for obj in page.get('Contents', [])]
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.map(copy_object_worker, object_keys)

def copy_objects_streaming():
    """Stream and copy objects page by page."""
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=SOURCE_BUCKET):
        if 'Contents' in page:
            process_page(page)

if __name__ == "__main__":
    copy_objects_streaming()