import boto3
from botocore.config import Config
import multiprocessing

# Custom retry configuration
custom_config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'adaptive'
    }
)

# Initialize S3 client with custom config
s3_client = boto3.client('s3', config=custom_config)

SOURCE_BUCKET = "your-source-bucket"
DEST_BUCKET = "your-destination-bucket"

def copy_object(object_key):
    try:
        copy_source = {'Bucket': SOURCE_BUCKET, 'Key': object_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=DEST_BUCKET, Key=object_key)
        print(f"Copied: {object_key}")
    except Exception as e:
        print(f"Failed to copy {object_key}: {e}")

# Multiprocessing for parallel copy
def copy_objects_in_parallel():
    paginator = s3_client.get_paginator('list_objects_v2')
    object_keys = []

    for page in paginator.paginate(Bucket=SOURCE_BUCKET):
        object_keys.extend(obj['Key'] for obj in page.get('Contents', []))

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.map(copy_object, object_keys)

if __name__ == "__main__":
    copy_objects_in_parallel()