import boto3
import os
from botocore.exceptions import ClientError
from multiprocessing import Pool

# S3 Clients for different regions
s3_client_c = boto3.client('s3', region_name='us-east-2')
s3_client_ab = boto3.client('s3', region_name='us-east-1')

# Buckets
BUCKET_C = "bucket-c-east-2"
BUCKET_B = "bucket-b-east-1"
BUCKET_A = "bucket-a-east-1"

# EFS Directory
EFS_DIRECTORY = "/mnt/efs"
RECON_FILE = os.path.join(EFS_DIRECTORY, "reconciliation.csv")


def download_file_from_bucket_c(file_key):
    """Download a file from Bucket C (east-2)."""
    local_file_path = os.path.join(EFS_DIRECTORY, file_key.replace("/", "_"))
    try:
        s3_client_c.download_file(BUCKET_C, file_key, local_file_path)
        return local_file_path
    except ClientError as e:
        print(f"Failed to download {file_key} from Bucket C: {e}")
        return None


def download_object_from_bucket_b(object_key):
    """Download an object from Bucket B (east-1) to EFS."""
    local_file_path = os.path.join(EFS_DIRECTORY, object_key.replace("/", "_"))
    try:
        s3_client_ab.download_file(BUCKET_B, object_key, local_file_path)
        return local_file_path
    except ClientError as e:
        print(f"Failed to download {object_key} from Bucket B: {e}")
        return None


def process_file_from_bucket_c(file_key):
    """Process a file from Bucket C, download objects from Bucket B, and upload the original file to Bucket A."""
    # Step 1: Download the file from Bucket C
    local_file_path = download_file_from_bucket_c(file_key)
    if not local_file_path:
        return None

    # Step 2: Read object keys from the file
    with open(local_file_path, "r") as f:
        object_keys = f.read().splitlines()

    total_lines = len(object_keys)
    successful_downloads = 0

    # Step 3: Download objects from Bucket B sequentially (no multiprocessing here)
    for object_key in object_keys:
        if download_object_from_bucket_b(object_key):
            successful_downloads += 1

    # Step 4: Upload the original file from Bucket C to Bucket A
    upload_file_to_bucket_a(local_file_path, file_key)

    # Step 5: Write reconciliation information
    write_reconciliation(file_key, total_lines, successful_downloads)

    return file_key


def upload_file_to_bucket_a(local_file_path, s3_key):
    """Upload a file to Bucket A (east-1)."""
    try:
        s3_client_ab.upload_file(local_file_path, BUCKET_A, s3_key)
        print(f"Uploaded to A: {s3_key}")
    except ClientError as e:
        print(f"Failed to upload {s3_key} to Bucket A: {e}")


def write_reconciliation(file_key, total_lines, successful_downloads):
    """Write reconciliation information to the recon file."""
    with open(RECON_FILE, "a") as recon:
        recon.write(f"{file_key},{total_lines},{successful_downloads}\n")

"""
def main():
    """Main function to process files from Bucket C."""
    # Create EFS directory if not exists
    os.makedirs(EFS_DIRECTORY, exist_ok=True)

    # Setup reconciliation file
    if not os.path.exists(RECON_FILE):
        with open(RECON_FILE, "w") as recon:
            recon.write("FileName,TotalLines,SuccessfulDownloads\n")

    # Paginate through Bucket C to get all files
    paginator = s3_client_c.get_paginator('list_objects_v2')
    file_keys = []

    for page in paginator.paginate(Bucket=BUCKET_C):
        file_keys.extend([obj['Key'] for obj in page.get('Contents', [])])

    # Process all files in parallel using a single multiprocessing pool
    with Pool(processes=os.cpu_count()) as pool:
        pool.map(process_file_from_bucket_c, file_keys)


"""
def main():
    """Main function to process files from Bucket C."""
    # Create EFS directory if not exists
    os.makedirs(EFS_DIRECTORY, exist_ok=True)

    # Setup reconciliation file
    if not os.path.exists(RECON_FILE):
        with open(RECON_FILE, "w") as recon:
            recon.write("FileName,TotalLines,SuccessfulDownloads\n")

    # Paginate through Bucket C to get all files
    paginator = s3_client_c.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=BUCKET_C):
        # Collect file keys from the current page
        file_keys = [obj['Key'] for obj in page.get('Contents', [])]

        # Process all files in parallel using a multiprocessing pool
        with Pool(processes=os.cpu_count()) as pool:
            pool.map(process_file_from_bucket_c, file_keys)

        # Clear file_keys list after processing the page (no longer needed)
        file_keys.clear()

if __name__ == "__main__":
    main()