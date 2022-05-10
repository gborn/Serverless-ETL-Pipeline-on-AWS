import os
import boto3
from download import download_file
from upload import upload_s3
from utils import get_prev_filename, get_next_filename

 
def lambda_handler(event, context):
    # Run the code in current environment to get appropriate permissions
    environ = os.environ.get('ENVIRON')
    if environ == 'DEV':
        print(f'Running in {environ} environment')
        os.environ.setdefault('AWS_PROFILE', 'glad')

    # Get the environment variables
    bucket = os.environ.get('BUCKET_NAME')
    file_prefix = os.environ.get('FILE_PREFIX')
    bookmark_file = os.environ.get('BOOKMARK_FILE')
    baseline_file = os.environ.get('BASELINE_FILE')

    # download and incrementally upload file to S3
    while True:
        prev_filename = get_prev_filename(bucket, file_prefix, bookmark_file, baseline_file)
        filename = get_next_filename(prev_filename)
        download_res = download_file(filename)
        if download_res.status_code == 404:
            print(f'Invalid file name or downloads caught up till {prev_filename}')
            break

        upload_res = upload_s3(
            download_res.content,
            bucket,
            f'{file_prefix}/{filename}'
        )
        print(f'File {filename} successfully processed')

        # update bookmark
        upload_bookmark(bucket, file_prefix, bookmark_file, filename)

    return upload_res