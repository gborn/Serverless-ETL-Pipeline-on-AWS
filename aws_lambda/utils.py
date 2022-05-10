from datetime import datetime as dt
from datetime import timedelta as td
import requests, boto3, os
from botocore.errorfactory import ClientError


def get_client():
    """
    Returns AWS S3 client
    """
    return boto3.client('s3')


def get_prev_filename(bucket, file_prefix, bookmark_file, baseline_file):
    """
    Get previous filename by reading bookmark file from S3
    If bookmark file doesn't exist yet, use the baseline file
    """
    s3_client = get_client()
    try:
        bookmark_file = s3_client.get_object(
            Bucket=bucket,
            Key=f'{file_prefix}/{bookmark_file}'
        )

        prev_file = bookmark_file['Body'].read().decode('utf-8')
    
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            prev_file = baseline_file
        else:
            raise e

    return prev_file


def get_next_filename(prev_file):
    """
    Get next filename by adding one hour to prev_file date
    """
    dt_part = prev_file.split('.')[0]
    next_dt = dt.strptime(dt_part, '%Y-%M-%d-%H') + td(hours=1)
    next_filename = f"{dt.strftime(next_dt, '%Y-%M-%d-%-H')}.json.gz"
    return next_filename


def upload_bookmark(bucket, file_prefix, bookmark_file, bookmark_contents):
    """
    Create bookmark file and place it on given S3 bucket
    """
    s3_client = get_client()
    s3_client.put_object(
        Bucket=bucket,
        Key=f'{file_prefix}/{bookmark_file}',
        Body=bookmark_contents.encode('utf-8')
    )