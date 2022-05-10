from aiohttp import ClientSession
import asyncio
import os
import yarl
import boto3

import pandas as pd
from datetime import datetime as dt
from datetime import timedelta as td

async def upload_file(session: ClientSession, s3_client: boto3.client, key: str) -> dict:
    """
    Downloads file from Github archive and uploads it to AWS S3 bucket in async way
    S3 bucket name is set in environment variable 
    @session aiohttp.ClientSession object
    @s3_client client to connect with AWS S3
    @key filename to download from Github archive
    """
    BUCKET_NAME = os.environ.get('GH_BUCKET_NAME')

    url = yarl.URL(f'https://data.gharchive.org/{key}.json.gz', encoded=True)

    async with session.get(url, allow_redirects=False) as response:

        try:
            print('[extract] downloading data from', response.url)
            stream_bytes = await response.read()

            print('[extract] loading data to bucket', BUCKET_NAME)
            
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=f'landing/{key}.json.gz',
                Body=stream_bytes,

        )
        except Exception as e:
            print(e)
            return False

    return True

async def main():

    START_DATE_RANGE = os.environ.setdefault('START_DATE_RANGE', '2021-01-01')
    END_DATE_RANGE = os.environ.setdefault('END_DATE_RANGE', '2021-01-03')
    boto3.setup_default_session(profile_name='glad')
    
    filenames = [
                dt.strftime(dr + td(hours=hour),'%Y-%m-%d-%-H') 
                for hour in range(1, 25) 
                for dr in pd.date_range(START_DATE_RANGE, END_DATE_RANGE)
            ]

    s3_client = boto3.client('s3')
    async with ClientSession() as session:
        tasks = [upload_file(session, s3_client, filename) for filename in filenames]
        results = await asyncio.gather(*tasks)
        print(f'[extract] Uploaded {sum(result for result in results if type(result) == bool)} files successfully')

asyncio.run(main())