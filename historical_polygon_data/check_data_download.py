import boto3
from botocore.config import Config

session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key=''
)

s3 = session.client(
  's3',
  endpoint_url='https://files.polygon.io',
  config=Config(signature_version='s3v4'),
)

years = [2025]
paginator = s3.get_paginator('list_objects_v2')

total = 0
for year in years:
    for page in paginator.paginate(Bucket='flatfiles', Prefix=f'us_stocks_sip/day_aggs_v1/{year}/01/'):
        for obj in page.get("Contents", []):
            total += obj["Size"]
print(f"Total input size (bytes): {total}")