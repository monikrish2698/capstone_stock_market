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

for year in years:
    prefix = f'us_stocks_sip/minute_aggs_v1/{year}'
    for page in paginator.paginate(Bucket='flatfiles', Prefix=prefix):
        for obj in page['Contents']:
            print(obj['Key'])