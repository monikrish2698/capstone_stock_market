from include.aws.aws_secret_manager import get_secret

common_kwargs = {
    "s3_bucket" : get_secret("AWS_S3_BUCKET_TABULAR"),
    "catalog_name" : get_secret("CATALOG_NAME"),
    "tabular_credential" : get_secret("TABULAR_CREDENTIAL"),
    "aws_access_key_id" : get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key" : get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY"),
    "aws_region" : get_secret("AWS_GLUE_REGION"),
    "polygon_credentials" : get_secret("POLYGON_CREDENTIALS"),
}