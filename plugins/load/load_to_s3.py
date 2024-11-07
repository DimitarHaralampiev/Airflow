import boto3
from io import StringIO

def load_to_s3(df, s3_bucket, s3_key, aws_region="eu-south-1"):
    s3_client = boto3.client("s3", region_name=aws_region)
    csv_buffer = StringIO(df.to_csv(index=False))

    try:
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer)
        print(f"File successfully uploaded to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"An error occurred: {e}")