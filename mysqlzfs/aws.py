#!/bin/env python3

import boto3
import os
from botocore.exceptions import ClientError


def aws_http_response(response):
    pass


def s3_upload(bucket, file_name, key):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=key)

        # Only return True when s3part does not exist
        # otherwise re-upload to be sure.
        if not os.path.isfile('%s.s3part' % file_name):
            return True
    except ClientError as err:
        pass

    try:
        s3.upload_file(file_name, bucket, key, ExtraArgs={'StorageClass': 'STANDARD_IA'})
    except ClientError as err:
        return False

    return True
