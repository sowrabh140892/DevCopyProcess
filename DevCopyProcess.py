import boto3
import json
import os
from os import environ
import time
import re
import logging
import sys
import shotgun_api3
import sentry_sdk
from sentry_sdk.integrations.logging import ignore_logger
sentry_sdk.init(dsn='https://2fee4ed938294813aeeb28f08e3614b8@sentry.io/1858927')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    SHOTGUN_SCRIPT_NAME=str(os.environ['SHOTGUN_SCRIPT_NAME'])
    SHOTGUN_SCRIPT_KEY=str(os.environ['SHOTGUN_SCRIPT_KEY'])
    SHOTGUN_HOST_NAME=str(os.environ['SHOTGUN_HOST_NAME'])	
except Exception as e:
    raise ValueError('Error while accessing Shotgun details from environment variables')
    logger.info('Error while accessing Shotgun details from environment variables')



if 'AWS_BATCH_JOB_ARRAY_INDEX' in os.environ:
    count=int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])
else:
    count=0

print(count)



BUCKET='aws-batch-parameter'
KEY=str(sys.argv[1])
session = boto3.Session()

try:
    s3 = boto3.resource('s3')
    s3.Bucket(BUCKET).download_file(KEY, '/tmp/%s'%(KEY))
except Exception as e:
    raise ValueError('Error while downloading temmplate file %s from %s '%(BUCKET))
    logger.info(' ERROR while downloading template file')
finally:
    logger.info('Downloading Template Ended')

file=open('/tmp/%s'%(KEY)).read().splitlines()
print(file[count])

line = file[count].split(' ')
source_bucket_key=str(line[0])
destination_bucket_key=str(line[1])
source_bucket_name=str(line[2])
source_region=str(line[3])
destination_bucket_name=str(line[4])
destination_region=str(line[5])
SHOTGUN_TYPE=str(line[7])
SHOTGUN_ENTITY_ID=str(line[8])
SHOTGUN_ENTITY_TYPE=str(line[9])
SHOTGUN_ATTRIBUTE_NAME=str(line[10])
SHOTGUN_ATTRIBUTE_VALUE=str(line[11])
SHOTGUN_ATTRIBUTE_VALUE=SHOTGUN_ATTRIBUTE_VALUE.replace("-"," ")

print(line)

if count==0:
    sg = shotgun_api3.Shotgun(SHOTGUN_HOST_NAME, SHOTGUN_SCRIPT_NAME, SHOTGUN_SCRIPT_KEY)
    sg.create("Reply", {"entity": {"type": SHOTGUN_ENTITY_TYPE, "id": int(SHOTGUN_ENTITY_ID)},"content": "AWS Copy Started..."})
					

def copy_to_vfx_vendor_test(source_bucket_key, destination_bucket_key, source_bucket_name, destination_bucket_name):
    """
    :param bucket_key:
    :param source_bucket_name:
    :param destination_bucket_name:
    :return:
    """
    try:
        s3 = boto3.client('s3', destination_region)
        source_client = boto3.client('s3', source_region)
        copy_source = {
            'Bucket': source_bucket_name,
            'Key': source_bucket_key
        }
        s3.copy(copy_source, destination_bucket_name, destination_bucket_key, SourceClient=source_client)
        print "Copy Completed!"
    except Exception as e:
        raise ValueError('Error while copying S3 objects %s from %s to %s - %s '%(source_bucket_key,source_bucket_name,destination_bucket_name,destination_bucket_key))
    finally:
        logger.info('Copying process ended')

def main():
    """
    :return:
    """
    copy_to_vfx_vendor_test(source_bucket_key, destination_bucket_key, source_bucket_name, destination_bucket_name)
	
if __name__ == '__main__':
    main()
