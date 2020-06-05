import boto3
import os
import logging
import sys
import shotgun_api3
import sentry_sdk
import json
import pprint
import traceback

sentry_sdk.init(dsn='https://2fee4ed938294813aeeb28f08e3614b8@sentry.io/1858927')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('CopyProcess-Log')

try:
    SHOTGUN_SCRIPT_NAME = str(os.environ['SHOTGUN_SCRIPT_NAME'])
    SHOTGUN_SCRIPT_KEY = str(os.environ['SHOTGUN_SCRIPT_KEY'])
    SHOTGUN_HOST_NAME = str(os.environ['SHOTGUN_HOST_NAME'])
except Exception as e:
    raise ValueError('Error while accessing Shotgun details from environment variables')


if 'AWS_BATCH_JOB_ARRAY_INDEX' in os.environ:
    count = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])
else:
    count = 0

logger.info("Count: {}".format(count))

BUCKET = 'aws-batch-parameter'
KEY = str(sys.argv[1])
session = boto3.Session()

try:
    s3 = boto3.resource('s3')
    s3.Bucket(BUCKET).download_file(KEY, '/tmp/%s' % KEY)
except Exception as e:
    raise ValueError('Error while downloading template file %s from %s ' % (KEY, BUCKET))
finally:
    logger.info('Downloading Template Ended')


class Delivery(object):
    def __init__(self, current_job_data, job_number, entity_type, entity_id, process_type, entity_status_updates):
        self.job_data = current_job_data
        self.count = job_number
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.process_type = process_type
        self.entity_status_updates = entity_status_updates
        self.sg = shotgun_api3.Shotgun(SHOTGUN_HOST_NAME, SHOTGUN_SCRIPT_NAME, SHOTGUN_SCRIPT_KEY)
        self.status = "NA"

    def deliver(self):
        if self.count == 0:
            self.sg.create("Reply", {
                "entity": {"type": self.entity_type, "id": int(self.entity_id)},
                "content": "AWS Copy Started..."
            })

        try:
            s3 = boto3.client('s3', self.job_data['target_region'])
            source_client = boto3.client('s3', self.job_data['source_region'])

            copy_source = {
                'Bucket': self.job_data['source_bucket'],
                'Key': self.job_data['source_key']
            }

            s3.copy(copy_source, self.job_data['target_bucket'],
                    self.job_data['target_key'], SourceClient=source_client)

            self.status = "Success"
            logger.info("Copy Completed!")

        except Exception as e:
            self.status = "Failure"
            raise ValueError('Error while copying S3 objects %s from %s to %s - %s\nTraceback: %s' %
                             (self.job_data['source_key'], self.job_data['source_bucket'],
                              self.job_data['target_bucket'], self.job_data['target_key'], traceback.format_exc()))

        finally:
            logger.info('Copying process ended')
            response = table.put_item(
                Item={'ProcessID': self.entity_id, 'ProcessNumber': self.count + 1, 'Status': self.status})
            logger.info("PutItem succeeded: %s" % response)


def main():
    """
    :return:
    """
    with open('/tmp/%s' % KEY) as batch_param_file:
        job_data = json.load(batch_param_file)

        # dict to create the kwargs for the copy process
        current_job_data = job_data['copy_data'][count]

        logger.info("Current Job kwargs: {}".format(pprint.pformat(current_job_data)))
        delivery = Delivery(current_job_data=current_job_data,
                            job_number=count,
                            entity_type=job_data['entity_type'],
                            entity_id=int(job_data['entity_id']),
                            process_type=job_data['process_type'],
                            entity_status_updates=job_data['entity_status_updates'])
        delivery.deliver()


if __name__ == '__main__':
    main()
