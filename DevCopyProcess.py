import boto3
import os
import logging
import sys
import traceback
import pprint
import time
from decimal import Decimal
import json
import sentry_sdk
sentry_sdk.init(dsn="https://b2c77e36a6794ca08dd31681c645c876@sentry.io/1412782")

import Utilities

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

if 'AWS_BATCH_JOB_ARRAY_INDEX' in os.environ:
    count = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])
else:
    count = 0

logger.info("Count: {}".format(count))


class Delivery(object):
    def __init__(self, sg, current_job_data, job_number, entity_type, entity_id, process_type, entity_status_updates):
        self.job_data = current_job_data
        self.count = job_number
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.process_type = process_type
        self.entity_status_updates = entity_status_updates
        self.sg = sg

    def deliver(self):
        logger.info('copy process started')
        if self.count == 0:
            self.sg.create("Reply", {
                "entity": {"type": self.entity_type, "id": int(self.entity_id)},
                "content": "AWS Copy Started..."
            })

        target_client = boto3.client('s3')
        source_client = boto3.client('s3')

        copy_source = {
            'Bucket': self.job_data['source_bucket'],
            'Key': self.job_data['source_key']
        }

        logger.info('trying to copy %s to %s %s' % (str(copy_source), self.job_data['target_bucket'],
                                                    self.job_data['target_key']))

        target_client.copy(copy_source, self.job_data['target_bucket'],
                           self.job_data['target_key'], SourceClient=source_client)

        logger.info("Copy Completed!")
        return '%s://%s' % (self.job_data['target_bucket'], self.job_data['target_key'])


def main():
    """
    :return:
    """
    bucket_name = str(sys.argv[1])
    json_key = str(sys.argv[2])
    logger.info('bucket_name - %s, json_key - %s' % (bucket_name, json_key))

    logger.info('downloading and parsing json')
    job_data = Utilities.get_json_data(bucket=bucket_name, obj_key=json_key)
    logger.info('downloaded and parsing json')

    required_keys = ['entity_type', 'entity_id', 'process_type', 'copy_data']
    for _key in required_keys:
        if _key not in job_data.keys():
            raise ValueError('Invalid json file received')

    start_time = time.time()
    target_path = ''
    try:
        logger.info("getting sg object")
        sg = Utilities.get_sg_object(job_data=job_data)
        # dict to create the kwargs for the copy process
        current_job_data = job_data['copy_data'][count]

        logger.info("Current Job kwargs: {}".format(pprint.pformat(current_job_data)))
        delivery = Delivery(sg=sg,
                            current_job_data=current_job_data,
                            job_number=count,
                            entity_type=job_data['entity_type'],
                            entity_id=int(job_data['entity_id']),
                            process_type=job_data['process_type'],
                            entity_status_updates=job_data['entity_status_updates'])
        target_path = delivery.deliver()
        status = "Success"
        error_msg = ''

    except Exception as e:
        status = "Failure"
        error_msg = 'Error while copying S3 object, json file - %s://%s, count - %s\n\nTraceback:\n%s' %\
                    (bucket_name, json_key, count, traceback.format_exc())

    process_time = time.time()-start_time

    db_data_to_update = {'ProcessID': str(job_data['entity_id']), 'ProcessNumber': count + 1,
                         'TargetPath': target_path, 'Status': status, 'Error': error_msg,
                         'ProcessTime': round(process_time, 2)}
    db_data_to_update = json.loads(json.dumps(db_data_to_update), parse_float=Decimal)
    try:
        logger.info("trying to update dynamo db")
        dynamo_db = boto3.resource('dynamodb', region_name=job_data['batch_region'])
        dynamo_table = dynamo_db.Table(job_data['dynamo_table_name'])
        response = dynamo_table.put_item(Item=db_data_to_update)
        logger.info("updated dynamo db")
    except:
        logger.info("failed to update dynamo db - raising error for sentry")
        logger.exception('dynamodb update failed')
        error_msg = 'Failed to update dynamo db for status'
        error_msg += '\n\nStatus of transaction\n-%s' % str(db_data_to_update)
        raise ValueError(error_msg)


if __name__ == '__main__':
    main()
