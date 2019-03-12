import sys
import argparse
import boto3
import logging
import os
import json
import requests

# Initialise logging
logger = logging.getLogger(__name__)
log_level = os.environ['LOG_LEVEL'] if 'LOG_LEVEL' in os.environ else 'ERROR'
logger.setLevel(logging.getLevelName(log_level.upper()))
logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s %(levelname)s %(module)s '
                           '%(process)s[%(thread)s] %(message)s')
logger.info("Logging at {} level".format(log_level.upper()))


def get_parameters():
    parser = argparse.ArgumentParser(
        description="Configure Confluent Kafka Consumers using RESTful API")

    # Parse command line inputs and set defaults
    parser.add_argument('--connector.name', default='s3-sink')
    parser.add_argument('--aws-profile', default='default')
    parser.add_argument('--aws-region', default='eu-west-2')
    parser.add_argument('--tasks-max', default='1')
    parser.add_argument('--topics', default='')
    parser.add_argument('--flush-size', default='1')
    parser.add_argument('--s3-bucket-name', default='')

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if 'CONNECTOR_NAME' in os.environ:
        _args.aws_profile = os.environ['CONNECTOR_NAME']

    if 'AWS_PROFILE' in os.environ:
        _args.aws_profile = os.environ['AWS_PROFILE']

    if 'AWS_REGION' in os.environ:
        _args.aws_region = os.environ['AWS_REGION']

    if 'TASKS_MAX' in os.environ:
        _args.tasks_max = os.environ['TASKS_MAX']

    if 'TOPICS' in os.environ:
            _args.tasks_max = os.environ['TOPICS']

    if 'FLUSH_SIZE' in os.environ:
        _args.tasks_max = os.environ['FLUSH_SIZE']

    if 'S3_BUCKET_NAME' in os.environ:
        _args.tasks_max = os.environ['S3_BUCKET_NAME']


    return _args


def handler(event, context):
    args = get_parameters()
    try:
        configure_confluent_kafka_consumer(event, args)
    except KeyError as key_name:
        logger.error(f'Key: {key_name} is required in payload')


def configure_confluent_kafka_consumer(event, args):
    if 'AWS_PROFILE' in os.environ:
        boto3.setup_default_session(profile_name=args.aws_profile,
                                    region_name=args.aws_region)

    if logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        logger.debug(f"Using boto3 {boto3.__version__}")

    logger.debug(event)

    message = json.loads(event['Records'][0]['Sns']['Message'])
    logger.debug(message)

    private_ip = message['detail']['containers'][0]['networkInterfaces'][0]['privateIpv4Address']
    logger.debug(private_ip)

    payload = {
        "config": {
            "connector.name": "s3-sink",
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": args.tasks_max,
            "topics": args.topics,
            "flush.size": args.flush_size,
            "s3.region": args.aws_region,
            "s3.bucket.name": args.s3_bucket_name,
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
            "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
            "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        }
    }
    response = requests.get(f"http://{private_ip}:8083/connectors")
    existing_connector = response.content.decode("utf-8")
    logger.debug(existing_connector)
    print (existing_connector)

    # existing_connector = ['s3-sink', 'other-connector']

    if existing_connector in existing_connector:
        logger.debug("update connector [PUT]")

        response = requests.put(f"http://{private_ip}:8083/connectors", json=payload)
        logger.debug(response.content.decode("utf-8"))

    else:

        logger.debug("create connector [POST]")

        response = requests.post(f"http://{private_ip}:8083/connectors", json=payload)
        logger.debug(response.content.decode("utf-8"))

    for existing_connector in existing_connector:
        if existing_connector not in args.topics:
            logger.debug("delete connector [DELETE]")

            response = requests.post(f"http://{private_ip}:8083/connectors", json=payload)
            logger.debug(response.content.decode("utf-8"))

if __name__ == "__main__":
    try:
        json_content = json.loads(open('event.json', 'r').read())
        handler(json_content, None)
    except Exception as e:
        logger.error("Unexpected error occurred")
        logger.error(e)
        raise e

