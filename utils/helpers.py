import boto3
import os
import sys
import json
import traceback
from typing import Any, Dict, Tuple
from dotenv import load_dotenv
load_dotenv()

def _parse_arn(arn: str) -> Dict:
    """
    Function to parse ARN to extract components. Following are the
    different ARN formats.

    arn:partition:service:region:account_id:resource_id\n
    arn:partition:service:region:account_id:resource_type/resource_id\n
    arn:partition:service:region:account_id:resource_type/resource_id\n

    For more documentation, read this;
    https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html

    Parameters:
        arn (str): The ARN to be parsed.

    Returns:
        result (dict): Returns a dictionary of different components of ARN.
        {\n
        'arn': elements[0],\n
        'partition': elements[1],\n
        'service': elements[2],\n
        'region': elements[3],\n
        'account': elements[4],\n
        'resource': elements[5],\n
        'resource_type': None\n
        }
    """

    try:
        elements = arn.split(':', 5)
        result = {
        'arn': elements[0],
        'partition': elements[1],
        'service': elements[2],
        'region': elements[3],
        'account': elements[4],
        'resource': elements[5],
        'resource_type': None
        }
        if '/' in result['resource']:
            result['resource_type'], result['resource'] = result['resource'].split('/',1)
        elif ':' in result['resource']:
            result['resource_type'], result['resource'] = result['resource'].split(':',1)
        return result
    except:
        print(f"ARN {arn} could not be parsed")
        sys.exit(0)

def create_sns_topic(sns_client: Any) -> str:
    """
    Function to create a SNS topic for summary report generation. Can be 
    generalized to create any SNS topic, just need to alter the logic for
    the SNS topic name. Right now it's just pulling from the .env file.

    Parameters:
        sns_client (Any): SNS client object.

    Returns:
        str: Returns the SNS ARN of the concerned topic.
    """
    # First check if any SNS topics present.
    SNS_TOPIC_NAME = os.getenv('SNS_TOPIC_NAME')
    topics = []

    response = sns_client.list_topics()
    if "Topics" in response:
        if response['Topics']:
            topics.extend(response['Topics'])
    if "NextToken" in response:
        while True:
            next_token = response['NextToken']
            contd_response = sns_client.list_topics(NextToken=next_token)
            topics.extend(contd_response["Topics"])
            if "NextToken" in contd_response:
                next_token = contd_response['NextToken']
            else:
                break
    
    for topic in topics:
        topic_name = {}
        topic_name = _parse_arn(topic['TopicArn'])
        # If Topic already present, pass back the ARN.
        if topic_name:
            if SNS_TOPIC_NAME == topic_name.get('resource'):
                print("\x1b[32mSNS topic already created.\x1b[0m")
                return topic['TopicArn']
    # If Topic not present, create and pass ARN.
    sns_response = sns_client.create_topic(Name = SNS_TOPIC_NAME)
    print("\x1b[32mCreated a SNS topic\x1b[0m")
    return sns_response['TopicArn']


def create_sqs_queue(sqs_client: Any) -> Tuple[str, str]:
    """
    Function to create a SQS queue if it doesn't exist.

    Parameters:
        sqs_client (Any): SQS client object.

    Returns:
        sqs_url (str): SQS URL.
        sqs_arn (str): SQS ARN.
    """
    SQS_QUEUE_NAME = os.getenv('SQS_QUEUE_NAME')
    urls = []

    response = sqs_client.list_queues()
    if "QueueUrls" in response:
        if response['QueueUrls']:
            urls.extend(response['QueueUrls'])
    if "NextToken" in response:
        while True:
            next_token = response['NextToken']
            contd_response = sqs_client.list_queues(NextToken=next_token)
            urls.extend(contd_response['QueueUrls'])
            if "NextToken" in contd_response:
                next_token = contd_response['NextToken']
            else:
                break

    for url in urls:
        queue_name = url.split('/')[-1]
        if queue_name:
            if SQS_QUEUE_NAME == queue_name:
                print("\x1b[32mSQS queue already created.\x1b[0m")
                try:
                    sqs_attrs = sqs_client.get_queue_attributes(QueueUrl = url, AttributeNames = ['QueueArn'])
                    sqs_arn = sqs_attrs['Attributes']['QueueArn']
                    return url, sqs_arn
                except Exception as e:
                    print(f"ERROR: {e}\n\nCouldn't retrieve SQS queue ARN.")
                    traceback.print_exc()
                    sys.exit(0)

    try:
        sqs_response = sqs_client.create_queue(QueueName = SQS_QUEUE_NAME)
        print("\x1b[32mCreated SQS Queue\x1b[0m")
    except Exception as e:
        print(f"ERROR: {e}\n\nCouldn't create SQS queue.")
        traceback.print_exc()
        sys.exit(0)
    try:
        sqs_attrs = sqs_client.get_queue_attributes(QueueUrl = sqs_response['QueueUrl'], AttributeNames = ['QueueArn'])
        sqs_arn = sqs_attrs['Attributes']['QueueArn']
        return sqs_response['QueueUrl'], sqs_arn
    except Exception as e:
        print(f"ERROR: {e}\n\nCouldn't retrieve SQS queue ARN")
        traceback.print_exc()
        sys.exit(0)
    
def subscribe(sns_arn: str, sqs_arn: str, sqs_url: str, sns_client: Any) -> None:
    """
    Function that subscribes SQS queue to SNS notification topic.

    Parameters:
        sns_arn (str): SNS ARN.
        sqs_arn (str): SQS ARN.
        sqs_url (str): SQS URL to retrieve queue attributes.
        sns_client (Any): SNS client object.

    Returns:
        None
    """
    # First let's check if there is a subscription already present.
    flag_policy_present = False
    paginator = sns_client.get_paginator('list_subscriptions_by_topic')
    for page in paginator.paginate(TopicArn=sns_arn):
        if page["Subscriptions"]:
            for subscription in page['Subscriptions']:
                endpoint = _parse_arn(subscription["Endpoint"]).get("resource")
                if os.getenv('SQS_QUEUE_NAME') == endpoint:
                    print(f"\x1b[32mSubscription is already present for queue {endpoint}.\x1b[0m")
                    return None

    sns_client.subscribe(TopicArn = sns_arn, Protocol = 'sqs', Endpoint = sqs_arn)
    print("\x1b[32mSubscribed SQS Queue to SNS Topic\x1b[0m")


def start_label_detection(bucket: str, 
                          folder: str, 
                          key: str, 
                          rekognition_client: Any,
                          sns_arn: str):
    """
    Function to start label_detection async job.

    Parameters:
        bucket (str): S3 bucket.
        folder (str): Folder in which videos are present.
        key (str): filename of video.
        rekognition_client (Any): Rekognition client object.
        sns_arn (str): SNS ARN for notification channel.

    Returns:
        jobId (str): Returns the job ID of the async job started.
    """
    SNS_ROLE_ARN = os.getenv("SNS_ROLE_ARN")
    if not SNS_ROLE_ARN:
        raise ValueError
    response = rekognition_client.start_label_detection(Video = {"S3Object": {'Bucket': bucket,
                                                                              'Name': os.path.join(folder, key)
                                                                              }
                                                                },
                                                        NotificationChannel={'SNSTopicArn': sns_arn,
                                                                             'RoleArn': SNS_ROLE_ARN
                                                                             },
                                                        MinConfidence = 75)
    
    print("\x1b[32mStarted Label detection async job.\x1b[0m")
    
    return response['JobId']

def poll_sqs_for_job_completion(expected_job_id, sqs_client: Any, sqs_url: str, wait_time=20, max_attempts=30):
    """
    Poll the SQS queue for messages from SNS about Rekognition job completion.
    Waits until the job with expected_job_id is SUCCEEDED or FAILED.

    Parameters:
        expected_job_id (str): Job ID to poll.
        sqs_client (Any): SQS client object.
        sqs_url (str): SQS Url.
        wait_time (int): Wait time.
        max_attempts (int): Max attempts before quitting polling.

    Returns:
        Job status.
    """
    print("\x1b[31mStarting polling...\x1b[0m")
    attempts = 0
    while attempts < max_attempts:
        response = sqs_client.receive_message(
            QueueUrl=sqs_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=wait_time
        )
        messages = response.get('Messages', [])
        print(messages)
        for message in messages:
            # SNS message is JSON inside the SQS message body
            body = json.loads(message['Body'])
            sns_message = json.loads(body['Message'])  # SNS message is a JSON string
            
            job_id = sns_message.get('JobId')
            status = sns_message.get('Status')  # 'SUCCEEDED' or 'FAILED'

            print(f"Status: {status}")
            
            if job_id == expected_job_id and status == 'SUCCEEDED':
                # Delete the message from SQS after processing
                sqs_client.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                print(f"\x1b[32mJob {job_id} finished with status: {status}\x1b[0m")
                return True
            elif status == 'FAILED':
                print("\x1b[31mJob failed\x1b[0m")
            else:
                # Not the job we are waiting for, just delete and ignore
                sqs_client.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
        attempts += 1
        print(f"\x1b[33mWaiting for job completion notification... attempt {attempts}/{max_attempts}\x1b[0m")
    return False

def get_label_detection_results(job_id, rekognition_client: Any):
    """
    Fetch the label detection results from Rekognition.
    Handles pagination with NextToken.
    """
    labels = []
    next_token = None
    while True:
        if next_token:
            response = rekognition_client.get_label_detection(JobId=job_id, NextToken=next_token, SortBy='TIMESTAMP')
        else:
            response = rekognition_client.get_label_detection(JobId=job_id, SortBy='TIMESTAMP')

        if response["JobStatus"] == "SUCCEEDED":
            labels.extend(response['Labels'])
            next_token = response.get('NextToken')
            if not next_token:
                break
        else:
            print(f"Status: {response['JobStatus']}")
            return None
    return labels