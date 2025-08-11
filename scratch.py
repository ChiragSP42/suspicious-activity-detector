import boto3
import os
import json
import time
import sys
from dotenv import load_dotenv
from utils.helpers import (
    create_sns_topic, 
    create_sqs_queue, 
    subscribe, 
    start_label_detection, 
    get_label_detection_results, 
    poll_sqs_for_job_completion)
from typing import Optional, Any
load_dotenv()

# Defining Global variables
AWS_ACCESS_KEY = None
AWS_SECRET_KEY = None
S3_BUCKET = 'rekogntion-testing'
FOLDER = 'Videos'
FILENAME = 'Abuse013_x264.mp4'

# Loading AWS access and secret keys
if not os.getenv("AWS_ACCESS_KEY") or not os.getenv("AWS_SECRET_KEY"):
    print("\x1b[13mAWS access tokens not set\x1b[0m")
else:
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

session = boto3.Session(aws_access_key_id = AWS_ACCESS_KEY,
                    aws_secret_access_key = AWS_SECRET_KEY,
                    region_name = 'us-east-2')


def main():
    print("\x1b[31mCreating Rekognition, SQS and SNS client objects.\x1b[0m")
    rekognition = session.client("rekognition")
    sns_client = session.client("sns")
    sqs_client = session.client("sqs")
    print("\x1b[32mCreated the client objects.\x1b[0m")

    sns_arn = create_sns_topic(sns_client=sns_client)
    sqs_url, sqs_arn = create_sqs_queue(sqs_client=sqs_client)

    print(f"SNS ARN: {sns_arn}\nSQS URL: {sqs_url}\nSQS ARN: {sqs_arn}")
    sys.exit(0)

    subscribe(sns_arn=sns_arn,
              sqs_arn=sqs_arn,
              sqs_url=sqs_url,
              sns_client=sns_client)
    
    job_id = start_label_detection(bucket=S3_BUCKET,
                                   folder=FOLDER,
                                   key=FILENAME,
                                   rekognition_client=rekognition,
                                   sns_arn=sns_arn)
    
    # status = poll_sqs_for_job_completion(expected_job_id=job_id, 
    #                                      sqs_client=sqs_client,
    #                                      sqs_url=sqs_url)

    while True:
        labels = get_label_detection_results(job_id=job_id, rekognition_client=rekognition)
        if not labels:
            time.sleep(5)
        else:
            print(f"Retrieved {len(labels)} labels for job {job_id}.")
            input()
            for label in labels:
                print(json.dumps(label, indent = 2))
                input()
            break

    
    # if status:
    #     labels = get_label_detection_results(job_id=job_id, rekognition_client=rekognition)
    #     if labels:
    #         print(f"Retrieved {len(labels)} labels for job {job_id}.")
    #         # print(labels)
    # else:
    #     print("\x1b[31mLabel detection failed.\x1b[0m")

    
if __name__ == "__main__":
    main()
    

    

    








