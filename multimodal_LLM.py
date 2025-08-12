import boto3
import os
import base64
import json
from dotenv import load_dotenv
from botocore.exceptions import ClientError
load_dotenv()

# Defining Global variables
AWS_ACCESS_KEY = None
AWS_SECRET_KEY = None
MODEL_ID = 'us.amazon.nova-pro-v1:0'
# MODEL_ID = 'us.amazon.nova-premier-v1:0'
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

bedrock_client = session.client("bedrock-runtime")

system_prompt = ("You are a law officer. Analyze the video and mention any illegal activity that takes place such as arson, arrest, abuse, theft, physical violence, etc. Make sure your output is in the JSON format. Here is a sample output format.\n"
                 "{\n"
                 "  \"flag\": \"SUSPICIOUS\",\n"
                 "  \"activity\": \"Looking into the camera\",\n"
                 "}")

system_list= [
    {
        "text": system_prompt
    }
]
# Define a "user" message including both the image and a text prompt.
message_list = [
    {
        "role": "user",
        "content": [
            {
                "text": system_prompt
            },
            {
                "video": {
                    "format": "mp4",
                    "source": {
                        "s3Location": {
                            'uri': f"s3://{S3_BUCKET}/{FOLDER}/{FILENAME}"
                        }
                    },
                }
            }
        ],
    }
]

# Invoke the model and extract the response body.
try:
    response = bedrock_client.converse(modelId=MODEL_ID, 
                                    messages=message_list)
    output_message = response['output']['message']

    print(f"Role: {output_message['role']}")

    for content in output_message['content']:
        print(f"Text: {content['text']}")

    token_usage = response['usage']
    print(f"Input tokens:  {token_usage['inputTokens']}")
    print(f"Output tokens:  {token_usage['outputTokens']}")
    print(f"Total tokens:  {token_usage['totalTokens']}")
    print(f"Stop reason: {response['stopReason']}")

except ClientError as err:
    message = err.response['Error']['Message']
    print(f"A client error occured: {message}")

else:
    print(
        f"Finished generating text with model {MODEL_ID}.")

# %%
from utils.helpers import _list_foundational_models, _list_inference_profiles

# _list_foundational_models()
# _list_inference_profiles()
# %%
