import boto3
import requests
from requests_aws4auth import AWS4Auth

# This is the code that is used to authenticate the Lambda function with the Amazon ES domain.
region = 'us-east-1' 
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# This is the code that is used to authenticate the Lambda function with the Amazon ES domain.
host = 'https://exampleurl.com' # the Amazon ES domain, with https://
index = 'lambda-index'
type = '_doc'
url = host + '/' + index + '/' + type + '/'

headers = { "Content-Type": "application/json" }

def lambda_handler(event, context):
    """
    For each record in the event, get the primary key, and if the event is a delete, delete the record
    from Elasticsearch, otherwise, put the record into Elasticsearch
    
    :param event: This is the event that triggered the Lambda function. In this case, it's the DynamoDB
    stream
    :param context: This is a Lambda-provided object that contains information about the invocation,
    function, and execution environment
    :return: The number of records processed.
    """
    count = 0
    for record in event['Records']:
        print(record)
        # Get the primary key for use as the Elasticsearch ID
        id = record['dynamodb']['Keys']['retailer_id']['S']


        if record['eventName'] == 'REMOVE':
            r = requests.delete(url + id, auth=awsauth)
            print(r.status_code , "if clause")
            print(r.text)
        else:
            document = record['dynamodb']['NewImage']
            r = requests.put(url + id, auth=awsauth, json=document, headers=headers)
            print(r.status_code)
            print("else clause")
            print(r.text)
        count += 1
    return str(count) + ' records processed.'
