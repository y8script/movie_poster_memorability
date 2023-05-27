import requests
import dataset
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import boto3
import json
from datetime import date, datetime

import pandas as pd
import numpy as np



imdb_base_url = 'https://www.imdb.com/title/'

sfn = boto3.client('stepfunctions')
rds = boto3.client('rds')


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))




def make_def(lambda_arn):

    definition = {
        "Comment": "My State Machine",
        "StartAt": "Map",
        "States": {
            "Map": {
                "Type": "Map",
                "End": True,
                "Iterator": {
                    "StartAt": "Lambda Invoke",
                    "States": {
                        "Lambda Invoke": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "OutputPath": "$.Payload",
                            "Parameters": {
                                "Payload.$": "$",
                                "FunctionName": lambda_arn
                            },
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "Lambda.ServiceException",
                                        "Lambda.AWSLambdaException",
                                        "Lambda.SdkClientException",
                                        "Lambda.TooManyRequestsException",
                                        "States.TaskFailed"
                                    ],
                                    "IntervalSeconds": 2,
                                    "MaxAttempts": 6,
                                    "BackoffRate": 2
                                }
                            ],
                            "End": True
                        }
                    }
                }
            }
        }
    }
    return definition

def main():

    # Create RDS instance
    global state_machine_arn
    try:
        response = rds.create_db_instance(
            DBInstanceIdentifier='relational-db-movies',
            DBName='moviesdb',
            MasterUsername='yuetong',
            MasterUserPassword='password',
            DBInstanceClass='db.t2.micro',
            Engine='mysql',
            AllocatedStorage=5
        )

        # Wait until DB is available to continue
        rds.get_waiter('db_instance_available') \
            .wait(DBInstanceIdentifier='relational-db-movies')


        # Describe where DB is available and on what port
        response = rds.describe_db_instances()
        db = [ tmp_db for tmp_db in response['DBInstances'] if tmp_db['DBName'] == 'moviesdb'][0]
        ENDPOINT = db['Endpoint']['Address']
        PORT = db['Endpoint']['Port']
        DBID = db['DBInstanceIdentifier']
    except:
        response = rds.describe_db_instances()
        db = [ tmp_db for tmp_db in response['DBInstances'] if tmp_db['DBName'] == 'moviesdb'][0]
        ENDPOINT = db['Endpoint']['Address']
        PORT = db['Endpoint']['Port']
        DBID = db['DBInstanceIdentifier']

    # Get Name of Security Group
    SGNAME = db['VpcSecurityGroups'][0]['VpcSecurityGroupId']

    # Adjust Permissions for that security group so that we can access it on Port 3306
    # If already SG is already adjusted, print this out
    try:
        ec2 = boto3.client('ec2')
        data = ec2.authorize_security_group_ingress(
            GroupId=SGNAME,
            IpPermissions=[
                {'IpProtocol': 'tcp',
                 'FromPort': PORT,
                 'ToPort': PORT,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
            ]
        )
    except ec2.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == 'InvalidPermission.Duplicate':
            print("Permissions already adjusted.")
        else:
            print(e)

    print('RDS ready!')

    # Create Lambda Function
    # Access our class IAM role, which allows Lambda
    # to interact with other AWS resources
    aws_lambda = boto3.client('lambda')
    iam_client = boto3.client('iam')
    role = iam_client.get_role(RoleName='LabRole')

    # Open zipped directory
    with open('movie_scrap_deployment.zip', 'rb') as f:
        lambda_zip = f.read()

    try:
        # If function hasn't yet been created, create it
        response = aws_lambda.create_function(
            FunctionName='movie_lambda',
            Runtime='python3.9',
            Role=role['Role']['Arn'],
            Handler='lambda_function_scrapping.lambda_handler',
            Code=dict(ZipFile=lambda_zip),
            Timeout=60
        )
    except aws_lambda.exceptions.ResourceConflictException:
        # If function already exists, update it based on zip
        # file contents
        response = aws_lambda.update_function_code(
            FunctionName='movie_lambda',
            ZipFile=lambda_zip
            )

    print('Lambda function ready!')

    lambda_arn = response['FunctionArn']

    # Create Step Function state machine
    sf_def = make_def(lambda_arn)

    try:
        response = sfn.create_state_machine(
            name='movie-final',
            definition=json.dumps(sf_def),
            roleArn=role['Role']['Arn'],
            type='EXPRESS'
        )
        response = sfn.list_state_machines()
        state_machine_arn = [sm['stateMachineArn']
                             for sm in response['stateMachines']
                             if sm['name'] == 'movie-final'][0]
    except sfn.exceptions.StateMachineAlreadyExists:
        response = sfn.list_state_machines()
        state_machine_arn = [sm['stateMachineArn']
                             for sm in response['stateMachines']
                             if sm['name'] == 'movie-final'][0]
        response = sfn.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=json.dumps(sf_def),
            roleArn=role['Role']['Arn']
        )
    print('Step function ready!')

    # Connect to RDS
    username = 'yuetong'
    password = 'password'
    db_url = \
            "mysql+mysqlconnector://{}:{}@{}:{}/moviesdb".format(username,password,ENDPOINT,PORT)
    '''
        db = None
    while True:
        try:
            db = dataset.connect(db_url)
            break
        except:
            continue
    '''
    # db = dataset.connect(db_url)
    # print('database connected!')

    # Scrape the pages in the catalogue
    url = imdb_base_url

    # failed_list = [] # Run this once to initialize, then comment out to append continuously

    # Need to provide a csv of IMDb IDs to scrape
    movie_df = pd.read_csv('imdb_movie_list.csv')

    start_year = 2015
    end_year = 2018
    movies_in_years = list(movie_df[(movie_df['year'] > start_year) & (movie_df['year'] <= end_year)]['tconst'])

    n = 50
    movies_batches = [{'movies': movies_in_years[i:i + n]} for i in range(0, len(movies_in_years), n)]

    response = sfn.start_sync_execution(
        stateMachineArn=state_machine_arn,
        name='movie_exec',
        input=json.dumps(movies_batches,default=json_serial)
    )
    # db.close()

if __name__ == "__main__":
    main()
