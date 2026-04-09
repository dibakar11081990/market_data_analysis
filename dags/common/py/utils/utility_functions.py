from airflow.hooks.base import BaseHook
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
import boto3
import json
import snowflake.connector
import logging
from types import SimpleNamespace
from datetime import datetime
import inspect
import os
from dags.common.py.utils.verbose_log import verbose, log






logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airflow.task")


def aws_conn_credentials():
    try:
        aws_conn = BaseHook.get_connection('aws_default')
    except Exception as e:
        verbose(e, 'ERROR')
    else:
        aws_access_key = aws_conn.login
        aws_secret_key = aws_conn.password
        
        return aws_access_key, aws_secret_key

def get_secretmanager_parameters(SECRET_NAME:str, FETCH_KEY:str, REGION_NAME:str='us-east-1'):

    secret_name = SECRET_NAME
    region_name = REGION_NAME

    aws_access_key, aws_secret_key = aws_conn_credentials()

    session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
            )
    # Create a Secrets Manager client
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value( SecretId=secret_name )
    except ClientError as e:
        raise e
    else:
        api_keys = get_secret_value_response['SecretString']
        decypted_api_keys = json.loads(api_keys)

        secret_value = decypted_api_keys[FETCH_KEY]

    return secret_value

def get_ssm_parameter(name, REGION_NAME='us-east-1', with_decryption=True):

    aws_access_key, aws_secret_key = aws_conn_credentials()

    try:
        session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=REGION_NAME
                )
        
        ssm_client = session.client('ssm', region_name=REGION_NAME)

        response = ssm_client.get_parameter(
            Name=name,
            WithDecryption=with_decryption
        )
        return response['Parameter']['Value']
    
    except Exception as e:
        print(f'::::Error while extracting SFTP creds from AWS, ERROR: {e}')
        return None

def upload_to_aws(local_file, bucket, s3_file_loc, ACCESS_KEY, SECRET_KEY):
    s3 = boto3.client('s3', 
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY
    )

    try:
        s3.upload_file(local_file, bucket, s3_file_loc)
        verbose(f":::::: File {s3_file_loc} uploaded to S3 ::::::")
        return True
    except FileNotFoundError:
        verbose(f":::::: File {local_file} was not found ::::::", 'ERROR')
        return False
    except NoCredentialsError:
        verbose(":::::: Credentials not available ::::::", 'ERROR')
        return False

def check_file_exists_in_s3(bucket, s3_file_loc, ACCESS_KEY, SECRET_KEY):

    try:
        aws_session = boto3.Session(
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    region_name='us-east-1'
                )
    except Exception as e:
        verbose(f"{e}", 'ERROR')
    
    s3 = aws_session.client('s3')

    try:
        s3.head_object(Bucket=bucket, Key=s3_file_loc)
        verbose(f":::::: File {s3_file_loc} exists in S3 ::::::")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            verbose(f"File '{s3_file_loc}' not found in bucket '{bucket}'.", 'ERROR')
            return False
        else:
            verbose(f"Error occurred: {e}", 'ERROR')
            return False
    
def get_boto3_session(REGION_NAME:str='us-east-1'):

    aws_access_key, aws_secret_key = aws_conn_credentials()

    try: 
        session = boto3.Session(
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=REGION_NAME
                    )
    except Exception as e:
        verbose(f"{e}", 'ERROR')
        raise 
    
    return session

    
def snowflake_conn(snowflake_connection_id):
    conn = BaseHook.get_connection(snowflake_connection_id)
    extra_field = json.loads(conn.extra)
    
    try:
        ctx = snowflake.connector.connect(
            account=extra_field['account'],
            user=conn.login,
            password=conn.password,
            warehouse=extra_field['warehouse'],
            database=extra_field['database'],   
            schema=conn.schema,      
            role=extra_field['role']
        )
        return ctx
    except KeyError as e:
        log(f"Missing required field in connection config: {e}", 'ERROR')
        raise
    except Exception as e:
        log(f"Failed to connect to Snowflake: {e}", 'ERROR')
        raise
    
def custom_snowflake_conn(snowflake_connection_id, c_warehouse, c_database, c_schema, c_role):

    conn = BaseHook.get_connection(snowflake_connection_id)

    # conn_attr = ''.join(f'{k}: {v}\n' for k, v in conn.__dict__.items())
    # namespace_attr = ''.join(f'{k}: {v}\n' for k, v in json.loads(conn.extra).items())

    # verbose(f"{conn_attr}", "Connection attributes")
    
    # verbose(f"{namespace_attr}", "Namespace attributes")

    extra_field = json.loads(conn.extra)
    # print(f"******** Warehouse field: {extra_field['warehouse']}")
    try:
        ctx = snowflake.connector.connect(
            account=extra_field['account'],
            user=conn.login,
            password=conn.password,
            warehouse=c_warehouse,
            database=c_database,   
            schema=c_schema,      
            role=c_role
        )
    except Exception as e:
        log(e, 'ERROR')
    else:
        return ctx


def getTableCols(db, schema, table, snowflake_connection_id):
    """
    Returns the underlying Column schema as a list
    :param db: which database to be used
    :param schema: which schema to be used
    :param table: table from which the column list needs to be obtained
    :return: list containing table columns
    """

    conn = snowflake_conn(snowflake_connection_id)
    try:
        # prepare the query which needs to be executed
        query = 'select * from "{database}"."{schema}"."{table}" limit 1;'.format(database=db,
                                                                                    schema=schema,
                                                                                    table=table)

        # Execute the query
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Get the table columns
        table_cols = [col[0] for col in cursor.description]

        # close the cursor object
        cursor.close()
        
        # close the connection to snowflake
        conn.close()

    except Exception as e:
        print("getTableCols() Raised an Exception >>>  " + str(e))
        table_cols = []

    # return the table cols
    return table_cols

def send_sns_notification(topic_arn, message, subject, region_name='us-east-1'):

    conn = BaseHook.get_connection('aws_default')
    aws_access_key = conn.login
    aws_secret_key = conn.password

    aws_session = boto3.Session(
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=region_name
                )

    # Initialize SNS client
    sns_client = aws_session.client('sns', region_name=region_name)

    # Publish a message
    response = sns_client.publish(TopicArn=topic_arn, Message=message, Subject=subject,)

    verbose(":::::: Message ID: ", response['MessageId'])


