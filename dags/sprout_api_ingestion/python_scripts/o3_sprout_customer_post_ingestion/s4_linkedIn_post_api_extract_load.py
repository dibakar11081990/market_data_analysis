'''
This python script picks and loads LINKEDIN Post Data from SPROUT API creates a equivalent csv file and puts it to S3
and truncate loads the SF table from it. 
Note that: the location of this s3 and other folder files is diff
'''

from dags.sprout_api_ingestion.python_scripts._helper_scripts.customer_id_api import get_customer_id_and_date
from dags.sprout_api_ingestion.python_scripts._helper_scripts.connections import making_aws_connection
from dags.sprout_api_ingestion.python_scripts._helper_scripts.connections import making_sf_connection


def req_details():
    
    #getting aws connection
    aws_connection_values = making_aws_connection()
    aws_connection = aws_connection_values[0]
    aws_access_key = aws_connection_values[1]
    aws_secret_key = aws_connection_values[2]
    
    #getting sf engine
    sf_engine = making_sf_connection()
    
    #extracting customer_id and date required
    get_customer_id_and_date_values = get_customer_id_and_date()
    
    customer_id = str(get_customer_id_and_date_values['customer_id'])
    auth_token = get_customer_id_and_date_values['auth_token']
    date = get_customer_id_and_date_values['date']
    print("\nPipeline run date for Sprout Profile data: ", date)
    
    return {"aws_session":aws_connection,"aws_access_key":aws_access_key,"aws_secret_key":aws_secret_key,\
        "sf_engine":sf_engine,"customer_id":customer_id,"auth_token":auth_token,"date":date}
    
    
# extracting req_details
req_details_values  = req_details()
aws_session =req_details_values["aws_session"]
aws_access_key =req_details_values["aws_access_key"]
aws_secret_key =req_details_values["aws_secret_key"]
sf_engine =req_details_values["sf_engine"]
customer_id = req_details_values["customer_id"]
auth_token =req_details_values["auth_token"]
date = req_details_values["date"]


#########################
# code from matillion job
#########################

import requests
import boto3
import pandas as pd
import numpy as np
import os
import json
from io import StringIO
from datetime import datetime, timedelta
from pandas import json_normalize
import snowflake.connector
import base64
from botocore.exceptions import ClientError
import io


# Vars
#Dynamically setting the SF database
if os.environ['ENVIRONMENT'] == 'prd':
    DB =  'RAW'
else:
    DB =  'DEV'
    

snowflake_database = DB
snowflake_schema = 'SPROUT'
snowflake_warehouse = 'BSM_QUERY'
snowflake_role = 'BSM_DEVELOPER'
TARGET_TABLE = 'LINKEDIN_POST_DATA'
tags_bucket = "com.tata.marketing.mars.prd.ue1.matillion-data"
region_name = "us-east-1"


bucket = 'com.tata.marketing.mars.dev.ue1.matillion-data'
#Dynamically setting the S3 key or prefix
if os.environ['ENVIRONMENT'] == 'prd':
    key =  'sprout/dt='+date+'/linkedin_post_data.csv' 
    tags_key = 'sprout/dt='+date+'/customer_tags_data.csv'

else:
    key =  'test/dt='+date+'/linkedin_post_data.csv' 
    tags_key = 'test/dt='+date+'/customer_tags_data.csv' 
    

# start_date = '2025-01-01'
# end_date = '2025-08-21'
start_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
end_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')

print("LinkedIn Data extraction started...")
url = 'https://api.sproutsocial.com/v1/'+customer_id+'/analytics/posts'

post_df = pd.DataFrame()


# Get Snowflake credentials from Secret Manager
def get_secret(id):
    """
    Function to retrieve the mysql password from aws secret manager.
    :param id: secret manager id for which data needs to be retrieved.
    :return: json returned by secret manager.
    """
    region_name = "us-east-1"

    # Create a Secrets Manager client
    client = aws_session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:

        get_secret_value_response = client.get_secret_value(
            SecretId=id
        )

    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        else:
            raise e
    else:
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

        return json.loads(secret)


# Get Customer IDs from Snowflake
def get_snowflake_data():
    """
    Get the data from snowflake.
    """

    try:

        # get the snowflake credentials from the secret manager.
        db_cred = get_secret(id='dbt_profile')

        # snowflake connection
        ctx = snowflake.connector.connect(
            account=db_cred['account'],
            user=db_cred['user'],
            password=db_cred['password'],
            database=snowflake_database,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse,
            role=snowflake_role,
            paramstyle="qmark"
        )

        # snowflake cursor creation
        cur = ctx.cursor()

        # sql to run
        sql = f"""
            SELECT DISTINCT CUSTOMER_PROFILE_ID
            FROM {DB}.{snowflake_schema}."CUSTOMER_DATA"
            WHERE NETWORK_TYPE IN ('linkedin_company')
        """

        # execute the sql
        cur.execute(sql)

        # get all the rows from snowflake
        # ret = cur.fetchmany(500000)
        ret = cur.fetchall()

        # save the returned data as a list of tuples
        snowflake_data = []
        for row in ret:
            snowflake_data.append(row)

        ctx.close()

    except Exception as e:
        print("get_snowflake_data() Raised an exception >>> " + str(e))
        raise e

    # return list
    return snowflake_data


# Get Customer IDs from Snowflake
print("Download Customer IDs from snowflake started")
sf_data = get_snowflake_data()
print("Download from snowflake completed!!!")
print(f"Customer IDs received from Snowflake >>> {len(sf_data)}")

# Required column list
col_list = [
  'created_time',
  'customer_profile_id',
  'post_type',
  'post_type',
  'network',
  'clickthrough_link.long',
  'clickthrough_link.short',
  'hashtags',
  'metrics.lifetime.post_content_clicks',
  'metrics.lifetime.reactions',
  'metrics.lifetime.shares_count',
  'metrics.lifetime.comments_count',
  'metrics.lifetime.video_views',
  'metrics.lifetime.impressions',
  'metrics.lifetime.likes',
  'internal.tags',
  'perma_link'
]



for cust_id in sf_data:
  profile_customer_id = cust_id[0]

  # JSON Payload for Customer Profile
  payload = {
    "fields": [
      "content_category",
      "created_time",
      "from.guid",
      "from.name",
      "from.profile_picture",
      "from.screen_name",
      "guid",
      "customer_profile_id",
      "internal.tags.id",
      "internal.sent_by.id",
      "internal.sent_by.email",
      "internal.sent_by.first_name",
      "internal.sent_by.last_name",
      "post_type",
      "post_category",
      "network",
      "perma_link",
      "profile_guid",
      "text",
      "title",
      "clickthrough_link.name",
      "clickthrough_link.long",
      "clickthrough_link.short",
      "hashtags",
      "visual_media"
    ],
    "filters": [
      "customer_profile_id.eq("+str(profile_customer_id)+")",
      "created_time.in("+str(start_date)+".."+str(end_date)+")"
    ],
    "metrics": [
      "lifetime.impressions",
      "lifetime.reactions",
      "lifetime.comments_count",
      "lifetime.shares_count",
      "lifetime.post_content_clicks",
      "lifetime.video_views",
      "lifetime.vote_count"
    ]
  }
  
  
  json_payload = json.dumps(payload)
  
  
  hed = {'Authorization': 'Bearer ' + auth_token, 'Content-Type': 'application/json', 'Accept': '*/*'}
  response = requests.post(url, headers=hed, data=json_payload)
  print(response)
  
  post_data=response.json()
  
  if len(post_data['data']) > 0:
    if type(post_data['data'][-1]) == list:
      post_data = json_normalize(post_data['data'][:-1])
    else:
      post_data = json_normalize(post_data['data'])
    
    df = pd.DataFrame(post_data)
    # df['ingestion_timestamp'] = datetime.today()
    # post_df = post_df.append(df) #append is deprecated
    post_df = pd.concat([post_df, df], ignore_index=True)

  
# replace empty list coming from the json to NaN
post_df = post_df.mask(post_df.applymap(str).eq('[]'))

# Insert ingestion time
post_df['ingestion_timestamp'] = datetime.today()

# Include the missing column into data frame
for i in col_list:
    if i not in post_df.columns:
        post_df[i] = ''


post_df.rename(columns={
  'clickthrough_link.long': 'long_url',
  'clickthrough_link.short': 'short_url',
  'metrics.lifetime.post_content_clicks': 'post_content_clicks',
  'metrics.lifetime.reactions': 'reactions',
  'metrics.lifetime.shares_count': 'shares_count',
  'metrics.lifetime.comments_count': 'comments_count',
  'metrics.lifetime.video_views': 'video_views',
  'metrics.lifetime.impressions': 'impressions',
  'metrics.lifetime.likes': 'likes',
  'internal.tags': 'tag_id',
  'perma_link': 'post_url'
}, inplace=True)


post_df = post_df[[
  'ingestion_timestamp',
  'created_time',
  'customer_profile_id',
  'post_type',
  'network',
  'long_url',
  'short_url',
  'hashtags',
  'post_content_clicks',
  'reactions',
  'shares_count',
  'comments_count',
  'video_views',
  'impressions',
  'likes',
  'tag_id',
  'post_url'
]]

post_df['tag_id'] = post_df['tag_id'].apply(
    lambda x: [item['id'] for item in x] if isinstance(x, list) else x
)

s3_client = boto3.client("s3",
                         aws_access_key_id=aws_access_key,
                         aws_secret_access_key=aws_secret_key,
                         region_name=region_name)
response = s3_client.get_object(Bucket=tags_bucket, Key=tags_key)
csv_content = response['Body'].read()
df_tags = pd.read_csv(io.BytesIO(csv_content))

result = dict(zip(df_tags['tag_id'], df_tags['text']))

post_df['tags'] = post_df['tag_id'].apply(
    lambda ids: [result.get(i) for i in ids if i in result] if isinstance(ids, list) else []
)


# Store CSV to S3 bucket
csv_buffer = StringIO()
post_df.to_csv(csv_buffer, index=False)
output_file_path = key
s3client = aws_session.client("s3")
response = s3client.put_object(
    Bucket=bucket,
    Key=output_file_path,
  	Body=csv_buffer.getvalue()
)

print("LinkedIn Data extraction completed...")



def sf_load():
    
# doing the truncate and load of the target table in SF
    with sf_engine.begin() as connection:
        
        #truncating the required table
        print(f'\n:::Truncating "{DB}"."{snowflake_schema}"."{TARGET_TABLE}"...::: ')
        truncate_query = connection.execute(f'''TRUNCATE TABLE IF EXISTS {DB}.{snowflake_schema}.{TARGET_TABLE}''')
        print(f':::Truncating "{DB}"."{snowflake_schema}"."{TARGET_TABLE}", {truncate_query.fetchone()[0]}:::')
        
        
        #Loading the data into SF
        copy_into_query = f'''
        COPY INTO "{DB}"."{snowflake_schema}"."{TARGET_TABLE}"("INGESTION_TIMESTAMP", "CREATED_TIME", "CUSTOMER_PROFILE_ID", "POST_TYPE", "NETWORK", "LONG_URL", "SHORT_URL", "HASHTAGS", "POST_CONTENT_CLICKS", "REACTIONS", "SHARES_COUNT", "COMMENTS_COUNT", "VIDEO_VIEWS", "IMPRESSIONS", "LIKES", "TAG_ID", "POST_URL", "TAGS")
        FROM 
                's3://{bucket}/'
                CREDENTIALS = (AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
                PATTERN='{key}'
                FILE_FORMAT= (
                    TYPE=CSV
                    COMPRESSION=AUTO,
                    RECORD_DELIMITER='\\n',
                    FIELD_DELIMITER=',',
                    SKIP_HEADER=1,
                    SKIP_BLANK_LINES=FALSE,
                    ESCAPE='\\\\',
                    ESCAPE_UNENCLOSED_FIELD='\\\\',
                    TRIM_SPACE=FALSE,
                    FIELD_OPTIONALLY_ENCLOSED_BY='"',
                    ERROR_ON_COLUMN_COUNT_MISMATCH=TRUE,
                    REPLACE_INVALID_CHARACTERS=FALSE,
                    EMPTY_FIELD_AS_NULL=TRUE,
                    ENCODING='UTF8'
                )
                ON_ERROR='ABORT_STATEMENT'
                PURGE=FALSE
                TRUNCATECOLUMNS=FALSE
                FORCE=FALSE
        '''
        
        # print(copy_into_query)
        
        print(f'\n:::Loading "{DB}"."{snowflake_schema}"."{TARGET_TABLE}"...::: ')

        # LOAD COMMAND:
        copy_into_query_result = connection.execute(copy_into_query).fetchall() 
        # print(copy_into_query_result)
            
        print(f':::Successfully Loaded "{DB}"."{snowflake_schema}"."{TARGET_TABLE}":::')
        
        
        #result set if CopyInto doesnt load any row
        if copy_into_query_result[0][0] == 'Copy executed with 0 files processed.':
            print('Load Details:-\n'\
                  ,f"0 Rows were processed!!"\
                  ,f"Either due to NO ROWS available to upload or some failure in Copy Into command"
                )
        #result set if CopyInto does load the row(s)
        else:      
            print('Load Details:-\n'\
                ,f"Status:{copy_into_query_result[0][1]}"\
                ,f"rows_parsed:{copy_into_query_result[0][2]}"\
                ,f"rows_loaded:{copy_into_query_result[0][3]}"\
                ,f"errors_seen:{copy_into_query_result[0][5]}")
        
    return '\n:::END OF Customer POST Data For Linkedin Extract Load script:::'   



#loading data to snowflake using copyinto
print(sf_load())


####END OF CODE####
