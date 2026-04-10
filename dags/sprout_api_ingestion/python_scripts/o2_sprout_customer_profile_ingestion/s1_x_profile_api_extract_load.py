'''
This python script picks and loads X/Twitter Profile Data from SPROUT API creates a equivalent csv file and puts it to S3
and truncate loads the SF table from it.
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


# Vars
#Dynamically setting the SF database
if os.environ['ENVIRONMENT'] == 'prd':
    DB =  'PROD' 
else:
    DB =  'DEV'
    
snowflake_database = DB
snowflake_schema = 'SPROUT'
snowflake_warehouse = 'BSM_QUERY'
snowflake_role = 'BSM_DEVELOPER'
TARGET_TABLE = 'TWITTER_PROFILE_DATA' 

bucket = 'com.tredence_analytics.marketing.mars.prd.ue1.matillion-data'
#Dynamically setting the S3 key or prefix
if os.environ['ENVIRONMENT'] == 'prd':
    key =  'sprout/dt='+date+'/x_profile_data.csv' 
else:
    key =  'test/dt='+date+'/x_profile_data.csv'  
    
# start_date = '2024-01-01'
# end_date = '2024-09-08'
start_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
end_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')


print("Twitter Data extraction started...")
url = 'https://api.sproutsocial.com/v1/'+str(customer_id)+'/analytics/profiles'

profile_df = pd.DataFrame()


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
            WHERE NETWORK_TYPE IN ('twitter')
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
      'metrics.lifetime_snapshot.followers_count',
      'metrics.net_follower_growth',
      'metrics.impressions',
      'metrics.post_media_views',
      'metrics.video_views',
      'metrics.reactions',
      'metrics.likes',
      'metrics.comments_count',
      'metrics.shares_count',
      'metrics.post_content_clicks',
      'metrics.post_link_clicks',
      'metrics.post_content_clicks_other',
      'metrics.post_media_clicks',
      'metrics.post_hashtag_clicks',
      'metrics.post_detail_expand_clicks',
      'metrics.post_profile_clicks',
      'metrics.engagements_other',
      'metrics.post_app_engagements',
      'metrics.post_app_installs',
      'metrics.post_app_opens',
      'metrics.posts_sent_count',
      'metrics.posts_sent_by_post_type',
      'metrics.posts_sent_by_content_type'
    ]


for cust_id in sf_data:
  profile_customer_id = cust_id[0]

  # JSON Payload for Customer Profile
  payload = {
    "filters": [
      "customer_profile_id.eq("+str(profile_customer_id)+")",
      "reporting_period.in("+str(start_date)+".."+str(end_date)+")"
    ],
    "metrics": [
      "lifetime_snapshot.followers_count",
      "net_follower_growth",
      "impressions",
      "post_media_views",
      "video_views",
      "reactions",
      "likes",
      "comments_count",
      "shares_count",
      "post_content_clicks",
      "post_link_clicks",
      "post_content_clicks_other",
      "post_media_clicks",
      "post_hashtag_clicks",
      "post_detail_expand_clicks",
      "post_profile_clicks",
      "engagements_other",
      "post_app_engagements",
      "post_app_installs",
      "post_app_opens",
      "posts_sent_count",
      "posts_sent_by_post_type",
      "posts_sent_by_content_type"
    ]
  }
  
  
  json_payload = json.dumps(payload)
  
  
  hed = {'Authorization': 'Bearer ' + str(auth_token), 'Content-Type': 'application/json', 'Accept': '*/*'}
  response = requests.post(url, headers=hed, data=json_payload)
  print(response)
  
  profile_data=response.json()
  print(profile_data)
  
  if len(profile_data['data']) > 0:
    if type(profile_data['data'][-1]) == list:
      profile_data = json_normalize(profile_data['data'][:-1])
    else:
      profile_data = json_normalize(profile_data['data'])
  
    df = pd.DataFrame(profile_data)
    df['ingestion_timestamp'] = datetime.today()
    # profile_df = profile_df.append(df)    #changed append to concat
    profile_df = pd.concat([profile_df, df], ignore_index=True)

  
# replace empty list coming from the json to NaN
profile_df = profile_df.mask(profile_df.applymap(str).eq('[]'))

# remove duplicates
profile_df.drop_duplicates()

# Include the missing column into data frame
for i in col_list:
    if i not in profile_df.columns:
        profile_df[i] = 0


profile_df.rename(columns={
  'dimensions.customer_profile_id': 'customer_profile_id',
  'dimensions.reporting_period.by(day)': 'reporting_period_day',
  'metrics.lifetime_snapshot.followers_count': 'followers_count',
  'metrics.net_follower_growth': 'net_follower_growth',
  'metrics.impressions': 'impressions',
  'metrics.post_media_views': 'post_media_views',
  'metrics.video_views': 'video_views',
  'metrics.reactions': 'reactions',
  'metrics.likes': 'likes',
  'metrics.comments_count': 'comments_count',
  'metrics.shares_count': 'shares_count',
  'metrics.post_content_clicks': 'post_content_clicks',
  'metrics.post_link_clicks': 'post_link_clicks',
  'metrics.post_content_clicks_other': 'post_content_clicks_other',
  'metrics.post_media_clicks': 'post_media_clicks',
  'metrics.post_hashtag_clicks': 'post_hashtag_clicks',
  'metrics.post_detail_expand_clicks': 'post_detail_expand_clicks',
  'metrics.post_profile_clicks': 'post_profile_clicks',
  'metrics.engagements_other': 'engagements_other',
  'metrics.post_app_engagements': 'post_app_engagements',
  'metrics.post_app_installs': 'post_app_installs',
  'metrics.post_app_opens': 'post_app_opens',
  'metrics.posts_sent_count': 'posts_sent_count',
  'metrics.posts_sent_by_post_type': 'posts_sent_by_post_type',
  'metrics.posts_sent_by_content_type': 'posts_sent_by_content_type'
}, inplace=True)


profile_df = profile_df[[
  'customer_profile_id',
  'reporting_period_day',
  'ingestion_timestamp',
  'followers_count',
  'net_follower_growth',
  'impressions',
  'post_media_views',
  'video_views',
  'reactions',
  'likes',
  'comments_count',
  'shares_count',
  'post_content_clicks',
  'post_link_clicks',
  'post_content_clicks_other',
  'post_media_clicks',
  'post_hashtag_clicks',
  'post_detail_expand_clicks',
  'post_profile_clicks',
  'engagements_other',
  'post_app_engagements',
  'post_app_installs',
  'post_app_opens',
  'posts_sent_count',
  'posts_sent_by_post_type',
  'posts_sent_by_content_type'
]]
  
  
# Store CSV to S3 bucket
csv_buffer = StringIO()
profile_df.to_csv(csv_buffer, index=False)
output_file_path = key 
s3client = aws_session.client("s3")
response = s3client.put_object(
    Bucket=bucket,
    Key=output_file_path,
  	Body=csv_buffer.getvalue()
)

print("Twitter Data extraction completed...")


def sf_load():
    
# doing the truncate and load of the target table in SF
    with sf_engine.begin() as connection:
        
        #truncating the required table
        print(f'\n:::Truncating "{DB}"."{snowflake_schema}"."{TARGET_TABLE}"...::: ')
        truncate_query = connection.execute(f'''TRUNCATE TABLE IF EXISTS {DB}.{snowflake_schema}.{TARGET_TABLE}''')
        print(f':::Truncating "{DB}"."{snowflake_schema}"."{TARGET_TABLE}", {truncate_query.fetchone()[0]}:::')
        
        
        #Loading the data into SF
        copy_into_query = f'''
        COPY INTO "{DB}"."{snowflake_schema}"."{TARGET_TABLE}"("CUSTOMER_PROFILE_ID", "REPORTING_PERIOD_DAY", "INGESTION_TIMESTAMP", "FOLLOWERS_COUNT", "NET_FOLLOWER_GROWTH", "IMPRESSIONS", "POST_MEDIA_VIEWS", "VIDEO_VIEWS", "REACTIONS", "LIKES", "COMMENTS_COUNT", "SHARES_COUNT", "POST_CONTENT_CLICKS", "POST_LINK_CLICKS", "POST_CONTENT_CLICKS_OTHER", "POST_MEDIA_CLICKS", "POST_HASHTAG_CLICKS", "POST_DETAIL_EXPAND_CLICKS", "POST_PROFILE_CLICKS", "ENGAGEMENTS_OTHER", "POST_APP_ENGAGEMENTS", "POST_APP_INSTALLS", "POST_APP_OPENS", "POSTS_SENT_COUNT", "POSTS_SENT_BY_POST_TYPE", "POSTS_SENT_BY_CONTENT_TYPE") 
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
        
    return '\n:::END OF Customer Profile Data For X/Twitter Extract Load script:::'    



#loading data to snowflake using copyinto
print(sf_load())


####END OF CODE####
