'''
This python script picks and loads Facebook Profile Data from SPROUT API creates a equivalent csv file and puts it to S3
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
if os.environ['ENVIRONMENT'] == 'prd':
    DB =  'RAW'
else:
    DB =  'DEV'

snowflake_database = DB
snowflake_schema = 'SPROUT'
snowflake_warehouse = 'BSM_QUERY'
snowflake_role = 'BSM_DEVELOPER'
TARGET_TABLE = 'FACEBOOK_PROFILE_DATA'

bucket = 'com.tata.marketing.mars.prd.ue1.matillion-data'
#Dynamically setting the S3 key or prefix
if os.environ['ENVIRONMENT'] == 'prd':
    key =  'sprout/dt='+date+'/facebook_profile_data.csv'
else:
    key =  'test/dt='+date+'/facebook_profile_data.csv'
    


# start_date = '2024-01-01'
# end_date = '2024-09-08'
start_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
end_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')

print("Facebook Data extraction started...")
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
            WHERE NETWORK_TYPE IN ('facebook')
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
	'metrics.followers_gained',
	'metrics.followers_gained_organic',
	'metrics.followers_gained_paid',
	'metrics.followers_lost',
	'metrics.lifetime_snapshot.fans_count',
	'metrics.fans_gained',
	'metrics.fans_gained_organic',
	'metrics.fans_gained_paid',
	'metrics.fans_lost',
	'metrics.impressions',
	'metrics.impressions_organic',
	'metrics.impressions_viral',
	'metrics.impressions_nonviral',
	'metrics.impressions_paid',
	'metrics.tab_views',
	'metrics.tab_views_login',
	'metrics.tab_views_logout',
	'metrics.post_impressions',
	'metrics.post_impressions_organic',
	'metrics.post_impressions_viral',
	'metrics.post_impressions_nonviral',
	'metrics.post_impressions_paid',
	'metrics.impressions_unique',
	'metrics.impressions_organic_unique',
	'metrics.impressions_viral_unique',
	'metrics.impressions_nonviral_unique',
	'metrics.impressions_paid_unique',
	'metrics.profile_views',
	'metrics.reactions',
	'metrics.comments_count',
	'metrics.shares_count',
	'metrics.post_link_clicks',
	'metrics.post_content_clicks_other',
	'metrics.post_photo_view_clicks',
	'metrics.post_video_play_clicks',
	'metrics.profile_actions',
	'metrics.post_engagements',
	'metrics.cta_clicks_login',
	'metrics.place_checkins',
	'metrics.negative_feedback',
	'metrics.video_views',
	'metrics.video_views_organic',
	'metrics.video_views_paid',
	'metrics.video_views_autoplay',
	'metrics.video_views_click_to_play',
	'metrics.video_views_repeat',
	'metrics.video_view_time',
	'metrics.video_views_unique',
	'metrics.video_views_30s_complete',
	'metrics.video_views_30s_complete_organic',
	'metrics.video_views_30s_complete_paid',
	'metrics.video_views_30s_complete_autoplay',
	'metrics.video_views_30s_complete_click_to_play',
	'metrics.video_views_30s_complete_repeat',
	'metrics.video_views_30s_complete_unique',
	'metrics.video_views_partial',
	'metrics.video_views_partial_organic',
	'metrics.video_views_partial_paid',
	'metrics.video_views_partial_autoplay',
	'metrics.video_views_partial_click_to_play',
	'metrics.video_views_partial_repeat',
	'metrics.video_views_10s',
	'metrics.video_views_10s_organic',
	'metrics.video_views_10s_paid',
	'metrics.video_views_10s_autoplay',
	'metrics.video_views_10s_click_to_play',
	'metrics.video_views_10s_repeat',
	'metrics.video_views_10s_unique',
	'metrics.posts_sent_count'
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
      "followers_gained",
      "followers_gained_organic",
      "followers_gained_paid",
      "followers_lost",
      "lifetime_snapshot.fans_count",
      "fans_gained",
      "fans_gained_organic",
      "fans_gained_paid",
      "fans_lost",
      "impressions",
      "impressions_organic",
      "impressions_viral",
      "impressions_nonviral",
      "impressions_paid",
      "tab_views",
      "tab_views_login",
      "tab_views_logout",
      "post_impressions",
      "post_impressions_organic",
      "post_impressions_viral",
      "post_impressions_nonviral",
      "post_impressions_paid",
      "impressions_unique",
      "impressions_organic_unique",
      "impressions_viral_unique",
      "impressions_nonviral_unique",
      "impressions_paid_unique",
      "profile_views",
      "reactions",
      "comments_count",
      "shares_count",
      "post_link_clicks",
      "post_content_clicks_other",
      "post_photo_view_clicks",
      "post_video_play_clicks",
      "profile_actions",
      "post_engagements",
      "cta_clicks_login",
      "place_checkins",
      "negative_feedback",
      "video_views",
      "video_views_organic",
      "video_views_paid",
      "video_views_autoplay",
      "video_views_click_to_play",
      "video_views_repeat",
      "video_view_time",
      "video_views_unique",
      "video_views_30s_complete",
      "video_views_30s_complete_organic",
      "video_views_30s_complete_paid",
      "video_views_30s_complete_autoplay",
      "video_views_30s_complete_click_to_play",
      "video_views_30s_complete_repeat",
      "video_views_30s_complete_unique",
      "video_views_partial",
      "video_views_partial_organic",
      "video_views_partial_paid",
      "video_views_partial_autoplay",
      "video_views_partial_click_to_play",
      "video_views_partial_repeat",
      "video_views_10s",
      "video_views_10s_organic",
      "video_views_10s_paid",
      "video_views_10s_autoplay",
      "video_views_10s_click_to_play",
      "video_views_10s_repeat",
      "video_views_10s_unique",
      "posts_sent_count"
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
    # profile_df = profile_df.append(df) #append is deprecated
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
  'metrics.lifetime_snapshot.followers_count': 'lifetime_snapshot_followers_count',
  'metrics.net_follower_growth': 'net_follower_growth',
  'metrics.followers_gained': 'followers_gained',
  'metrics.followers_gained_organic': 'followers_gained_organic',
  'metrics.followers_gained_paid': 'followers_gained_paid',
  'metrics.followers_lost': 'followers_lost',
  'metrics.lifetime_snapshot.fans_count': 'lifetime_snapshot_fans_count',
  'metrics.fans_gained': 'fans_gained',
  'metrics.fans_gained_organic': 'fans_gained_organic',
  'metrics.fans_gained_paid': 'fans_gained_paid',
  'metrics.fans_lost': 'fans_lost',
  'metrics.impressions': 'impressions',
  'metrics.impressions_organic': 'impressions_organic',
  'metrics.impressions_viral': 'impressions_viral',
  'metrics.impressions_nonviral': 'impressions_nonviral',
  'metrics.impressions_paid': 'impressions_paid',
  'metrics.tab_views': 'tab_views',
  'metrics.tab_views_login': 'tab_views_login',
  'metrics.tab_views_logout': 'tab_views_logout',
  'metrics.post_impressions': 'post_impressions',
  'metrics.post_impressions_organic': 'post_impressions_organic',
  'metrics.post_impressions_viral': 'post_impressions_viral',
  'metrics.post_impressions_nonviral': 'post_impressions_nonviral',
  'metrics.post_impressions_paid': 'post_impressions_paid',
  'metrics.impressions_unique': 'impressions_unique',
  'metrics.impressions_organic_unique': 'impressions_organic_unique',
  'metrics.impressions_viral_unique': 'impressions_viral_unique',
  'metrics.impressions_nonviral_unique': 'impressions_nonviral_unique',
  'metrics.impressions_paid_unique': 'impressions_paid_unique',
  'metrics.profile_views': 'profile_views',
  'metrics.reactions': 'reactions',
  'metrics.comments_count': 'comments_count',
  'metrics.shares_count': 'shares_count',
  'metrics.post_link_clicks': 'post_link_clicks',
  'metrics.post_content_clicks_other': 'post_content_clicks_other',
  'metrics.post_photo_view_clicks': 'post_photo_view_clicks',
  'metrics.post_video_play_clicks': 'post_video_play_clicks',
  'metrics.profile_actions': 'profile_actions',
  'metrics.post_engagements': 'post_engagements',
  'metrics.cta_clicks_login': 'cta_clicks_login',
  'metrics.place_checkins': 'place_checkins',
  'metrics.negative_feedback': 'negative_feedback',
  'metrics.video_views': 'video_views',
  'metrics.video_views_organic': 'video_views_organic',
  'metrics.video_views_paid': 'video_views_paid',
  'metrics.video_views_autoplay': 'video_views_autoplay',
  'metrics.video_views_click_to_play': 'video_views_click_to_play',
  'metrics.video_views_repeat': 'video_views_repeat',
  'metrics.video_view_time': 'video_view_time',
  'metrics.video_views_unique': 'video_views_unique',
  'metrics.video_views_30s_complete': 'video_views_30s_complete',
  'metrics.video_views_30s_complete_organic': 'video_views_30s_complete_organic',
  'metrics.video_views_30s_complete_paid': 'video_views_30s_complete_paid',
  'metrics.video_views_30s_complete_autoplay': 'video_views_30s_complete_autoplay',
  'metrics.video_views_30s_complete_click_to_play': 'video_views_30s_complete_click_to_play',
  'metrics.video_views_30s_complete_repeat': 'video_views_30s_complete_repeat',
  'metrics.video_views_30s_complete_unique': 'video_views_30s_complete_unique',
  'metrics.video_views_partial': 'video_views_partial',
  'metrics.video_views_partial_organic': 'video_views_partial_organic',
  'metrics.video_views_partial_paid': 'video_views_partial_paid',
  'metrics.video_views_partial_autoplay': 'video_views_partial_autoplay',
  'metrics.video_views_partial_click_to_play': 'video_views_partial_click_to_play',
  'metrics.video_views_partial_repeat': 'video_views_partial_repeat',
  'metrics.video_views_10s': 'video_views_10s',
  'metrics.video_views_10s_organic': 'video_views_10s_organic',
  'metrics.video_views_10s_paid': 'video_views_10s_paid',
  'metrics.video_views_10s_autoplay': 'video_views_10s_autoplay',
  'metrics.video_views_10s_click_to_play': 'video_views_10s_click_to_play',
  'metrics.video_views_10s_repeat': 'video_views_10s_repeat',
  'metrics.video_views_10s_unique': 'video_views_10s_unique',
  'metrics.posts_sent_count': 'posts_sent_count'
}, inplace=True)


profile_df = profile_df[[
  'customer_profile_id',
  'reporting_period_day',
  'ingestion_timestamp',
  'lifetime_snapshot_followers_count',
  'net_follower_growth',
  'followers_gained',
  'followers_gained_organic',
  'followers_gained_paid',
  'followers_lost',
  'lifetime_snapshot_fans_count',
  'fans_gained',
  'fans_gained_organic',
  'fans_gained_paid',
  'fans_lost',
  'impressions',
  'impressions_organic',
  'impressions_viral',
  'impressions_nonviral',
  'impressions_paid',
  'tab_views',
  'tab_views_login',
  'tab_views_logout',
  'post_impressions',
  'post_impressions_organic',
  'post_impressions_viral',
  'post_impressions_nonviral',
  'post_impressions_paid',
  'impressions_unique',
  'impressions_organic_unique',
  'impressions_viral_unique',
  'impressions_nonviral_unique',
  'impressions_paid_unique',
  'profile_views',
  'reactions',
  'comments_count',
  'shares_count',
  'post_link_clicks',
  'post_content_clicks_other',
  'post_photo_view_clicks',
  'post_video_play_clicks',
  'profile_actions',
  'post_engagements',
  'cta_clicks_login',
  'place_checkins',
  'negative_feedback',
  'video_views',
  'video_views_organic',
  'video_views_paid',
  'video_views_autoplay',
  'video_views_click_to_play',
  'video_views_repeat',
  'video_view_time',
  'video_views_unique',
  'video_views_30s_complete',
  'video_views_30s_complete_organic',
  'video_views_30s_complete_paid',
  'video_views_30s_complete_autoplay',
  'video_views_30s_complete_click_to_play',
  'video_views_30s_complete_repeat',
  'video_views_30s_complete_unique',
  'video_views_partial',
  'video_views_partial_organic',
  'video_views_partial_paid',
  'video_views_partial_autoplay',
  'video_views_partial_click_to_play',
  'video_views_partial_repeat',
  'video_views_10s',
  'video_views_10s_organic',
  'video_views_10s_paid',
  'video_views_10s_autoplay',
  'video_views_10s_click_to_play',
  'video_views_10s_repeat',
  'video_views_10s_unique',
  'posts_sent_count'
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

print("Facebook Data extraction completed...")





def sf_load():
    
# doing the truncate and load of the target table in SF
    with sf_engine.begin() as connection:
        
        #truncating the required table
        print(f'\n:::Truncating "{DB}"."{snowflake_schema}"."{TARGET_TABLE}"...::: ')
        truncate_query = connection.execute(f'''TRUNCATE TABLE IF EXISTS {DB}.{snowflake_schema}.{TARGET_TABLE}''')
        print(f':::Truncating "{DB}"."{snowflake_schema}"."{TARGET_TABLE}", {truncate_query.fetchone()[0]}:::')
        
        
        #Loading the data into SF
        copy_into_query = f'''
        COPY INTO "{DB}"."{snowflake_schema}"."{TARGET_TABLE}"("CUSTOMER_PROFILE_ID", "REPORTING_PERIOD_DAY", "INGESTION_TIMESTAMP", "LIFETIME_SNAPSHOT_FOLLOWERS_COUNT", "NET_FOLLOWER_GROWTH", "FOLLOWERS_GAINED", "FOLLOWERS_GAINED_ORGANIC", "FOLLOWERS_GAINED_PAID", "FOLLOWERS_LOST", "LIFETIME_SNAPSHOT_FANS_COUNT", "FANS_GAINED", "FANS_GAINED_ORGANIC", "FANS_GAINED_PAID", "FANS_LOST", "IMPRESSIONS", "IMPRESSIONS_ORGANIC", "IMPRESSIONS_VIRAL", "IMPRESSIONS_NONVIRAL", "IMPRESSIONS_PAID", "TAB_VIEWS", "TAB_VIEWS_LOGIN", "TAB_VIEWS_LOGOUT", "POST_IMPRESSIONS", "POST_IMPRESSIONS_ORGANIC", "POST_IMPRESSIONS_VIRAL", "POST_IMPRESSIONS_NONVIRAL", "POST_IMPRESSIONS_PAID", "IMPRESSIONS_UNIQUE", "IMPRESSIONS_ORGANIC_UNIQUE", "IMPRESSIONS_VIRAL_UNIQUE", "IMPRESSIONS_NONVIRAL_UNIQUE", "IMPRESSIONS_PAID_UNIQUE", "PROFILE_VIEWS", "REACTIONS", "COMMENTS_COUNT", "SHARES_COUNT", "POST_LINK_CLICKS", "POST_CONTENT_CLICKS_OTHER", "POST_PHOTO_VIEW_CLICKS", "POST_VIDEO_PLAY_CLICKS", "PROFILE_ACTIONS", "POST_ENGAGEMENTS", "CTA_CLICKS_LOGIN", "PLACE_CHECKINS", "NEGATIVE_FEEDBACK", "VIDEO_VIEWS", "VIDEO_VIEWS_ORGANIC", "VIDEO_VIEWS_PAID", "VIDEO_VIEWS_AUTOPLAY", "VIDEO_VIEWS_CLICK_TO_PLAY", "VIDEO_VIEWS_REPEAT", "VIDEO_VIEW_TIME", "VIDEO_VIEWS_UNIQUE", "VIDEO_VIEWS_30S_COMPLETE", "VIDEO_VIEWS_30S_COMPLETE_ORGANIC", "VIDEO_VIEWS_30S_COMPLETE_PAID", "VIDEO_VIEWS_30S_COMPLETE_AUTOPLAY", "VIDEO_VIEWS_30S_COMPLETE_CLICK_TO_PLAY", "VIDEO_VIEWS_30S_COMPLETE_REPEAT", "VIDEO_VIEWS_30S_COMPLETE_UNIQUE", "VIDEO_VIEWS_PARTIAL", "VIDEO_VIEWS_PARTIAL_ORGANIC", "VIDEO_VIEWS_PARTIAL_PAID", "VIDEO_VIEWS_PARTIAL_AUTOPLAY", "VIDEO_VIEWS_PARTIAL_CLICK_TO_PLAY", "VIDEO_VIEWS_PARTIAL_REPEAT", "VIDEO_VIEWS_10S", "VIDEO_VIEWS_10S_ORGANIC", "VIDEO_VIEWS_10S_PAID", "VIDEO_VIEWS_10S_AUTOPLAY", "VIDEO_VIEWS_10S_CLICK_TO_PLAY", "VIDEO_VIEWS_10S_REPEAT", "VIDEO_VIEWS_10S_UNIQUE", "POSTS_SENT_COUNT")
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
        
    return '\n:::END OF Customer Profile Data For FACEBOOK Extract Load script:::'   



#loading Facebook data to snowflake using copyinto
print(sf_load())


####END OF CODE####