'''
This python script picks and loads Customer Users data from SPROUT API creates a equivalent csv file and puts it to S3
and truncate loads the SF table from it.
'''

import requests
import boto3
import pandas as pd
import numpy as np
import os
import json
from io import StringIO
from sqlalchemy import text
from datetime import datetime
from pandas import json_normalize
from dags.sprout_api_ingestion.python_scripts._helper_scripts.customer_id_api import get_customer_id_and_date
from dags.sprout_api_ingestion.python_scripts._helper_scripts.connections import making_aws_connection
from dags.sprout_api_ingestion.python_scripts._helper_scripts.connections import making_sf_connection





def customer_users_data_extract_load():
    
    #getting aws connection
    aws_connection_values = making_aws_connection()
    aws_connection = aws_connection_values[0]
    aws_access_key = aws_connection_values[1]
    aws_secret_key = aws_connection_values[2]
    
    #getting sf engine
    sf_engine = making_sf_connection()
    
    #sf required vars    
    #Dynamically setting the SF database
    if os.environ['ENVIRONMENT'] == 'prd':
        DB =  'PROD'
    else:
        DB =  'DEV'
    
    SCHEMA =  'SPROUT'
    TARGET_TABLE = 'CUSTOMER_USER_DATA'

    
    #extracting customer_id and date required
    get_customer_id_and_date_values = get_customer_id_and_date()
    customer_id = str(get_customer_id_and_date_values['customer_id'])
    auth_token = get_customer_id_and_date_values['auth_token']
    date = get_customer_id_and_date_values['date']
    print("\nPipeline run date for Sprout Customer data: ", date)

    
    #s3 bucket details
    bucket = 'com.tata.marketing.mars.prd.ue1.matillion-data'
    
    #Dynamically setting the S3 key or prefix
    if os.environ['ENVIRONMENT'] == 'prd':
        key =  'sprout/dt='+date+'/customer_users_data.csv'
    else:
        key =  'test/dt='+date+'/customer_users_data.csv'
    
    
    #url details
    url = 'https://api.sproutsocial.com/v1/'+customer_id+'/metadata/customer/users'

    
    
    
    
    
    
    print("Customer Users Data extraction started...")
    #extracting the file
    hed = {'Authorization': 'Bearer ' + auth_token}
    response = requests.get(url, headers=hed)
    user_data=response.json()
    
    
    
    #dataframe transformations
    if type(user_data['data'][-1]) == list:
        user_data = json_normalize(user_data['data'][:-1])
    else:
        user_data = json_normalize(user_data['data'])
    # print(customer_data)

    user_df = pd.DataFrame(user_data)

    # replace empty list coming from the json to NaN
    user_df = user_df.mask(user_df.applymap(str).eq('[]'))

    # remove duplicates
    user_df = user_df.loc[user_df.astype(str).drop_duplicates().index]

    user_df['ingestion_timestamp'] = datetime.today()





    #uploading dataframe to S3
    # Store CSV to S3 bucket
    csv_buffer = StringIO()
    user_df.to_csv(csv_buffer, index=False)
    output_file_path = key
    
    #uploading to s3 bucket
    s3client = aws_connection.client("s3")
    response = s3client.put_object(
        Bucket=bucket,
        Key=output_file_path,
        Body=csv_buffer.getvalue()
    )
    print(f":::Customer User Data extraction completed and uploaded to S3 location: {bucket}/{key}...:::")
    
    
    
    # doing the truncate and load of the target table in SF
    with sf_engine.begin() as connection:
        
        #truncating the required table
        print(f'\n:::Truncating "{DB}"."{SCHEMA}"."{TARGET_TABLE}"...::: ')
        truncate_query = connection.execute(text(f'''TRUNCATE TABLE IF EXISTS {DB}.{SCHEMA}.{TARGET_TABLE}'''))
        print(f':::Truncating "{DB}"."{SCHEMA}"."{TARGET_TABLE}", {truncate_query.fetchone()[0]}:::')
        
        
        #Loading the data into SF
        copy_into_query = f'''
        COPY INTO "{DB}"."{SCHEMA}"."{TARGET_TABLE}"("ID", "NAME", "INGESTION_TIMESTAMP") FROM 
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
        
        print(f'\n:::Loading "{DB}"."{SCHEMA}"."{TARGET_TABLE}"...::: ')

        # LOAD COMMAND:
        copy_into_query_result = connection.execute(text(copy_into_query)).fetchall() 
            
        print(f':::Successfully Loaded "{DB}"."{SCHEMA}"."{TARGET_TABLE}":::')
        
        
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
        
    return '\n:::END OF Customer TAG Data Api Exctract Load script:::'
    



# print(customer_users_data_extract_load())