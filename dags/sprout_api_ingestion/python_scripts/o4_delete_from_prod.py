'''
This python script Deletes from Prod in Prod env from required tables for current_date-1 filter
'''


import os
from dags.sprout_api_ingestion.python_scripts._helper_scripts.connections import making_sf_connection



def req_details():
    #getting sf engine
    sf_engine = making_sf_connection()
    
    return {"sf_engine":sf_engine}
    
    
# extracting req_details
req_details_values  = req_details()
sf_engine = req_details_values["sf_engine"]


#Dynamically setting the SF database
if os.environ['ENVIRONMENT'] == 'prd':
    DB =  'PROD' 
    SCHEMA = 'SPROUT'
else:
    DB =  'DEV'
    SCHEMA = 'SPROUT2'


#########################
# code from matillion job
#########################

delete_tables ={
    "t1": {"table_name": "TWITTER_PROFILE_DATA", "filter_col": "REPORTING_PERIOD_DAY"},
    "t2": {"table_name": "FACEBOOK_PROFILE_DATA", "filter_col": "REPORTING_PERIOD_DAY"},
    "t3": {"table_name": "LINKEDIN_PROFILE_DATA", "filter_col": "REPORTING_PERIOD_DAY"},
    "t4": {"table_name": "YOUTUBE_PROFILE_DATA", "filter_col": "REPORTING_PERIOD_DAY"},
    "t5": {"table_name": "INSTAGRAM_PROFILE_DATA", "filter_col": "REPORTING_PERIOD_DAY"},
    "t6": {"table_name": "TIKTOK_PROFILE_DATA", "filter_col": "REPORTING_PERIOD_DAY"},
    
    "t7": {"table_name": "TWITTER_POST_DATA", "filter_col": "CREATED_TIME"},
    "t8": {"table_name": "FACEBOOK_POST_DATA", "filter_col": "CREATED_TIME"},
    "t9": {"table_name": "LINKEDIN_POST_DATA", "filter_col": "CREATED_TIME"},
    "t10": {"table_name": "YOUTUBE_POST_DATA", "filter_col": "CREATED_TIME"},
    "t11": {"table_name": "INSTAGRAM_POST_DATA", "filter_col": "CREATED_TIME"},
    "t12": {"table_name": "TIKTOK_POST_DATA", "filter_col": "CREATED_TIME"},
}


with sf_engine.begin() as connection:    
    
    for table, info in delete_tables.items():
        tb = info['table_name']
        fl = info['filter_col']
        
        print(f'\n:::Deleting data from "{DB}"."{SCHEMA}"."{tb}"...::: ')
        delete_query = connection.execute(f'''DELETE FROM {DB}.{SCHEMA}.{tb} WHERE {fl}::DATE = CURRENT_DATE()-1 ''')
        print(f':::Deleting data from "{DB}"."{SCHEMA}"."{tb}", Deleted {delete_query.fetchall()[0][0]} Records:::')
    
    print('\nDELETED DATA FROM ALL REQUIRED TABLES')
    print("::::END OF DELTE_FROM_PROD SCRIPT::::\n")


#####END OF CODE#####