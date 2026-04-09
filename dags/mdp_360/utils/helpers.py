import snowflake.connector
import os
from dags.common.py.utils.verbose_log import verbose, log
from dags.common.py.utils.utility_functions import get_boto3_session, get_ssm_parameter,snowflake_conn, aws_conn_credentials
from datetime import datetime, timedelta
from airflow.models import Variable
import json
import paramiko
import boto3
import sqlalchemy
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine,text
import shutil
from airflow.hooks.base_hook import BaseHook 
import pytz
from tqdm import tqdm
from multiprocessing import context







ENVIRONMENT = 'dev' if os.environ['ENVIRONMENT'].lower() == 'stg' else os.environ['ENVIRONMENT']
SNOWFLAKE_CONNECTION_ID = os.environ['SNOWFLAKE_CONN']
snowflake_connection = BaseHook.get_connection(SNOWFLAKE_CONNECTION_ID)


################################################################################################################

# SFTP & S3 required Variables
S3_BUCKET_NAME= 'com.tredence_analytics.marketing.mars.prd.ue1.ftpserver-data'
S3_TARGET_PREFIX_PRD = 'mdfuser1/source_files/'
S3_TARGET_PREFIX_DEV = 'test/'
#Dynamically setting the S3 location
S3_TARGET_PREFIX = S3_TARGET_PREFIX_PRD if os.environ['ENVIRONMENT'] == 'prd' else S3_TARGET_PREFIX_DEV

# --- Temp local folder to hold files ---
LOCAL_TEMP_DIR = './mdp_i360_new_sftp_files/'
os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

#Dynamically setting the Database
DB = 'BSM_ETL' if os.environ['ENVIRONMENT'] == 'prd' else 'DEV'
SCHEMA = 'MDF'

aws_access_key, aws_secret_key = aws_conn_credentials()

################################################################################################################





def generate_dbt_command(**kwargs):          # FIX 1: *args removed; use **kwargs only
    dbt_target  = ENVIRONMENT.lower()
    project_dir = '/usr/local/airflow/dbt/mars_dbt/'
    dbt_dir     = '/usr/local/airflow/dbt_venv/bin/dbt'

    # FIX 1 & 2: access via kwargs, and key corrected to 'dbt_variable_template'
    operation            = kwargs['operation']
    model                = kwargs['model']
    profile              = kwargs['profile']
    dbt_variable_template = kwargs['dbt_variable_template']   # was: args['dbt_variable']

    dbt_command = (
        f"{dbt_dir} {operation} --vars '{dbt_variable_template}' "
        f"-s {model} --project-dir {project_dir} "
        f"--profiles-dir {project_dir} --profile {profile} --target {dbt_target} "
        "--log-path /tmp --target-path /tmp/dbt_target"
    )
    log(f"dbt_command: {dbt_command}")

    # return the dbt command
    return dbt_command

def load_initial_variables(**kwargs):
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    firstdayofmonth = now.replace(day=1).strftime('%Y%m%d')
    tempday = now - timedelta(7)

    input_date_parms = Variable.get('gdg_email_inputs', default_var='{}', deserialize_json=True)
    if isinstance(input_date_parms, str):
        try:
            input_date_parms = json.loads(input_date_parms)
        except json.JSONDecodeError:
            input_date_parms = {}

    historic_refresh = str(input_date_parms.get('historic_refresh', 'false')).lower()

    if historic_refresh == 'false':
        start_date = tempday.strftime('%Y%m%d') if now.day < 8 else firstdayofmonth
        end_date   = now.strftime('%Y%m%d')
    else:
        start_date = input_date_parms.get('start_date')
        end_date   = input_date_parms.get('end_date')
        if not start_date or not end_date:
            raise ValueError(
                "historic_refresh=true requires 'start_date' and 'end_date' in gdg_email_inputs"
            )

    dbt_variable_template = "{{START_DATE: '{0}', END_DATE: '{1}', historic_refresh: {2}}}".format(
        start_date, end_date, historic_refresh
    )

    kwargs['ti'].xcom_push(key='dbt_variable_template', value=dbt_variable_template)

# extracting SFTP creds from AWS SSM Parameter Store
def extracting_sftp_creds_f_aws(name):
    
    #### creating AWS CONNECTION ####
    try:
        
        response = get_ssm_parameter(name)

        creds = json.loads(response["Parameter"]["Value"])
        global SFTP_HOST
        global SFTP_PORT
        global SFTP_USERNAME
        global SFTP_PASSWORD
    
        SFTP_HOST = creds["SFTP_HOST"]
        SFTP_PORT = creds["SFTP_PORT"]
        SFTP_USERNAME = creds["SFTP_USERNAME"]
        SFTP_PASSWORD = creds["SFTP_PASSWORD"]
        
    except Exception as e:
        print(f'::::Error while extracting SFTP creds from AWS, ERROR: {e}')
        
    return ''

def making_sftp_connection():
    
    try:
        #creating SSH client using paramiko
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy()) #auto-approve unknown hosts

        #connecting w th eserver using the client created
        client.connect(
            hostname=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USERNAME,
            password=SFTP_PASSWORD,
            compress=True 
        )

        #opening the onnection for use
        sftp = client.open_sftp()
        
        print("\n:::Connecting with SFTP server..:::")
        
        # Always close afterwards
        sftp.close()
        return client

    except Exception as e:
        print(f'::::Error while making the SFTP connection, ERROR: {e}')
        return None

'''
function to find the latest files in sftp directory
to achieve this we find the latest date and with that date pick the latest files from the
sftp server
'''
def finding_latest_files():
    
    # finding the latest date 
    def find_date(files,fileatr):
      latest = 0
      for i in range(len(fileatr)):
        if fileatr[i].st_mtime > latest:
          latest = fileatr[i].st_mtime
      modified_date = datetime.fromtimestamp(latest,pytz.timezone("EST")).strftime('%Y-%m-%d-%H')
    #   print(modified_date)
      return modified_date
  

    #finding the latest files based on latest date
    def find_files():
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("\n\n:::Connected with SFTP server:::")
        
        #sftp.chdir("/tredence_analytics MDF Manual Report/in")
        sftp.chdir("/in")
        files = sftp.listdir()
        fileatr = sftp.listdir_attr()
        
        modified_date = find_date(files,fileatr)
        print(":::Latest Date Found Y-M-D-H::: ",modified_date)
        print("\n:::Finding Latest Files:::")

        
        
        # Find the latest files based on the modified date
        # We will filter the files that match the latest modified date
        # and return them as a list
        latestfile = []
        latestfile = [files[f] for f in range(len(fileatr)) if datetime.fromtimestamp(fileatr[f].st_mtime,pytz.timezone("EST")).strftime('%Y-%m-%d-%H') == modified_date]
        print(':::Latest files found are:::->',latestfile)
        
        
        allowed_prefixes = ("ActivityQuestions_", "Campaigns_", "PlanClaimHeader_")
        latestfile = [f for f in latestfile if f.lower().startswith(tuple(p.lower() for p in allowed_prefixes))]
        
        sftp.close()
        transport.close()
        print(":::SFTP Connection Closed::::\n")
        
        if len(latestfile) == 3:
            return [{"Total Files Found" : len(latestfile)},{"F1": latestfile[0], "F2": latestfile[1], "F3": latestfile[2]}]
        else:
            raise Exception(":::Error: Not enough files found. Expected 3 files, but found:", len(latestfile))
    
    
    latest_files = find_files()
    # print(":::Latest Files Found:::",latest_files)
    return latest_files
    
    
# print(finding_latest_files())

# downloading the files from SFTP to local instance
def download_from_sftp_to_local(sftp, remote_path, local_path, file_size):
    
    # defining vars for big files i.e size>25Mb
    CHUNK_SIZE = 1024 * 1024 * 3  # 3MB
    LARGE_FILE_THRESHOLD = 25 * 1024 * 1024  # 25MB
    
    
    use_chunked = file_size > LARGE_FILE_THRESHOLD
    print(f":::Downloading {remote_path} → {local_path} using {'chunked' if use_chunked else 'sftp.get'} method...:::")
    progress_bar = tqdm(total=file_size, unit='B', unit_scale=True, desc=f":::Progress:::", mininterval=10)
    
    # Using chunked method for all files to maintain consistency
    try:
        with sftp.open(remote_path, 'rb') as remote_file, open(local_path, 'wb') as local_file:
            while True:
                chunk = remote_file.read(CHUNK_SIZE)
                if not chunk:
                    break
                local_file.write(chunk)
                progress_bar.update(len(chunk))
    finally:
        progress_bar.close()
        
        
# uploading the files to S3     
def uploading_2_s3(file_name,local_path,aws_s3):
    new_file_name = file_name.split('_')[0]

    
    # Upload to S3
    print(f":::Uploading {file_name} to S3 as {new_file_name}.csv ...:::")
    aws_s3.upload_file(local_path, S3_BUCKET_NAME, S3_TARGET_PREFIX + new_file_name+'.csv')
    print(f":::{file_name} Successfully uploaded to {S3_BUCKET_NAME}/{S3_TARGET_PREFIX} ...:::")
    print("__________________________________________________________________________________\n")
    
    return ''
    
    
#Truncate loading the files to Snowflake
def uploading_2_snowflake():
    
    #getting SF engine for making connection
    conn = snowflake_conn(SNOWFLAKE_CONNECTION_ID)
    
    tables_config = {
        't1':       {'table_name':'MDP_ACTIVITY_QUESTIONS',
                    'columns':('''"PLANID", "PLANDETAILID", "CLAIMNUMBER", "CLAIMDETAILID", "QUESTIONID", "ACTIVITY QUESTION", "ACTIVITY ANSWER"'''),
                    'pattern' : '.*ActivityQuestions.csv.*'
        },
        't2':       {'table_name':'MDP_CAMPAIGNS',
                    'columns':('''"PLANID", "PLANDETAILID", "CLAIMNUMBER", "CLAIMDETAILID", "CAMPAIGN NAME", "CAMPAIGN PERCENTAGE"'''),
                    'pattern' : '.*Campaigns.csv.*'
        },
        't3':       {'table_name':'MDP_PLANCLAIMHEADER',
                    'columns':('''"FUNDID", "FUNDNAME", "FUNDPERIOD", "PLANID", "PLANDETAILID", "CLAIMNUMBER", "CLAIMDETAILID", "CUSTOMERID", "CUSTOMERNAME", 
                    "GROUPNAME", "AREA", "REGION", "COUNTRY", "SPONSOR", "FUNDTYPE", "FUNDCOUNTRYCODE", "REIMBURSEMENTBY", "PROMOTIONBY", "REIMBURSEMENTMETHOD",
                    "PLANSTATUS", "PLANSUBMISSIONDATE", "PLANCHANGEDDATE", "PLANAPPROVALDATE", "MARKETINGACTIVITYCONTACT", "MARKETINGACTIVITYCONTACTPHONE",
                    "MARKETINGACTIVITYCONTACTEMAIL", "MARKETINGACTIVITYOTHEREMAILS", "PLANSUBMITTERNAME", "PLANSUBMITTEREMAIL", "PLANINVESTMENTAMOUNTLC", 
                    "PLANINVESTMENTAMOUNTCONVERTEDTOUSD", "PLANREIMBURSEMENTAMOUNTLC", "PLANREIMBURSEMENTAMOUNTCONVERTEDTOUSD", "PLANAPPROVEDREIMBURSEMENTAMOUNTLC",
                    "PLANAPPROVEDREIMBURSEMENTAMOUNTCONVERTEDTOUSD", "PAPAIDAMOUNTLC", "PAPAIDAMOUNTUSD", "PAPAYMENTDATE", "PAPAYMENTID", "ACTIVITYBEGINDATE", 
                    "ACTIVITYENDDATE", "QUARTEROFACTIVITYENDDATE", "ACTIVITY", "SUBACTIVITY", "CAMPAIGN", "PLANPROMOTIONID", "PUBLICATIONNAME", "PLANDESCRIPTION", 
                    "CLAIMDESCRIPTION", "PLANAPPROVER", "PLANEXTERNALCOMMENTS", "PLANINTERNALCOMMENTS", "CLAIMAUDITSTATUS", "CLAIMVOIDDATE", "CLAIMCONTACT", 
                    "CLAIMCONTACTPHONE", "CLAIMCONTACTEMAIL", "CLAIMOTHEREMAILS", "CLAIMSUBMITTERNAME", "CLAIMSUBMITTEREMAIL", "CLAIMSUBMITTEDDATE", 
                    "TRANSACTIONHOLDREVIEWER", "HOLD/DENY REASON", "HELDDATE", "TRANSACTIONAPPROVER", "TRANSACTIONDATE", "PAYMENTREGISTERCREATED", "PAYMENTREGISTERID",
                    "PAYMENTREGISTERAPPROVER", "PAYMENTREGISTERAPPROVALDATE", "PAYMENTREGISTERCHECKRUNDATE", "PAYMENTDATESUBMITTED", "CLAIMPAIDDATE", "CLAIMAPPROVALDATE",
                    "CLAIMINVESTMENTAMOUNTLC", "CLAIMINVESTMENTAMOUNTCONVERTEDTOUSD", "CLAIMREIMBURSEMENTAMOUNTLC", "CLAIMREIMBURSEMENTAMOUNTCONVERTEDTOUSD", "CLAIMPAIDAMOUNTLC",
                    "CLAIMPAIDAMOUNTCONVERTEDTOUSD", "CLAIMHELDAMOUNTLC", "CLAIMHELDAMOUNTCONVERTEDTOUSD", "CLAIMDENIEDAMOUNTLC", "CLAIMDENIEDAMOUNTCONVERTEDTOUSD",
                    "CLAIMPROMOTIONID", "CLAIMBYDATE", "CLAIMAGE", "CLAIMAGERANGE", "LASTDATECLAIMUPDATED", "CLAIMPAIDYEAR", "CLAIMPAIDMONTH", "CLAIMPAIDQUARTER",
                    "PLANSUBMITTEDYEAR", "PLANSUBMITTEDMONTH", "PLANSUBMITTEDQUARTER", "CLAIMEXTERNALCOMMENTS", "CLAIMINTERNALCOMMENTS", "PRIMARYEMAIL", "REPORTCREATEDATE"'''),
                    'pattern' : '.*PlanClaimHeader.csv.*'
        }
    }
    
    for tb,config in tables_config.items():
        
        try: 
            cursor = conn.cursor()
                
            #truncating the table
            print(f'\n:::Truncating "{DB}"."{SCHEMA}"."{config["table_name"]}" in SF')
            truncate_query = cursor.execute(f'''TRUNCATE "{DB}"."{SCHEMA}"."{config["table_name"]}";''')
            print(truncate_query.fetchall()[0][0])

                
            #loading the table with data from S3
            copy_into_query = f'''
                                COPY INTO "{DB}"."{SCHEMA}"."{config["table_name"]}"({config["columns"]}) FROM 
                                's3://{S3_BUCKET_NAME}/{S3_TARGET_PREFIX}'
                                CREDENTIALS = (AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
                                PATTERN='{config["pattern"]}'
                                FILE_FORMAT= (
                                    TYPE=CSV
                                    COMPRESSION=AUTO,
                                    FIELD_DELIMITER=',',
                                    SKIP_HEADER=1,
                                    SKIP_BLANK_LINES=FALSE,
                                    TRIM_SPACE=FALSE,
                                    FIELD_OPTIONALLY_ENCLOSED_BY='"',
                                    ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE,
                                    REPLACE_INVALID_CHARACTERS=FALSE,
                                    EMPTY_FIELD_AS_NULL=TRUE
                                )
                                ON_ERROR='ABORT_STATEMENT'
                                PURGE=FALSE
                                TRUNCATECOLUMNS=FALSE
                                FORCE=FALSE '''
                                
            print(f':::Loading "{DB}"."{SCHEMA}"."{config["table_name"]}"...::: ')

            # LOAD COMMAND EXECUTION:
            copy_into_query_result = cursor.execute(text(copy_into_query)).fetchall()
            print(f':::Successfully Loaded "{DB}"."{SCHEMA}"."{config["table_name"]} in SF":::')
 
                
            if len(copy_into_query_result[0]) > 1:
                print('Load Details:-\n'\
                    ,f"Status:{copy_into_query_result[0][1]}"\
                    ,f"rows_parsed:{copy_into_query_result[0][2]}"\
                    ,f"rows_loaded:{copy_into_query_result[0][3]}"\
                    ,f"errors_seen:{copy_into_query_result[0][5]}")
            else:
                print(f"Load Details::{copy_into_query_result}")
                
        except Exception as e:    
            print(f'Error while LOADING the {DB}.{SCHEMA}.{config["table_name"]}, Error: {e}')
            raise Exception 
   

# function to move files from in directory to out directory on SFTP
def sftp_move_files_out(**context):

    #getting the files which are to be moved from xcom
    files = context['ti'].xcom_pull(task_ids='1_SFTP_data_load.sftp_2_s3_2_sf_load', key='return_value')

    F1, F2, F3 = files['F1'], files['F2'], files['F3']

    log(f"files picked are: F1:{F1}, F2:{F2}, F3:{F3}")

    if os.environ['ENVIRONMENT'] == 'prd':
        # extracting SFTP creds from AWS, this sets the global variables
        extracting_sftp_creds_f_aws()

        # creating sftp connection
        client = making_sftp_connection()

        #opening the onnection for use
        sftp = client.open_sftp()

        try:
            print("\n:::Moving files from IN/ to OUT/ directory on SFTP server...:::")

            #sftp.rename('/tredence_analytics MDF Manual Report/in/'+F1,'/tredence_analytics MDF Manual Report/out/'+F1)
            sftp.rename('/in/'+F1,'/out/'+F1)
            print(F1 + ' moved to OUT; ')

            #sftp.rename('/tredence_analytics MDF Manual Report/in/'+F2,'/tredence_analytics MDF Manual Report/out/'+F2)
            sftp.rename('/in/'+F2,'/out/'+F2)
            print(F2 + ' moved to OUT; ')

            #sftp.rename('/tredence_analytics MDF Manual Report/in/'+F3,'/tredence_analytics MDF Manual Report/out/'+F3)
            sftp.rename('/in/'+F3,'/out/'+F3)
            print(F3 + ' moved to OUT')

        except Exception as e:
            print(f'Error while moving files to OUT directory on SFTP, Error: {e}')
            raise Exception

        finally:
            #closing the SFTP connection & SSH client
            sftp.close()
            client.close()
            print(":::SFTP Connection Closed::::\n")
        
    else:
        print(":::SKIPPING THE FILE MOVE OUT PART, AS THIS IS NOT A PRD ENVIRONMENT, MOVING AHEAD..:::")
    
    return ''


#Function to upload the files from SFTP to S3
def upload_sftp_files_to_s3_to_sf_for_mdp_i360_new(**context):

    aws_session = get_boto3_session()
    aws_s3 = aws_session.client('s3')
    
    # extractig the SFTP creds from aws, this sets the global variables
    extracting_sftp_creds_f_aws()

    # creating sftp connection
    client = making_sftp_connection()
    
     #opening the onnection for use
    sftp = client.open_sftp()
    
    # finding the latest files to transfer
    files_to_transfer_values = finding_latest_files()
    files = files_to_transfer_values[1]
    valid_count = files_to_transfer_values[0]['Total Files Found']

    
    
    #only downloading the files for further process if exaclty 4 required files are found
    if valid_count==3: 
        try:
            for file_name in files_to_transfer_values[1].values() :
                local_path = os.path.join(LOCAL_TEMP_DIR, file_name)
                remote_path = f"/in/{file_name}"  # assuming files are in root of SFTP

                # Download from SFTP
                print(f":::Loading {remote_path} from SFTP into local instance...:::")
                
                # Get total file size from SFTP
                file_size = sftp.stat(remote_path).st_size
                
                # sftp.get(remote_path, local_path)
                
                # Get total file size from SFTP
                file_size = sftp.stat(remote_path).st_size
                
                download_from_sftp_to_local(sftp, remote_path, local_path, file_size)
                print(f":::Successfully Downloaded - {remote_path} from SFTP...:::\n")

                
                # Upload to S3
                uploading_2_s3(file_name,local_path,aws_s3)
                
            #loading data to snowflake
            uploading_2_snowflake()
            
            
            #removing the files from os
            shutil.rmtree("./mdp_i360_new_sftp_files/")
            
           #closing the SFTP connection & SSH client
            sftp.close()
            client.close()

        except Exception as e:
            print(f'Error in the MAIN code block, Error: {e}')
            raise Exception
    
    return files
