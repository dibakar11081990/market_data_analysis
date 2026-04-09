##################################################################
"""
Settings related to Adobe should be put here.
"""
# Imports
import os
# Get the System Environment
ENVIRONMENT = os.environ['ENVIRONMENT']
SNOWFLAKE_CONN_ID = os.environ['SNOWFLAKE_CONN']


##################################################################

# EMR Related settings

# EMR Cluster name
EMR_CLUSTER_NAME = 'emr-marketing-mars-{env}-mpmingestion'.format(env=ENVIRONMENT)
# EMR Artifact s3 bucket name
EMR_ARTIFACT_S3_BUCKET = 'com.tredence_analytics.marketing.mars.{env}.ue1.emr-artifacts'.format(env=ENVIRONMENT)
# EMR Cloudformation template
EMR_CLOUDFORMATION_TEMPLATE_FILE = 'https://s3.amazonaws.com/{name}/mpm_ingestion_emr_stack.yaml'.format(
    name=EMR_ARTIFACT_S3_BUCKET)
# EMR Bootstrap script
EMR_BOOTSTRAP_FILE = 's3://{name}/bootstrap_scripts/EMR_Snowflake_bootstrap.sh'.format(name=EMR_ARTIFACT_S3_BUCKET)
# EMR Cloudformation Stack name
EMR_CLOUDFORMATION_STACK_NAME = 'cf-marketing-mars-{env}-EMR'.format(env=ENVIRONMENT)


# EMR Cloudformation template defaults.
EMR_CLOUDFORMATION_PARMS = {
    'EMRVersion': 'emr-5.24.0',
    'MasterInstanceType': 'm5.2xlarge',
    'MasterNodeCount': '1',
    'CoreInstanceType': 'm5.2xlarge',
    'CoreNodeCount': '2',
    'MinCoreNodeCount': '2',
    'MaxCoreNodeCount': '2',
    'KeyPairName': 'EMR-Key',
    'VPCID': 'vpc-07ee290c8fe619e52',
    'VPCSubnetId': 'subnet-0420b35f11e225e56',
    'RemoteAccessCIDR': '10.0.0.0/8',
    'BootstrapSnowflakeConnectorScript': EMR_BOOTSTRAP_FILE,
    'Environment': ENVIRONMENT
}

# MAX Limit on the Core Node Count.
EMR_MAX_CORE_NODE_COUNT = 9

##################################################################
# Airflow  Dag / Connections ID's / Variables.

# Owner of the Job.
DAG_OWNER = 'airflow'
# Schedule Interval
##SCHEDULE_INTERVAL = None
SCHEDULE_INTERVAL = '30 5/4 * * *'


# Connection to Snowflake
AIRFLOW_SNOWFLAKE_CONN_ID = 'mars.{env}.snowflake_conn'.format(env=ENVIRONMENT)
# Connection to S3
AIRFLOW_S3_CONN_ID = 'mars.{env}.s3_conn'.format(env=ENVIRONMENT)
# Matillion Connection ID.
AIRFLOW_MATILLION_CONN_ID = 'mars.{env}.matillion_conn'.format(env=ENVIRONMENT)
# MYSQL Connection
AIRFLOW_MYSQL_CONN_ID = 'mars.{env}.mysql_conn'.format(env=ENVIRONMENT)
# Ingestion Tracker API Connection
# TODO Bug in airflow hardcoded to http_default in the operator.
AIRFLOW_INGESTION_TRACKER_API_CONN_ID = 'http_default'

# Airflow Vars.
AIRFLOW_VAR_ADOBE_AUTOMATED = 'adobe_automated_ingestion_settings'
AIRFLOW_VAR_ADOBE_ONDEMAND = 'adobe_ondemand_ingestion_settings'


##################################################################
# Spark related parms

# Spark job to do sessionization.
SPARK_JOB_SESSIONIZATION = 'adobe_sessionization'
# Spark job to create the ID Stack.
SPARK_JOB_ID_STACK = 'adobe_id_stack'
# Spark job to create the engagement stack.
SPARK_JOB_EVT_STACK = 'adobe_engagement_stack_revised'
# Spark job to create the conversion stack.
SPARK_JOB_CONV_STACK = 'adobe_conversion_stack'
# Spark S3 Landing zone.
##SPARK_S3_LANDING_ZONE_BUCKET = 's3://com.tredence_analytics.marketing.mars.prd.ue1.landing-zone/adp-data/markettoorder_public/web_analytics_adobe' #original location in old airlfow
SPARK_S3_LANDING_ZONE_BUCKET = 's3://com.tredence_analytics.marketing.mars.{env}.ue1.landing-zone/adp-data/markettoorder_public/web_analytics_adobe_multiple'.format(env='prd')
# SPARK_S3_LANDING_ZONE_BUCKET = 's3://com.tredence_analytics.marketing.mars.{env}.ue1.landing-zone/adp-data/markettoorder_public/web_analytics_adobe_hourly/data'.format(env=ENVIRONMENT) #use this for hourly changes

# Spark S3 Adobe Stack bucket.
SPARK_S3_ADOBE_STACK_BUCKET = 's3://com.tredence_analytics.marketing.mars.{env}.ue1.mpm/adobe/stack_data_revised_multiple'.format(env=ENVIRONMENT)

# Stack lookup files.
LOOKUPS = {
    SPARK_JOB_EVT_STACK: 's3://com.tredence_analytics.marketing.mars.{env}.ue1.mpm/adobe/lookups/mod_mktvar002.csv'.format(env=ENVIRONMENT)
}

##################################################################
# Matillion Configurations

# Matillion group name where MPM related jobs are present.
MATILLION_GROUP_NAME = 'marketing'
# Matillion Project where the MPM related jobs are present.
MATILLION_PROJECT_NAME = 'mars-{env}'.format(env=ENVIRONMENT.upper())
# Matillion Version.
MATILLION_VERSION_NAME = 'default'
# Matillion environment.
MATILLION_ENV_NAME = 'MPM_{env}'.format(env=ENVIRONMENT.upper())
# ID Stack orchestration job name in matillion.
MATILLION_JOB_ID_STACK = 'ID_STACK_DRIVER'
# Engagement Stack orchestration job name in matillion.
MATILLION_JOB_ENV_STACK = 'ENGAGEMENT_STACK_DRIVER'
# Conversion Stack orchestration job name in matillion.
MATILLION_JOB_CONV_STACK = 'CONVERSION_STACK_DRIVER'


# Job Variables within Matillion.
MATILLION_VAR_S3_PREFIX = 'stack_data_revised_multiple'
MATILLION_VAR_MPM_SCHEMA = 'MPM'


##################################################################
# DBT related Settings.

# DBT Profile.
DBT_PROFILE = 'mpm'
# DBT Models.
DBT_MODEL_EXTTBL_REFRESH = 'tag:exttbl_adobe'
###DBT_MODEL_S3LOAD = 'tag:mpm_ds_adobe'
DBT_MODEL_ADOBE_WORKFLOW = 'tag:mpm_ds_adobe'


# DAG Constants
##################################################################
# SUBDAG Names

SUBDAG_AUTOMATED_INGESTION_INIT = 'SUBDAG_AUTOMATED_INGESTION_INIT'
SUBDAG_EMR_INIT = 'SUBDAG_EMR_INIT'
SUBDAG_EMR_STEPS = 'SUBDAG_EMR_STEPS'
SUBDAG_MATILLION = 'SUBDAG_MATILLION'
SUBDAG_DEL_EMR = 'SUBDAG_DEL_EMR'
SUBDAG_AUTOMATED_INGESTION_CLEANUP = 'SUBDAG_AUTOMATED_INGESTION_CLEANUP'


##################################################################
# TASK Names

TASK_SQL_SENSOR = 'SQL_SENSOR'
TASK_HTTP_SENSOR = 'HTTP_SENSOR'
TASK_SET_AIRFLOW_SETTINGS = 'SET_AIRFLOW_SETTINGS'
TASK_BUILD_SPARK_REPO = 'BUILD_SPARK_REPO'
TASK_CREATE_EMR_CLUSTER = 'CREATE_EMR_CLUSTER'
TASK_SPARK_CONFIG_GENERATOR = 'SPARK_CONFIG_GENERATOR'
TASK_EMR_SESSIONIZATION = 'EMR_SESSIONIZATION'
TASK_EMR_IDSTACK = 'EMR_IDSTACK'
TASK_EMR_CONVERSION_STACK = 'EMR_CONVERSION_STACK'
TASK_EMR_ENGAGEMENT_STACK = 'EMR_ENGAGEMENT_STACK'
TASK_DEL_S3_USELESS_FILES = 'DEL_S3_USELESS_FILES'
TASK_MATILLION_IDSTACK_JOB = 'MATILLION_IDSTACK_JOB'
TASK_MATILLION_EVT_STACK_JOB = 'MATILLION_EVT_STACK_JOB'
TASK_MATILLION_CONV_STACK_JOB = 'MATILLION_CONV_STACK_JOB'
TASK_DBT_EXTTABLE_REFRESH = 'DBT_EXTTBL_REFRESH'
TASK_DELETE_EMR_CLUSTER = 'DELETE_EMR_CLUSTER'
TASK_TRIGGER_SPEND_REFRESH_DAG = 'TASK_TRIGGER_SPEND_REFRESH_DAG'
TASK_RESET_AIRFLOW_SETTINGS = 'RESET_AIRFLOW_SETTINGS'
TASK_UPDATE_RDS = 'UPDATE_RDS'
TASK_RETRIGGER_DAG = 'RETRIGGER_DAG'
TASK_DBT_S3_LOAD = 'TASK_ADOBE_DBT_S3_LOAD'
##TASK_DBT_ADOBE_WORKFLOW = 'TASK_DBT_ADOBE_WORKFLOW'


##################################################################
# XCOM Names

XCOM_ORIGINAL_INGESTION_SETTINGS = 'ORIGINAL_INGESTION_SETTINGS'
XCOM_ORIGINAL_EMR_SETTINGS = 'ORIGINAL_EMR_SETTINGS'


##################################################################

# ADOBE DATASOURCE NAME in RDS
ADOBE_DATASOURCE_NAME = 'ADP_GLUE_ADOBE'


BUCKET_NAME = 'com.tredence_analytics.marketing.mars.{env}.ue1.landing-zone'.format(env='prd')
PREFIX = 'adp-data/markettoorder_public/web_analytics_adobe_multiple'
# PREFIX = 'adp-data/markettoorder_public/web_analytics_adobe_hourly/data' #uncomment this for hourly changes