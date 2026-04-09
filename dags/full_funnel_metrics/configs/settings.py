##################################################################
"""
Settings related to Full Funnel Metrics DAG.

"""
# Imports
import os

# Get the System Environment
ENVIRONMENT = os.environ['ENVIRONMENT']

######################################################
# Airflow Dag / Connection Related settings

# Dag Name.
DAG_ID = 'Full_Funnel_Metrics'
# Owner of the Job.
DAG_OWNER = 'ESAE Product Management'
# Schedule Interval
SCHEDULE_INTERVAL = '00 19 * * *'


##################################################################
# Task names
TASK_DBT_RUN_FFM = 'TASK_DBT_RUN_FFM'

TASK_DBT_PRE_REFRESH_TESTS_FFM = 'TASK_DBT_PRE_REFRESH_TESTS_FFM'

TASK_DBT_POST_REFRESH_TESTS_FFM = 'TASK_DBT_POST_REFRESH_TESTS_FFM'

SNOWFLAKE_CONN_ID = os.environ['SNOWFLAKE_CONN']


TASK_REVMART_SQLSENSOR = 'TASK_REVMART_SQLSENSOR'
REVMART_SQL = 'SELECT COUNT(*) FROM BSD_PUBLISH.FINMART_PRIVATE.CVC_FINMART WHERE TRANSACTION_DT=CURRENT_DATE()'

TASK_ADOBE_SQLSENSOR ='TASK_ADOBE_SQLSENSOR'
ADOBE_SQL ='SELECT COUNT(*) FROM  ADP_PUBLISH.MARKETTOORDER_PUBLIC.WEB_ANALYTICS_ADOBE_ENRICHED WHERE DT=DATEADD(DAY,-1,CURRENT_DATE())'


TASK_MARKETABILITY_SQLSENSOR = 'TASK_MARKETABILITY_SQLSENSOR'
MARKETABILITY_SQL = 'SELECT MAX(COREDATASET_SNAPSHOTS) FROM PROD.CORE_DATASETS.MARKETABILITY WHERE COREDATASET_SNAPSHOTS = CURRENT_DATE()'

TASK_EIOCUSTOMER_SQLSENSOR = 'TASK_EIOCUSTOMER_SQLSENSOR'
EIOCUSTOMER_SQL = 'SELECT COUNT(*) FROM EIO_PUBLISH.CUSTOMER_SHARED.CUSTOMER_PHASES WHERE INSERT_DT = CURRENT_DATE();'


TASK_POLARIS_SQLSENSOR='TASK_POLARIS_SQLSENSOR'
POLARIS_SQL = 'SELECT COUNT(*) FROM PROD.POLARIS.POLARIS_ID_LOOKUP_MCVISID_TO_ACCOUNT_CSN WHERE UPDATE_TIMESTAMP::DATE >= CURRENT_DATE()-1'

SENSOR_TIMEOUT =900
SENSOR_POKE_INTERVAL=23*60*60

##################################################################
# DBT related Settings.

# DBT Profile.
DBT_PROFILE = 'ffm'

# DBT Models. Individual fact table models or TAG name
DBT_TAG_FFM = 'tag:full_funnel_metrics'

#TAG for Post refresh test cases
DBT_TAG_FFM_POST_CHECKS = 'tag:full_funnel_metrics_post_check'

#TAG for Pre refresh test cases
DBT_TAG_FFM_PRE_CHECKS = 'tag:full_funnel_metrics_pre_checks'


#AIRFLOW UI Variable name which holds the Boolean value to decide whether to run post refresh checks or not
RUN_POST_CHECKS_VARIABLE = "run_post_checks_for_full_funnel_metrics_dag"
