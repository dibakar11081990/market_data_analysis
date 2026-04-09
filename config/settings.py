##################################################################
"""
Settings common across DAGS should be put here
"""
# Imports
import os
import boto3
from botocore.exceptions import ClientError
import json 
from airflow.hooks.base import BaseHook
import yaml
from types import SimpleNamespace
import json




# Get the System Environment
ENVIRONMENT = os.environ['ENVIRONMENT']
##################################################################

# AWS Related params
AWS_DEFAULT_REGION = 'us-east-1'

# EMR Configurations
EMR_INSTANCE_TYPES = {
    'm3.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 15, 'YARN_NODEMANAGER_MEMORY': 14336},
    'm3.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 30, 'YARN_NODEMANAGER_MEMORY': 28672},

    'm4.large': {'NO_OF_CORES': 2, 'MEMORY_PER_NODE': 8, 'YARN_NODEMANAGER_MEMORY': 7168},
    'm4.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 16, 'YARN_NODEMANAGER_MEMORY': 15360},
    'm4.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'm4.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 64, 'YARN_NODEMANAGER_MEMORY': 57344},
    'm4.10xlarge': {'NO_OF_CORES': 40, 'MEMORY_PER_NODE': 160, 'YARN_NODEMANAGER_MEMORY': 155648},
    'm4.16large': {'NO_OF_CORES': 65, 'MEMORY_PER_NODE': 256, 'YARN_NODEMANAGER_MEMORY': 253952},

    'm5.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 16, 'YARN_NODEMANAGER_MEMORY': 15360},
    'm5.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'm5.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 64, 'YARN_NODEMANAGER_MEMORY': 57344},
    'm5.12xlarge': {'NO_OF_CORES': 48, 'MEMORY_PER_NODE': 192, 'YARN_NODEMANAGER_MEMORY': 188416},
    'm5.24xlarge': {'NO_OF_CORES': 96, 'MEMORY_PER_NODE': 384, 'YARN_NODEMANAGER_MEMORY': 385024},

    'm5a.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 16, 'YARN_NODEMANAGER_MEMORY': 15360},
    'm5a.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'm5a.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 64, 'YARN_NODEMANAGER_MEMORY': 57344},
    'm5a.12xlarge': {'NO_OF_CORES': 48, 'MEMORY_PER_NODE': 192, 'YARN_NODEMANAGER_MEMORY': 188416},
    'm5a.24xlarge': {'NO_OF_CORES': 96, 'MEMORY_PER_NODE': 384, 'YARN_NODEMANAGER_MEMORY': 385024},

    'm5d.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 16, 'YARN_NODEMANAGER_MEMORY': 15360},
    'm5d.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'm5d.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 64, 'YARN_NODEMANAGER_MEMORY': 57344},
    'm5d.12xlarge': {'NO_OF_CORES': 48, 'MEMORY_PER_NODE': 192, 'YARN_NODEMANAGER_MEMORY': 188416},
    'm5d.24xlarge': {'NO_OF_CORES': 96, 'MEMORY_PER_NODE': 384, 'YARN_NODEMANAGER_MEMORY': 385024},

    'c3.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 7.5, 'YARN_NODEMANAGER_MEMORY': 5632},
    'c3.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 15, 'YARN_NODEMANAGER_MEMORY': 13312},
    'c3.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 30, 'YARN_NODEMANAGER_MEMORY': 28672},
    'c3.8xlarge': {'NO_OF_CORES': 32, 'MEMORY_PER_NODE': 60, 'YARN_NODEMANAGER_MEMORY': 53248},

    'c4.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 7.5, 'YARN_NODEMANAGER_MEMORY': 5632},
    'c4.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 15, 'YARN_NODEMANAGER_MEMORY': 13312},
    'c4.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 30, 'YARN_NODEMANAGER_MEMORY': 28672},
    'c4.8xlarge': {'NO_OF_CORES': 36, 'MEMORY_PER_NODE': 60, 'YARN_NODEMANAGER_MEMORY': 53248},

    'c5.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 8, 'YARN_NODEMANAGER_MEMORY': 7168},
    'c5.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 16, 'YARN_NODEMANAGER_MEMORY': 14336},
    'c5.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'c5.9xlarge': {'NO_OF_CORES': 36, 'MEMORY_PER_NODE': 72, 'YARN_NODEMANAGER_MEMORY': 65536},
    'c5.18xlarge': {'NO_OF_CORES': 72, 'MEMORY_PER_NODE': 144, 'YARN_NODEMANAGER_MEMORY': 139264},

    'c5d.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 8, 'YARN_NODEMANAGER_MEMORY': 7168},
    'c5d.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 16, 'YARN_NODEMANAGER_MEMORY': 14336},
    'c5d.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'c5d.9xlarge': {'NO_OF_CORES': 36, 'MEMORY_PER_NODE': 72, 'YARN_NODEMANAGER_MEMORY': 65536},
    'c5d.18xlarge': {'NO_OF_CORES': 72, 'MEMORY_PER_NODE': 144, 'YARN_NODEMANAGER_MEMORY': 188416},

    'c5n.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 10.5, 'YARN_NODEMANAGER_MEMORY': 9216},
    'c5n.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 21, 'YARN_NODEMANAGER_MEMORY': 18432},
    'c5n.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 42, 'YARN_NODEMANAGER_MEMORY': 34816},
    'c5n.9xlarge': {'NO_OF_CORES': 36, 'MEMORY_PER_NODE': 96, 'YARN_NODEMANAGER_MEMORY': 90112},
    'c5n.18xlarge': {'NO_OF_CORES': 72, 'MEMORY_PER_NODE': 192, 'YARN_NODEMANAGER_MEMORY': 188416},

    'r3.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 30.5, 'YARN_NODEMANAGER_MEMORY': 28672},
    'r3.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 61, 'YARN_NODEMANAGER_MEMORY': 54272},
    'r3.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 122, 'YARN_NODEMANAGER_MEMORY': 116736},
    'r3.8xlarge': {'NO_OF_CORES': 32, 'MEMORY_PER_NODE': 244, 'YARN_NODEMANAGER_MEMORY': 241664},

    'r4.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 30.5, 'YARN_NODEMANAGER_MEMORY': 28672},
    'r4.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 61, 'YARN_NODEMANAGER_MEMORY': 54272},
    'r4.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 122, 'YARN_NODEMANAGER_MEMORY': 116736},
    'r4.8xlarge': {'NO_OF_CORES': 32, 'MEMORY_PER_NODE': 244, 'YARN_NODEMANAGER_MEMORY': 241664},
    'r4.16xlarge': {'NO_OF_CORES': 64, 'MEMORY_PER_NODE': 488, 'YARN_NODEMANAGER_MEMORY': 491520},

    'r5.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'r5.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 64, 'YARN_NODEMANAGER_MEMORY': 57344},
    'r5.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 128, 'YARN_NODEMANAGER_MEMORY': 122880},
    'r5.12xlarge': {'NO_OF_CORES': 48, 'MEMORY_PER_NODE': 384, 'YARN_NODEMANAGER_MEMORY': 385024},
    'r5.24xlarge': {'NO_OF_CORES': 96, 'MEMORY_PER_NODE': 768, 'YARN_NODEMANAGER_MEMORY': 778240},

    'r5a.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'r5a.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 64, 'YARN_NODEMANAGER_MEMORY': 57344},
    'r5a.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 128, 'YARN_NODEMANAGER_MEMORY': 122880},
    'r5a.12xlarge': {'NO_OF_CORES': 48, 'MEMORY_PER_NODE': 384, 'YARN_NODEMANAGER_MEMORY': 385024},
    'r5a.24xlarge': {'NO_OF_CORES': 64, 'MEMORY_PER_NODE': 768, 'YARN_NODEMANAGER_MEMORY': 778240},

    'r5d.xlarge': {'NO_OF_CORES': 4, 'MEMORY_PER_NODE': 32, 'YARN_NODEMANAGER_MEMORY': 30720},
    'r5d.2xlarge': {'NO_OF_CORES': 8, 'MEMORY_PER_NODE': 64, 'YARN_NODEMANAGER_MEMORY': 57344},
    'r5d.4xlarge': {'NO_OF_CORES': 16, 'MEMORY_PER_NODE': 128, 'YARN_NODEMANAGER_MEMORY': 122880},
    'r5d.12xlarge': {'NO_OF_CORES': 48, 'MEMORY_PER_NODE': 384, 'YARN_NODEMANAGER_MEMORY': 385024},
    'r5d.24xlarge': {'NO_OF_CORES': 64, 'MEMORY_PER_NODE': 768, 'YARN_NODEMANAGER_MEMORY': 778240},

}

##################################################################
# Airflow Connections ID's

# # Slack Connection ID.
AIRFLOW_SLACK_CONN_ID = 'mars.{env}.Slack_Conn'.format(env=ENVIRONMENT)

# AWS Connection ID.
AIRFLOW_AWS_CONN_ID = 'mars.aws_default'


##################################################################
# SNS Topic ARN's

SNS_AIRFLOW_NOTIFICATION_TOPIC_ARN = 'arn:aws:sns:us-east-1:261991560536:Airflow_Notification_Topic'


##################################################################
# XCOM Names

XCOM_SPARK_DRIVER_MEMORY = 'SPARK_DRIVER_MEMORY'
XCOM_SPARK_DRIVER_CORES = 'SPARK_DRIVER_CORES'
XCOM_SPARK_YARN_DRIVER_MEMORYOVERHEAD = 'SPARK_YARN_DRIVER_MEMORYOVERHEAD'
XCOM_SPARK_EXECUTOR_INSTANCES = 'SPARK_EXECUTOR_INSTANCES'
XCOM_SPARK_EXECUTOR_MEMORY = 'SPARK_EXECUTOR_MEMORY'
XCOM_SPARK_EXECUTOR_CORES = 'SPARK_EXECUTOR_CORES'
XCOM_SPARK_YARN_EXECUTOR_MEMORYOVERHEAD = 'SPARK_YARN_EXECUTOR_MEMORYOVERHEAD'

##################################################################




DBT_PROFILES_DIR = os.environ['DBT_PROFILES_DIR']

def create_dbt_profile(**kwargs):

    conn = BaseHook.get_connection(kwargs['conn_id_'])
    print(f"******** Connection attributes: {dir(conn)}, {conn.conn_type}, {SimpleNamespace(**json.loads(conn.extra))}")

    profile = {
        f"{kwargs['profile_']}": {
            'target': f"{kwargs['target_']}",
            'outputs': {
                f"{kwargs['target_']}": {
                    'type': 'snowflake',
                    'account': 'tredence_analytics.us-east-1',
                    'host': conn.host,
                    'user': conn.login,
                    'password': conn.password,
                    'database': 'BSM_ETL',
                    'schema': conn.schema,
                    'warehouse': 'BSM_ETL_MEDIUM',
                    'threads': 6,
                    'role': 'BSM_DEVELOPER',
                    'client_session_keep_alive': False
                }
            }
        }
    }

    print(f"******** Profile: {profile}")
    
    os.makedirs(DBT_PROFILES_DIR, exist_ok=True)
    with open(os.path.join(DBT_PROFILES_DIR, "profiles.yml"), "w+") as f:
        yaml.dump(profile, f, default_flow_style=False)