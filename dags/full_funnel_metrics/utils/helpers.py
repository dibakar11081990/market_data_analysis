import json
from datetime import datetime, timedelta

from airflow.models import Variable

from dags.common.py.utils.verbose_log import log


REVMART_SQL = (
    'SELECT COUNT(*) FROM BSD_PUBLISH.FINMART_PRIVATE.CVC_FINMART '
    'WHERE TRANSACTION_DT=CURRENT_DATE()'
)

ADOBE_SQL = (
    'SELECT COUNT(*) FROM ADP_PUBLISH.MARKETTOORDER_PUBLIC.WEB_ANALYTICS_ADOBE_ENRICHED '
    'WHERE DT=DATEADD(DAY,-1,CURRENT_DATE())'
)

MARKETABILITY_SQL = (
    'SELECT MAX(COREDATASET_SNAPSHOTS) FROM PROD.CORE_DATASETS.MARKETABILITY '
    'WHERE COREDATASET_SNAPSHOTS = CURRENT_DATE()'
)

EIOCUSTOMER_SQL = (
    'SELECT COUNT(*) FROM EIO_PUBLISH.CUSTOMER_SHARED.CUSTOMER_PHASES '
    'WHERE INSERT_DT = CURRENT_DATE()'
)

POLARIS_SQL = (
    'SELECT COUNT(*) FROM PROD.POLARIS.POLARIS_ID_LOOKUP_MCVISID_TO_ACCOUNT_CSN '
    'WHERE UPDATE_TIMESTAMP::DATE >= CURRENT_DATE()-1'
)


def load_initial_variables(**kwargs):
    """Compute date-range dbt vars and push them to XCom under 'dbt_variable_template'."""
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    firstdayofmonth = now.replace(day=1).strftime('%Y%m%d')
    tempday = now - timedelta(7)

    input_date_parms = Variable.get('ffm_configs', default_var='{}', deserialize_json=True)
    if isinstance(input_date_parms, str):
        try:
            input_date_parms = json.loads(input_date_parms)
        except json.JSONDecodeError:
            input_date_parms = {}

    historic_refresh = str(input_date_parms.get('historic_refresh', 'false')).lower()

    if historic_refresh == 'false':
        start_date = tempday.strftime('%Y%m%d') if now.day < 8 else firstdayofmonth
        end_date = now.strftime('%Y%m%d')
    else:
        start_date = input_date_parms.get('start_date')
        end_date = input_date_parms.get('end_date')
        if not start_date or not end_date:
            raise ValueError(
                "historic_refresh=true requires 'start_date' and 'end_date' in ffm_configs"
            )

    dbt_variable_template = "{{START_DATE: '{0}', END_DATE: '{1}', historic_refresh: {2}}}".format(
        start_date, end_date, historic_refresh
    )
    log(f"dbt_variable_template: {dbt_variable_template}")
    kwargs['ti'].xcom_push(key='dbt_variable_template', value=dbt_variable_template)
