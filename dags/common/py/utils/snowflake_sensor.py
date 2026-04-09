

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeSqlSensor(BaseSensorOperator):
    def __init__(self, conn_id, sql, parameters=None, **kwargs):
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters
        super().__init__(**kwargs)

    def get_connection(self):
        ## Create connection to your snowflake database
        #snowflake_connection = BaseHook.get_connection()
        snowflake_connection = SnowflakeHook(snowflake_conn_id=self.conn_id)
        return snowflake_connection
    
    def poke(self, context):
        self.snowflake_conn = self.get_connection()
        self.log.info('Poking: %s (with parameters %s)', self.sql, self.parameters)
        records = self.snowflake_conn.get_first(self.sql)
        if not records:
            return False
        else:
            if str(records[0]) in ('0', '',):
                return False
            else:
                return True