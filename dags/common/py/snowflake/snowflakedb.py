#######################################################################
"""
Python class for handling direct Snowflake db interactions
"""
#######################################################################
# Imports
import snowflake.connector
from snowflake.connector import DictCursor


########################################################################

class SnowFlakeDB(object):
    """
    SnowFlake Class for handling DB level querying
    """

    def __init__(self, user='', password='', account='', warehouse='', database='', schema=''):
        """
        initialises the connection and returns the connection object
        :param user: Username for connecting to snowflake
        :param password: Password for connecting to snowflake
        :param account: service account for snowflake
        :param warehouse: which warehouse to be used in snowflake
        :param database: which database to be used as default in snowflake
        :param schema: which schema to be used as default in snowflake
        """
        # Initiate a connection to snowflake
        self.connection = snowflake.connector.connect(user=user,
                                                      password=password,
                                                      account=account,
                                                      warehouse=warehouse,
                                                      database=database,
                                                      schema=schema,
                                                      paramstyle='qmark')



    def _execute(self, query):
        """
        Given a query this function would create the cursor object and execute the query
        :param query: query which needs to be executed
        :return: cursor object for getting the resultset
        """

        # return the results of the query as a dictionary
        cursor = self.connection.cursor(DictCursor)

        try:
            # execute the query
            cursor.execute(query)

        except Exception as e:
            # retry if there is any exception for once
            cursor = self.connection.cursor()
            cursor.execute(query)

        # return the cursor
        return cursor


    def getManyRows(self, query, limit=10):
        """
        returns the list of rows of the query-result where each row is a dictionary with keys as column names
        :param query: query which needs to be executed
        :param limit: limit to be applied on the query result set
        :return: list of rows with each row being a dictionary
        """
        try:
            # execute the query
            cursor = self._execute(query)

            # set the limit for the query
            if limit:
                # if limit is set they only fetch tat many rows
                rows = cursor.fetchmany(limit)
            else:
                # if limit is set to zero then fetch all the rows
                rows = cursor.fetchall()

            # close the cursor object
            cursor.close()

        except Exception as e:
            print("getManyRows() Raised an Exception >>>  " + str(e))
            rows = None

        # return the rows
        return rows


    def getSingleRow(self, query):
        """
        returns a single row as a dictionary with keys as column names
        :param query: query which needs to be executed
        :return: dictionary with column names as keys
        """
        try:
            # Execute the query
            cursor = self._execute(query)

            # Fetch only one row from the output
            row = cursor.fetchone()

            # close the cursor object
            cursor.close()

        except Exception as e:
            print("getSingleRow() Raised an Exception >>>  " + str(e))
            row = None

        # return the final row
        return row


    def getTableCols(self, db, schema, table):
        """
        Returns the underlying Column schema as a list
        :param db: which database to be used
        :param schema: which schema to be used
        :param table: table from which the column list needs to be obtained
        :return: list containing table columns
        """
        try:
            # prepare the query which needs to be executed
            query = 'select * from "{database}"."{schema}"."{table}" limit 1;'.format(database=db,
                                                                                      schema=schema,
                                                                                      table=table)

            # Execute the query
            cursor = self._execute(query)

            # Get the table columns
            table_cols = [col[0] for col in cursor.description]

            # close the cursor object
            cursor.close()

        except Exception as e:
            print("getTableCols() Raised an Exception >>>  " + str(e))
            table_cols = []

        # return the table cols
        return table_cols


    def insert_bulk(self, query, data):
        """
        Inserts bulk data in to a snowflake table.
        :param query: parameterized insert query
        :param data: data which needs to be dumped in to snowflake table.
        :return:
        """

        # return the results of the query as a dictionary
        cursor = self.connection.cursor()

        # execute the insert query
        cursor.executemany(query, data)

        # close the cursor object
        cursor.close()


    def close(self):
        """
        close the connection
        """
        self.connection.close()