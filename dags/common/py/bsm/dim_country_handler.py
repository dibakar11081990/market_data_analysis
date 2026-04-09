#############################################################################
# Imports
import pandas as pd
import pycountry
from dags.common.py.bsm.fuzzysearch_country import fuzzy_search_country
from dags.common.py.snowflake.snowflakedb import SnowFlakeDB


#############################################################################


class DimCountry:
    """
    Class for handling the country dimension across the project.
    """

    def __init__(self, snowflake_account, snowflake_user, snowflake_password):
        """
        Constructor.
        """

        # Snowflake account settings.
        self.snowflake_account = snowflake_account
        self.snowflake_user = snowflake_user
        self.snowflake_password = snowflake_password
        self.snowflake_warehouse = 'BSM_ETL'
        self.snowflake_database = 'RAW'
        self.snowflake_schema = 'LOOKUPS'

        # Country -> Geo Mapping table
        self.QUERY_DIM_GEO = "SELECT DISTINCT COUNTRY, GEO, MKT_REPORTING_GEO, REGION, ALPHA_3 " \
                             "FROM DIM_GEO"

        # Dynamic Lookup Table.
        self.QUERY_DIM_COUNTRY = "SELECT * FROM DIM_FUZZYSEARCH_COUNTRY"

        # country exclusion list.
        self.QUERY_COUNTRY_EXCLUSION_LIST = "SELECT DISTINCT CODES FROM COUNTRY_EXCLUSION_LIST"

        # Bulk insert to the raw tables.
        self.INSERT_QUERY = "INSERT INTO DIM_FUZZYSEARCH_COUNTRY " \
                            "(FUZZY_SEARCH_CODE, ALPHA_2, ALPHA_3, COUNTRY, REGION, " \
                            "GEO, MKT_REPORTING_GEO) VALUES (?, ?, ?, ?, ?, ?, ?)"


    def get_db_connection(self):
        """
        Create the connection to snowflake and return the connection.
        :return: connection object.
        """

        # create a connection to Snowflake.
        snowflake_connection = SnowFlakeDB(user=self.snowflake_user,
                                           password=self.snowflake_password,
                                           account=self.snowflake_account,
                                           warehouse=self.snowflake_warehouse,
                                           database=self.snowflake_database,
                                           schema=self.snowflake_schema)

        # return the connection
        return snowflake_connection


    def close_connection(self, conn):
        """
        Close the connection with snowflake.
        :param conn: Connection to snowflake.
        :return:
        """
        # close the connection.
        conn.close()


    def get_geo_df(self, conn):
        """
        Get the geo dimension data
        :param conn: Connection to snowflake.
        :return: geo_df
        """

        # Issue the query to get the GEO_MAPPING Lookup
        snowflake_rows = conn.getManyRows(query=self.QUERY_DIM_GEO, limit=None)

        # convert it in to pandas df
        geo_df = pd.DataFrame(snowflake_rows)

        # select only the required columns
        geo_df = geo_df[['ALPHA_3', 'REGION', 'GEO', 'MKT_REPORTING_GEO']]

        # drop those rows if alpha_3 is null
        geo_df = geo_df.dropna(subset=['ALPHA_3'])

        # return geo.
        return geo_df


    def get_mapped_country_codes(self, conn):
        """
        Get the Country codes for which we already have mapping.
        :param conn: Connection to snowflake.
        :return: mapped_country_codes_list
        """

        # Issue the query to get the GEO_MAPPING Lookup
        snowflake_rows = conn.getManyRows(query=self.QUERY_DIM_COUNTRY, limit=None)

        # convert it in to pandas df
        reference_df = pd.DataFrame(snowflake_rows)

        country_reference_list = []
        if not reference_df.empty:
            # get the list for which there is a mapping already existing.
            country_reference_list = reference_df['FUZZY_SEARCH_CODE'].values.tolist()

        return country_reference_list


    def get_exclusion_codes(self, conn):
        """
        Get those codes which needs to be excluded
        :param conn:  Connection to snowflake.
        :return: exclusion_list.
        """

        # Issue the query to get the GEO_MAPPING Lookup
        snowflake_rows = conn.getManyRows(query=self.QUERY_COUNTRY_EXCLUSION_LIST, limit=None)

        # convert it in to pandas df
        reference_df = pd.DataFrame(snowflake_rows)

        exclusion_list = []
        if not reference_df.empty:
            # get the list for which there is a mapping already existing.
            exclusion_list = reference_df['CODES'].str.upper().values.tolist()

        return exclusion_list


    @staticmethod
    def get_fuzzysearch_results(country_name):
        """
        For a given country_name which can be in any format ALPHA_2/ALPHA_3/Country string
        it would try to search for the right ISO Standard name for the corresponding country
        :param country_name: Fuzzy search on country name.
        :return:
        """
        try:

            # check if the input is ALPHA_2
            if len(country_name) == 2:
                pycountry_obj = pycountry.countries.get(alpha_2=country_name)

            # check if the input is ALPHA_3
            elif len(country_name) == 3:
                pycountry_obj = pycountry.countries.get(alpha_3=country_name)

            else:

                # Perform fuzzy search use some string comparision alg to find the right ISO Country.
                pycountry_obj = fuzzy_search_country(country_name)[0]

        except:
            pycountry_obj = None
            pass

        # if the search returned the country object return alpha_2/alpha_3/country_name
        if pycountry_obj is not None:
            return [pycountry_obj.alpha_2, pycountry_obj.alpha_3, pycountry_obj.name]
        else:
            return [None, None, None]


    def dim_country_handler(self, country_list):
        """
        Does the fuzzy search for the list of countries being passed and updates it to the database
        it returns the exception dataframe.
        :return: exception_df
        """

        # get the connection.
        conn = self.get_db_connection()

        # get the geo df
        geo_df = self.get_geo_df(conn=conn)

        # get the mapped country
        mapped_country_codes = self.get_mapped_country_codes(conn=conn)

        # get the exclusion list.
        exclusion_list = self.get_exclusion_codes(conn=conn)

        # fuzzy search to get the right country
        search_results = []
        for country in country_list:
            if country is not None:
                country_u = country.upper()
                if country_u not in mapped_country_codes and country_u not in exclusion_list and len(country_u) >= 2:
                    fuzzy_search_result = self.get_fuzzysearch_results(country_u)
                    search_results.append([country_u] + fuzzy_search_result)

        # convert the output in to dataframe again.
        final_df = pd.DataFrame(search_results, columns=['FUZZY_SEARCH_CODE', 'ALPHA_2', 'ALPHA_3', 'COUNTRY'])

        # Merge the geo lookup
        final_df = final_df.merge(geo_df, on='ALPHA_3', how='left')

        # Exception report
        exception_df = final_df[final_df.isnull().any(axis=1)]
        exception_df = exception_df[exception_df['FUZZY_SEARCH_CODE'] != 'UNKNOWN']
        print("Exception Report")
        print(exception_df.head(10000))

        # drop the nan
        final_df = final_df.dropna()

        # final columns
        final_df = final_df[['FUZZY_SEARCH_CODE', 'ALPHA_2', 'ALPHA_3', 'COUNTRY', 'REGION', 'GEO', 'MKT_REPORTING_GEO']]

        # drop duplicates
        final_df.drop_duplicates(subset="FUZZY_SEARCH_CODE", inplace=True)

        print("Below codes are added to the table.")
        print(final_df['FUZZY_SEARCH_CODE'].values.tolist())

        if not final_df.empty:
            # Execute query to insert in to snowflake.
            conn.insert_bulk(query=self.INSERT_QUERY, data=final_df.values.tolist())

        # close the connection.
        self.close_connection(conn=conn)

        return exception_df
