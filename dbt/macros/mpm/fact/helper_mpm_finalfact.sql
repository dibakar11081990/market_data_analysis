{% macro helper_mpm_finalfact(ref_object, filter_predicate=None) %}

  {{- log("Macro -> helper_mpm_finalfact:" ~ ref_object.name) -}}

  {%- call statement('i_schema_get_cols', fetch_result=True) %}
        SELECT COLUMN_NAME
        FROM {{ref_object.database}}.INFORMATION_SCHEMA.COLUMNS
        WHERE
          TABLE_SCHEMA LIKE '{{ref_object.schema}}' AND
          TABLE_NAME LIKE '{{ref_object.name}}'
        ORDER BY ORDINAL_POSITION
  {%- endcall -%}

  {# call the function and get all the columns #}
  {%- set table_columns = [] -%}
  {%- set response = load_result('i_schema_get_cols') -%}
  {%- set response_data = response['data'] -%}
  {%- for row in response_data -%}
    {%- set _ = table_columns.append(row[0]) -%}
  {%- endfor -%}

  {# This line logs schema and table name where the columns are being picked #}
  {{ log('helper_mpm_finalfact: Columns found in ' ~ ref_object.schema ~ '.' ~ ref_object.name ~ ': ' ~ table_columns | join(','), info=True) }}

  {%- set target_columns = {
        'ALLOCADIA_ID': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'CATEGORY_ID': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'CHANNEL': {'datatype':'VARCHAR(50)', 'col_type': 'dimension', 'rename_col': 'INTERIM_CHANNEL'},
        'CHANNEL_TYPE': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'FACT_TYPE': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'EVENT_TYPE': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'DEVICE_TYPE': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'CONVERSION_TYPE': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'MARKETING_CONTRIBUTION_TYPE': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'DATE': {'datatype':'DATE', 'col_type': 'dimension'},
        'ACCOUNT_NAME': {'datatype':'VARCHAR(500)', 'col_type': 'dimension'},
        'ACCOUNT_TIER': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'CHILD_INDUSTRY_GROUP': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'PRODUCT': {'datatype':'VARCHAR(100)', 'col_type': 'dimension','rename_col': 'PRODUCT', 'strfunction': 'INITCAP'},
        'INITIATIVE': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'PRODUCT_FAMILY': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'PRODUCT_INDUSTRY': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'SPD_CAMPAIGN_TYPE': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'SPD_BRAND': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'SPD_AGENCY': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'ADSET_NAME': {'datatype':'VARCHAR(500)', 'col_type': 'dimension'},
        'PLACEMENT_NAME': {'datatype':'VARCHAR(500)', 'col_type': 'dimension'},
        'CAMPAIGN_NAME': {'datatype':'VARCHAR(500)', 'col_type': 'dimension'},
        'CREATIVE_NAME': {'datatype':'VARCHAR(500)', 'col_type': 'dimension'},
        'VISIT_COUNTRY': {'datatype':'VARCHAR(100)', 'col_type': 'dimension', 'rename_col': 'COUNTRY'},
        'REGION': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'MKT_REPORTING_GEO': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'OPPORTUNITY_ID': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'RENEWAL_OPPORTUNITYID': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'OPPORTUNITY_CREATED_DATE': {'datatype':'DATE', 'col_type': 'dimension'},
        'OPPORTUNITY_CLOSE_DATE': {'datatype':'DATE', 'col_type': 'dimension'},
        'OPPORTUNITY_PROJECTED_CLOSE_DATE': {'datatype':'DATE', 'col_type': 'dimension'},
        'OPPORTUNITY_RECORDTYPE': {'datatype':'VARCHAR(1000)', 'col_type': 'dimension'},
        'OPPORTUNITY_LINEITEM_ID': {'datatype':'VARCHAR(16777216)', 'col_type': 'dimension'},
        'IS_TRANSITIONED_OPPORTUNITY': {'datatype':'VARCHAR(1000)', 'col_type': 'dimension'},
        'OPPORTUNITY_CLASSIFICATION': {'datatype':'VARCHAR(1000)', 'col_type': 'dimension'},
        'STAGENAME': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'SALES_REP': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'SALES_MANAGER': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'LEAD_STATUS': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'LEAD_TYPE': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'PROGRAM': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'LANGUAGE': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'PUBLISHER_SITE': {'datatype':'VARCHAR(500)', 'col_type': 'dimension'},
        'ORDER_ID': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'SF_INDICATOR': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'OPPORTUNITY_TYPE': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'OPPORTUNITY_CHANNEL': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'SUBCRIPTION_PERIOD': {'datatype':'VARCHAR(100)', 'col_type': 'dimension'},
        'INBOUND_FLAG': {'datatype':'BOOLEAN', 'col_type': 'dimension'},
        'MARKETING_CONTRIBUTION_TYPE_HISTORIC': {'datatype':'VARCHAR(50)', 'col_type': 'dimension'},
        'PRIMARY_SALES_MOTION': {'datatype':'VARCHAR(16777216)', 'col_type': 'dimension'},
        'SALES_MOTION_CATEGORY': {'datatype':'VARCHAR(16777216)', 'col_type': 'dimension'},
        'VISITS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'CART_ADDS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'BOUNCES': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'UNIQUE_VISITORS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'SESSION_COUNT': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'BILLINGS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'BILLINGS_ACV': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'SUBSCRIPTIONS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'SEATS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'TRAIL_DOWNLOAD_COUNT': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'CART_ABANDONS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'SPEND': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EXEC_CONVERSION': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'IMPRESSIONS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'CLICKS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'MQLS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'PIPELINE': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'CONV_ACV': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'RENEWAL_PIPELINE': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'RENEWAL_ACV': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'OPPO_FRAC_CREDIT': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EMAIL_DELIVERED': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EMAIL_SENT': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EMAIL_OPEN': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EMAIL_LINK': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EMAIL_CLICK': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EMAIL_BOUNCED': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EMAIL_FORMFILL': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'EMAIL_UNSUBSCRIBE': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'PLAN_BUDGET': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'},
        'ACTUALS': {'datatype':'DECIMAL(38,6)', 'col_type': 'metric', 'agg': 'SUM'}
    }
  -%}

  {%- set predicate_col_list = [] -%}
  {%- set groupby_col_list = [] -%}

  {%- for col, col_prop_dict in target_columns.items() -%}

      {%- if col in table_columns -%}
          {%- if col_prop_dict['col_type'] == 'dimension' and 'rename_col' not in col_prop_dict  -%}
            {%- set _ = predicate_col_list.append('CAST('+col+ ' AS '+col_prop_dict['datatype']+') AS '+col ) -%}
            {%- set _ = groupby_col_list.append(col) -%}
          {%- elif col_prop_dict['col_type'] == 'dimension' and 'rename_col' in col_prop_dict and 'strfunction' in col_prop_dict  -%}
             {%- set _ = predicate_col_list.append('CAST('+col_prop_dict['strfunction']+'('+col+') AS '+col_prop_dict['datatype']+') AS '+col_prop_dict['rename_col'] ) -%}

             {%- set _ = groupby_col_list.append(col_prop_dict['rename_col']) -%}
          {%- elif col_prop_dict['col_type'] == 'dimension' and 'rename_col' in col_prop_dict  -%}
            {%- set _ = predicate_col_list.append('CAST('+col+ ' AS '+col_prop_dict['datatype']+') AS '+col_prop_dict['rename_col'] ) -%}
            {%- set _ = groupby_col_list.append(col_prop_dict['rename_col']) -%}

          {% else %}
            {%- set _ = predicate_col_list.append(col_prop_dict['agg']+'('+col+') AS '+col) -%}
          {%- endif -%}

      {% else %}
          {%- if col_prop_dict['col_type'] == 'dimension' and 'rename_col' not in col_prop_dict  -%}
            {%- set _ = predicate_col_list.append('CAST(NULL AS '+col_prop_dict['datatype']+') AS '+col ) -%}
          {%- elif col_prop_dict['col_type'] == 'dimension' and 'rename_col' in col_prop_dict  -%}
            {%- set _ = predicate_col_list.append('CAST(NULL AS '+col_prop_dict['datatype']+') AS '+col_prop_dict['rename_col'] ) -%}
          {% else %}
            {%- set _ = predicate_col_list.append('CAST(NULL AS '+col_prop_dict['datatype']+') AS '+col ) -%}
          {%- endif -%}
      {%- endif -%}
  {%- endfor -%}

  {%- if filter_predicate is none %}
    SELECT
      {{','.join(predicate_col_list).rstrip(',')}}
      ,CURRENT_DATE() AS LAST_UPDATED_DATE
    FROM {{ref_object.database}}.{{ref_object.schema}}.{{ref_object.name}}
    GROUP BY
      {{','.join(groupby_col_list)}}
  {% else %}

    SELECT
      {{','.join(predicate_col_list).rstrip(',')}}
      ,CURRENT_DATE() AS LAST_UPDATED_DATE
    FROM {{ref_object.database}}.{{ref_object.schema}}.{{ref_object.name}}
    WHERE
      {{filter_predicate}}
    GROUP BY
      {{','.join(groupby_col_list)}}

  {%- endif -%}


{% endmacro %}
