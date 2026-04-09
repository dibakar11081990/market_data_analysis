{% macro store_test_cases_failed_result() %}
-- It's to read all test cases reuslt views for DBT_TEST__AUDIT schema using information schema
{%- call statement('test_case_views', fetch_result=True) -%}
    SELECT TABLE_NAME, TABLE_SCHEMA, TABLE_CATALOG, VIEW_DEFINITION, CREATED
    FROM DEV.INFORMATION_SCHEMA.VIEWS 
    WHERE TABLE_SCHEMA = 'DBT_TEST__AUDIT'
{%- endcall -%}
{%- set views = load_result('test_case_views')['data'] -%}

-- processing all views one by one
{% for view_data in views %}
    -- sql query to read all data of view as json data to store in a single column
    {% set view_query_result %}
        select array_agg(result) from (
            select object_construct(*) as result from {{view_data[2]}}.{{view_data[1]}}.{{view_data[0]}} limit 100)
    {% endset %}
    {% set results = run_query(view_query_result) %}
    {% set results_list = results.columns[0].values()[0] | replace("'", "\\") %}

    -- Getting count to decide weather the test case is failed or passed 
    {% set query %}
            select count(*) as result from {{view_data[2]}}.{{view_data[1]}}.{{view_data[0]}}
    {% endset %}
    {% set count_value = run_query(query).columns[0].values()[0] %}

    {% if execute %}
        {% if count_value > 0 %}
            {%- set query_status -%}
                FAIL
            {%- endset -%}
        {% else %}
            {%- set query_status -%}
                PASS
            {%- endset -%}
        {% endif %}
        {% set view_query %}
            {{ view_data[3] | replace("'", "\\'") }}
        {% endset %}
        -- Taking model name from test case name
        {% set model_name = view_data[0].split('__')[0] %}
        {% set insert_query %}
            INSERT INTO BSM_SANDBOX.DBT_TEST_AUDIT.TEST_CASE_RESULT 
            VALUES('{{view_data[0]}}', '{{model_name}}', '{{results_list}}', '{{view_query}}', '{{query_status}}', '{{view_data[4]}}')
        {% endset %}
        {% do run_query(insert_query) %}
        -- Dropping the test case view once the result are inserted into snowflake table
        {% set drop_view %}
                drop view {{view_data[2]}}.{{view_data[1]}}.{{view_data[0]}}
        {% endset %}
        {% do run_query(drop_view) %}
    {% endif %}
{% endfor %}
{{ return("SUCCESS") }}
{% endmacro %}