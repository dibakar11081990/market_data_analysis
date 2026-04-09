-- macros/select_db_and_schema.sql

{% macro get_db_and_schema(table_name) %}
    {% if target.name|lower == 'prd' %}
        {{ return('PROD.MQA_AMPLIFY_ABM.' ~ table_name) }}
    {% else %}
        {{ return('BSM_SANDBOX.MQA_AMPLIFY_ABM_SANDBOX.' ~ table_name) }}
    {% endif %}
{% endmacro %}



