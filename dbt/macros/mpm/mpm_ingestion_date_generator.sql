{% macro mpm_ingestion_date_generator(start_date, end_date) -%}

    {# Function which calls the snowflake JS UDF Code.#}
    {%- call statement('get_intervals_between', fetch_result=True) %}
        SELECT "PROD"."MPM"."MPM_INGESTION_DATE_GENERATOR"('{{start_date}}','{{end_date}}');
    {%- endcall -%}

    {# Issue the snowflake query which inturns calls the javascript function and store the value#}
    {%- set value_list = load_result('get_intervals_between') -%}

    {# check if the query returned any data#}
    {%- if value_list and value_list['data'] -%}

        {# Cast to String type#}
        {%- set result = value_list['data'][0][0]|string %}

        {# Query returns a string concatenated by a pipe. split using pipe to convert to list for iteration.#}
        {%- set result_list = result.split('||')%}

        {{log(result_list)}}
        {# return the values#}
        {{ return(result_list)}}

    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{%- endmacro %}
