-- macros/get_external_columns.sql
{% macro get_external_table_columns(db_name, external_table_name) %}

  {% set db_properties = get_dbproperties(db_name) %}
  {% set database = db_properties['database'] %}
  {% set schema = db_properties['schema'] %}

    {% set query %}
        select value
        from {{ database }}.{{ schema }}.{{ external_table_name }}
        where row_number = 1
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set header_json = results.columns[0].values()[0] %}
        {% set parsed = fromjson(header_json) %}
        {% set mapped = parsed.items() | list %}

            {# {{ log("External table extracted columns:\n" ~ mapped, info=True) }} #}

        {{ return(mapped) }}
    {% else %}
        {{ return([]) }}
    {% endif %}

{% endmacro %}