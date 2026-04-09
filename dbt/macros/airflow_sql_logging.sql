{% macro log_sql_for_current_airflow_task(sql_content, message="EXECUTING SQL") %}
  {% set current_model = this.name %}
  {% set target_airflow_model = var('current_airflow_model', '') %}
  
  {% if target_airflow_model == current_model %}
    {{ log(">>>>> " ~ message ~ " FOR " ~ current_model ~ ": " ~ sql_content, info=True) }}
  {% endif %}
  
  {# {{ return(sql_content) }} #}
{% endmacro %}

{# Enhanced macro for selective logging based on tags #}
{% macro log_sql_for_tag_execution(sql_content, message="EXECUTING SQL") %}
  {% set current_model = this.name %}
  {% set target_tag = var('dbt_tag_filter', '') %}
  {% set enable_logging = var('enable_dbt_logging', 'false') %}
  
  {% if enable_logging == 'true' and target_tag != '' %}
    {# Get current model's tags #}
    {% set current_tags = model.config.tags or [] %}
    
    {# Check if the target tag is in current model's tags #}
    {% if target_tag in current_tags %}
      {{ log(">>>>> [TAG_EXECUTION: " ~ target_tag ~ "] " ~ message ~ " FOR " ~ current_model, info=True) }}
      {{ log(">>>>> SQL: " ~ sql_content, info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}


{# New macro for regular log messages that also respects tag filtering #}
{% macro log_for_tag_execution(message, log_level="info") %}
  {% set current_model = this.name %}
  {% set target_tag = var('dbt_tag_filter', '') %}
  {% set enable_logging = var('enable_dbt_logging', 'false') %}
  
  {% if enable_logging == 'true' and target_tag != '' %}
    {# Get current model's tags #}
    {% set current_tags = model.config.tags or [] %}
    
    {# Check if the target tag is in current model's tags #}
    {% if target_tag in current_tags %}
      {% if log_level == "info" %}
        {{ log(">>>>> [TAG_EXECUTION: " ~ target_tag ~ "] " ~ message ~ " FOR " ~ current_model, info=True) }}
      {% elif log_level == "debug" %}
        {{ log(">>>>> [TAG_EXECUTION: " ~ target_tag ~ "] DEBUG: " ~ message ~ " FOR " ~ current_model, info=True) }}
      {% else %}
        {{ log(">>>>> [TAG_EXECUTION: " ~ target_tag ~ "] " ~ message ~ " FOR " ~ current_model, info=True) }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}