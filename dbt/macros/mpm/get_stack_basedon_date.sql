{% macro get_stack_basedon_date(start_date, end_date, stack) %}

  {{ log("Macro -> get_stack_basedon_date Parameters recieved: " ~ start_date ~ ", " ~ end_date ~ "," ~ stack.name) }}

  {# Condition where start date and end date is  passed. Find out which stack to pick. #}
  {%- if start_date is not none and end_date is not none -%}

      {%- set start_date = modules.datetime.datetime.strptime(start_date|string, '%Y%m%d') -%}
      {%- set end_date = modules.datetime.datetime.strptime(end_date|string, '%Y%m%d') -%}

      {%- set snowflake_three_part_name = 'DEV.MPM.' + stack.name %}
      {%- if ('prd' in target.name) -%}
          {%- set snowflake_three_part_name = 'PROD.MPM.' + stack.name %}
      {%- endif -%}

      {%- if start_date < modules.datetime.datetime(2020, 2, 1, 0, 0, 0) -%}
        {%- set snowflake_three_part_name = 'DEV.MPM_LEGACY_INCR.' + stack.name %}
        {%- if ('prd' in target.name) -%}
            {%- set snowflake_three_part_name = 'PROD.MPM_LEGACY.' + stack.name %}
        {%- endif -%}
      {%- endif -%}


  {# Condition where start date and end date is not passed. COnsider the new stack. #}
  {% else %}
      {%- set snowflake_three_part_name = 'DEV.MPM.' + stack.name %}
      {%- if ('prd' in target.name) -%}
          {%- set snowflake_three_part_name = 'PROD.MPM.'+ stack.name %}
      {%- endif -%}
  {%- endif -%}


  {{- log('Macro -> get_stack_basedon_date  final3partname: ' ~ snowflake_three_part_name) -}}

  {{- snowflake_three_part_name -}}

{% endmacro %}
