
-- get the weekstart for the start_Date and week_end_date for the week_end_date
-- week starts on Sunday

{% macro get_weekend_ranges(input_date, start_date='NO') -%}
    {{ log("Macro -> get_weekend_ranges Parameters recieved: " ~ input_date ~ ", " ~ week_start) }}

    {% if input_date is not none -%}
      {% set dt = modules.datetime.datetime.strptime(input_date|string,'%Y%m%d') %}
      {% if dt.weekday()+1 == 7 -%}
        {% set week_start = dt %}
      {% else %}
        {% set week_start = dt - modules.datetime.timedelta(days=dt.weekday()+1) %}
      {% endif %}

      {% if start_date == 'YES' and week_start >= modules.datetime.datetime(2020, 2, 1, 0, 0, 0) %}
        {% set final_dt_obj = week_start %}
      {% elif start_date == 'YES' %}
        {% set final_dt_obj = modules.datetime.datetime(2020, 2, 1, 0, 0, 0) %}
      {% else %}
        {% set final_dt_obj = week_start + modules.datetime.timedelta(days=6) %}
      {% endif %}

      {{ return(modules.datetime.datetime.strftime(final_dt_obj,'%Y-%m-%d')) }}
    {% endif %}

{%- endmacro %}
