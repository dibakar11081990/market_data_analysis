{# This macro generates a list of dates between start_yyyymmdd and end_yyyymmdd inclusive #}
{% macro date_range_iterator(start_yyyymmdd, end_yyyymmdd) -%}

  {%- set start_dt = modules.datetime.datetime.strptime(start_yyyymmdd, '%Y%m%d') -%}
  {%- set end_dt = modules.datetime.datetime.strptime(end_yyyymmdd, '%Y%m%d') -%}

  {%- if end_dt < start_dt -%}
    {{ exceptions.raise_compiler_error(
      'END_DATE must be >= START_DATE. Got START_DATE=' ~ start_yyyymmdd ~ ' END_DATE=' ~ end_yyyymmdd
    ) }}
  {%- endif -%}

  {%- set days = (end_dt - start_dt).days -%}
  {%- set out = [] -%}

  {%- for i in range(days + 1) -%}
    {%- do out.append((start_dt + modules.datetime.timedelta(days=i)).strftime('%Y%m%d')) -%}
  {%- endfor -%}

  {{ return(out) }}
{%- endmacro %}
