{% macro set_query_tag() %}
  {% if target.type != 'snowflake' %}{% do return(none) %}{% endif %}

  {# Resolve node name/type/materialization safely across models/tests/seeds/snapshots #}
  {% set _name = (model.name if model is defined
                  else (node.name if node is defined
                        else (this.name if this is defined else 'unknown'))) %}

  {# working with node type (values: model, seed, snapshot, test, etc.) #}
  {% set _type = (model.resource_type if model is defined
                  else (node.resource_type if node is defined else 'sql')) %}


  {# working with materialization (values: table, incremental, etc.) #}
  {% set _mat = (
      model.config.materialized
        if model is defined and model is not none and model.config is defined and model.config.materialized is defined
      else (node.config.materialized
        if node is defined and node is not none and node.config is defined and node.config.materialized is defined
      else 'n/a')
  ) %}


  {# working with tags #}
  {% set _tags = [] %}
  {% if node is defined and node is not none and node.tags is not none %}
  {% set _tags = node.tags %}
  {% elif model is defined and model is not none
      and model.config is defined and model.config.tags is defined %}
  {% set _tags = model.config.tags %}
  {% endif %}


   {# working with file path #}
  {% set _orig = (
      node.original_file_path if (node is defined and node and node.original_file_path is defined)
      else (model.original_file_path if (model is defined and model and model.original_file_path is defined)
      else none)
  ) %}
  {% set _path = _orig if _orig else 'N/A' %}





  {# Base query tag structure #}
  {% set _raw = {
    "app": "dbt",
    "project": project_name,
    "env": target.name,
    "node": _name,
    "node_type": _type,                 
    "materialization": _mat,  
    "file_path": _path,
    "dbt_run_id": invocation_id,
    "dbt_tags": (_tags if _tags else ['N/A']),
    "team": env_var('TEAM','Mars GET')  
  } %}


{# working with extra model tags #}
{# using this the user can add its own custom tags into the query #}

{# in order to add custom tag on top of the default ones 
do this in the sql code/model: #}

  {# ---- Extensibility (custom keys) ----
       {{ config(
                meta={"query_tag_extra": {"airflow_dag":"marketing_loading"}}
                )
        }}
  #}


{% set _extra_model = {} %}

{% if node is defined and node is not none
      and node.config is defined and node.config.meta is not none
      and node.config.meta.get('query_tag_extra') %}
  {% set _extra_model = node.config.meta.get('query_tag_extra') %}

{% elif model is defined and model is not none
      and model.config is defined and model.config.meta is not none
      and model.config.meta.get('query_tag_extra') %}
  {% set _extra_model = model.config.meta.get('query_tag_extra') %}
{% endif %}






  {# Merge: base <- model extras #}
  {% set tag = _raw.copy() %}
  {% do tag.update(_extra_model) %}



{# final query tag #}
  {% do run_query("alter session set query_tag = '" ~ tojson(tag) ~ "'") %}
{% endmacro %}
