{% macro generate_stack_object_name(project_name, stack_name) %}

  {# Generate Datamodel Name for the MPM #}
  {%- if project_name|lower == 'mpm' -%}

      {%- set STACK_NAME_LATEST = 'DEV.MPM.' + stack_name|upper %}
      {%- set STACK_NAME_LEGACY = 'DEV.MPM_LEGACY.' + stack_name|upper %}

      {%- if 'prd' in target.name -%}
        {%- set STACK_NAME_LATEST = 'PROD.MPM.' + stack_name|upper %}
        {%- set STACK_NAME_LEGACY = 'PROD.MPM_LEGACY.' + stack_name|upper %}
      {%- endif -%}

  {# Generate Datamodel Name for the MPM_EXCHANGE #}
  {%- elif project_name|lower == 'mpm_exchange' -%}
      {%- set STACK_NAME_LATEST = 'DEV.MPM_EXCHANGE.' + stack_name|upper %}
      {%- set STACK_NAME_LEGACY = 'DUMMY' %}
      {%- if 'prd' in target.name -%}
        {%- set STACK_NAME_LATEST = 'PROD.MPM_EXCHANGE.' + stack_name|upper %}
      {%- endif -%}

  {%- else -%}
      {%- set STACK_NAME_LATEST = 'DUMMY' %}
      {%- set STACK_NAME_LEGACY = 'DUMMY' %}

  {%- endif -%}

  {{- log('Macro -> generate_stack_object_name STACK_NAME_LATEST : ' ~ STACK_NAME_LATEST) -}}
  {{- log('Macro -> generate_stack_object_name STACK_NAME_LEGACY : ' ~ STACK_NAME_LEGACY) -}}

  {{ return({'latest':STACK_NAME_LATEST, 'legacy':STACK_NAME_LEGACY})}}

{% endmacro %}
