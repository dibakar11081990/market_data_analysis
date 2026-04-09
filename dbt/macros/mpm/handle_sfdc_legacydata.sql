{% macro handle_sfdc_legacydata(legacy_prep_table, stack) %}


  {{- log("Macro -> handle_sfdc_legacydata:" ~ legacy_prep_table ~ stack) -}}


  {%- if 'prd' in target.name -%}
    {%- set stack_object_name='PROD.MPM_LEGACY.' ~ stack -%}
  {%- else -%}
    {%- set stack_object_name='DEV.MPM_LEGACY.' ~ stack -%}
  {%- endif -%}

  {# Function to delete the data from the legacy stack. #}
  {%- call statement('delete_legacy_data', fetch_result=False) %}
      {{- log('Macro -> handle_sfdc_legacydata: Deleting SFDC Legacy data if any') -}}
      DELETE FROM {{stack_object_name}} WHERE CONV_DATASOURCE IN  ({{ var('DSID_CONV_SFDC_OPPORTUNITY') }}, {{ var('DSID_CONV_SFDC_LEADS') }})
  {%- endcall -%}

  {# Function to insert the new data from the legacy stack. #}
  {%- call statement('insert_legacy_data', fetch_result=False) %}
      {{- log('Macro -> handle_sfdc_legacydata: Inserting SFDC Legacy data if any') -}}
      INSERT INTO {{stack_object_name}} SELECT * from {{legacy_prep_table}}
  {%- endcall -%}

  {%- set dummmy = load_result('delete_legacy_data') -%}
  {%- set dummmy = load_result('insert_legacy_data') -%}

  {{ return(none) }}

{% endmacro %}
