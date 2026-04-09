{% macro get_last_model_type(geo) %}

    {% set query %}
        SELECT MODEL_TYPE
                FROM {{this.database}}.D2B_LEAD_GEN.PERSONA_PILOT_TRACKING 
                WHERE GEO = '{{ geo }}'
                QUALIFY ROW_NUMBER() OVER (ORDER BY SNAPSHOT_DATE DESC) = 1
    {% endset %}
    
    {% set results = run_query(query) %}

    {% if results %}
        {% set table = results.columns %}
        {% set rows = results.rows %}
        {% if rows | length > 0 %}
            {% set final_result = rows[0][0] %}
        {% else %}
            {% set final_result = 'unknown' %}
        {% endif %}
    {% else %}
        {% set final_result = 'unknown' %}
    {% endif %}
   
    {{ return(final_result) }}
    
{% endmacro %}