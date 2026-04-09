{% macro check_country_count() %}
    {% set result = run_query("SELECT COUNT(*) FROM BSM_ETL.MPM_INCR.MPM_USERFACT_METRICSCALC_STAGE WHERE CHARINDEX('/', VISIT_COUNTRY) > 0") %}
    {% if result and result.columns[0].values()[0] > 0 %}
        {{ return(True) }}
    {% else %}
        {{ return(False) }}
    {% endif %}
{% endmacro %}