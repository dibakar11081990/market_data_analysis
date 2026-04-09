

{% macro forty_days_ago() %}

    {% set forty_days_ago = (modules.datetime.date.today() - modules.datetime.timedelta(days=40)).strftime('%Y-%m-%d') %}

    {{ return(forty_days_ago) }}
    
{% endmacro %}