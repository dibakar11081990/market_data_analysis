{% macro store_test_results() %}
    {% if execute and flags.WHICH == 'test' %}
        {% set test_results = results %}

        {% if 'prd' in target.name %}
            {% set database_name = 'PROD' %}
        {% else %}
            {% set database_name = 'DEV' %}
        {% endif %}

        {% for result in test_results %}
            {% set sql %}
                INSERT INTO {{ database_name }}.FFM.DBT_TEST_RESULTS 
                (test_id, test_name, model_name, status, error_message, execution_time, timestamp)
                VALUES (
                    {{ loop.index }}, 
                    '{{ result.node.name }}', 
                    '{{ result.node.depends_on.nodes | join(",") }}', 
                    '{{ result.status.value }}',
                    '{{ result.message | replace("'", "''") }}',
                    {{ result.execution_time }},
                    CURRENT_TIMESTAMP
                );
            {% endset %}
            {% do run_query(sql) %}
        {% endfor %}
    {% endif %}
{% endmacro %}