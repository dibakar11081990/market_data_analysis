{% macro return_s3_stage_latest_file(file_path) %}

    -- Remove leading @ and stage name, keep only prefix
    {% set path_no_stage = file_path.split('/', 1)[1] %}
    {% set folder_slash_count = path_no_stage.count('/') %}


    {% set query %}
        SELECT
            metadata$filename AS file_name,
            metadata$file_last_modified AS file_last_modified
        FROM {{ file_path }}
        WHERE DATE(metadata$file_last_modified) = CURRENT_DATE
        AND REGEXP_COUNT(metadata$filename, '/') = {{ folder_slash_count }} --ensure file is directly in this folder, not in nested subfolders
    {% endset %}

    {% set result_data = run_query(query) %}

    {% if execute %}
        {% if not result_data or result_data.rows | length == 0 %}
            {{ log("No files found for today in: " ~ file_path, info=True) }}
            {{ return(none) }}
        {% else %}
            {% set files = [] %}
            {% for row in result_data.rows %}
                {% set file_dict = {
                    "file_name": row[0],
                    "file_last_modified": row[1]
                } %}
                {% do files.append(file_dict) %}
            {% endfor %}

            {% set latest_file = files | sort(attribute='file_last_modified') | last %}

            {{ log("Latest file found: " ~ latest_file.file_name, info=True) }}
            {{ return(latest_file.file_name) }}
        {% endif %}
    {% endif %}

{% endmacro %}