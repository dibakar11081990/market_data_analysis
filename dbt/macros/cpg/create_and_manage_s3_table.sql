
{% macro create_and_manage_s3_table(s3_location) %}
    -- Query to check if the table is existed or not
    {% set query %}
    SELECT count(*) as result
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = '{{ this.name }}'
        AND TABLE_SCHEMA = '{{ this.schema }}'
        AND TABLE_CATALOG = '{{ this.database }}'
    {% endset %}
    {% set result_data = run_query(query) %}
    {% if execute %}
    {% set final_result = result_data.columns[0][0] %}
    {% endif %}
    -- end
    --  Query to columns names from S3 json file
    {% set get_columns_query %}
            SELECT 
            ARRAY_TO_STRING(ARRAY_FLATTEN(ARRAY_AGG(DISTINCT OBJECT_KEYS(PARSE_JSON($1[0])))), ',') AS field_names
            FROM {{s3_location}}
            (FILE_FORMAT => 'ADP_WORKSPACES.AKP_PRIVATE.FF_JSON')
            {% if final_result != 0 %}
            WHERE metadata$filename not in (select distinct file_name from {{this}})
            {% endif %}
        {% endset %}
        {% set get_columns_query_result = run_query(get_columns_query) %}
        {% if execute %}
            {% set all_columns = get_columns_query_result.columns[0][0] %}
        {% endif %}
    -- end
    -- Logic to create table if not available
    {% if final_result == 0 %}  -- 0 means table is not available
        
        {% if all_columns is not none and all_columns | length > 0 %}
            {% set result_string = all_columns | replace(',', ' TEXT, ') %}
            {% set create_table_string = '"' ~ result_string ~ '"' %}
            
            {% call statement('create_table') %}
                CREATE TABLE {{this}} (
                    {{result_string}} TEXT,
                    FILE_NAME TEXT
                )

            {% endcall %}
            
        {% endif %}
    {% else %}
        {% set get_new_cols %}
            SELECT LISTAGG(CONCAT(VALUE, ' TEXT'), ',')
            FROM
            (SELECT 
            COLUMN_NAME, ORDINAL_POSITION
            FROM 
                INFORMATION_SCHEMA.COLUMNS
            WHERE  TABLE_NAME = '{{ this.name }}'
                AND TABLE_SCHEMA= '{{ this.schema }}'
                AND TABLE_CATALOG = '{{ this.database }}') a
            RIGHT JOIN 
            (SELECT VALUE FROM (SELECT SPLIT('{{all_columns}}', ',') as a), LATERAL FLATTEN(input => a)) b
            ON UPPER(COLUMN_NAME) = UPPER(b.VALUE)
            WHERE COLUMN_NAME IS NULL
        {% endset %}
        {% set run_get_new_cols = run_query(get_new_cols) %}
        {% if execute %}
            {% set new_cols = run_get_new_cols.columns[0][0] %}
        {% endif %}
        {% if new_cols is not none and new_cols != '' and new_cols != ' ' %}
            {% call statement('alter_table') %}
                    ALTER TABLE {{this}} ADD {{new_cols}}
            {% endcall %} 
        {% endif %}
    {% endif %}
    -- end create table logic

    -- To retrun all columns from table, helps for copy command
    {% set get_table_cols_query %}
    SELECT LISTAGG(CONCAT('$1:', COALESCE(VALUE, COLUMN_NAME), '::STRING AS ', COLUMN_NAME), ', ')
    WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    FROM
    (SELECT 
    COLUMN_NAME, ORDINAL_POSITION
    FROM 
        INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_NAME = '{{ this.name }}'
        AND TABLE_SCHEMA= '{{ this.schema }}'
        AND TABLE_CATALOG = '{{ this.database }}'
        AND COLUMN_NAME != 'FILE_NAME') as a
    LEFT JOIN 
    (SELECT VALUE FROM (SELECT SPLIT('{{all_columns}}', ',') as a), LATERAL FLATTEN(input => a)) as b
    ON UPPER(COLUMN_NAME) = UPPER(VALUE)
    {% endset %}
    {% set run_get_table_cols_query = run_query(get_table_cols_query) %}
    {% if execute %}
        {% set copy_definition_query = run_get_table_cols_query.columns[0][0] %}
    {% endif %}
    {{ return(copy_definition_query) }}
    
{% endmacro %}



------ check table macro
{% macro check_table_availability() %}

    {% set query %}
        SELECT count(*) as result
        FROM {{ this.database }}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{{ this.name }}'
            AND TABLE_SCHEMA = '{{ this.schema }}'
    {% endset %}
    {% set result_data = run_query(query) %}
    {% if execute %}
        {% set final_result = result_data.columns[0][0] %}
        {{ return(final_result) }}
    {% endif %}
    
{% endmacro %}



{% macro get_stage_new_files(stage_path) %}

    {% set stage_query %}
        SELECT LISTAGG(DISTINCT CONCAT('''',SPLIT(METADATA$FILENAME, '/')[3], ''''), ',') as files FROM 
        {{stage_path}}
        WHERE METADATA$FILENAME NOT IN (SELECT DISTINCT FILE_NAME FROM {{this}})
    {% endset %}
    {% set stage_result = run_query(stage_query) %}
    {% if execute %}
        {% set final_result = stage_result.columns[0][0] %}
        {{ return(final_result) }}
    {% endif %}

{% endmacro %}


{% macro read_schema_from_json() %}
    -- To read the latest json format from landing table
    {% set read_json_query %}
        BEGIN;
        SELECT DATA FROM DEV.CONTENT_GEN_AI.USER_CHAT_HISTORY_LANDING
        WHERE data:role = 'user'
        ORDER BY INSERT_DATE DESC LIMIT 1 ;
        COMMIT;
    {% endset %}
    {% set result = run_query(read_json_query) %}
    {% if execute %}
        {% set main_json = fromjson(result.columns[0][0] | string) %}   
    {% else %}  
         {% set main_json = {} %}
    {% endif %}
    -------
    --- read all columns and it's type from the json object
    {% set final_result = get_keys_from_dict(main_json) %}
    
    {% set keys_list = [] %}
    {% set re = modules.re %}

    ---- Generate SQL template for each column
    {% for key, value in final_result.items() %}
        {% do keys_list.append( 'data:' ~ key ~ '::' ~ value ~ ' AS ' ~ (re.sub(':|-', '_', key)) | replace('"', '') ) %}
    {% endfor %}

    --- Additional metadate columns
    {% do keys_list.extend(['FILE_NAME AS FILE_NAME', 'CURRENT_TIMESTAMP() AS INSERT_DATE']) %}

    {% set table_value = check_table_availability_with_parameters(this.database, this.schema, this.name) %}
    {% set new_cols = [] %}
    {% set current_cols = [] %}

    {% if table_value == 0 %}
        {% set current_cols = keys_list %}
    {% else %}
     
    --- This else part to read the last executed query from inforamtion schema
    --- and compare with the latest format to find new columns/fomrat changes
        {% set get_table_columns  %}
            BEGIN;
            SELECT REPLACE(COMMENT, '\\', '') FROM {{ this.database }}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{{ this.name }}'
            AND TABLE_SCHEMA = '{{ this.schema }}';
            COMMIT;
        {% endset %}
        {% set get_table_columns_result = run_query(get_table_columns) %}
        {% if execute %}
            {% set current_cols = get_table_columns_result.columns[0][0] %}
            {% if current_cols is not none and current_cols != '' and current_cols != ' ' %}
                {% set current_cols = current_cols.split(', ') %}
                {% set new_cols = list_diff(current_cols, keys_list) %}  
            {% else %}
                {% set current_cols = keys_list %}
            {% endif %}
        {% endif %}
        ---- end else part     
    {% endif %}
    -- {% do log(current_cols) %}
    -- {% do log(new_cols) %}

    -- Adding new columns to the existed table if new columns available
    {% if new_cols %}
        {% for value in new_cols %}
            {% set alter_query %}
                ALTER TABLE {{this}} ADD COLUMNS {{ value.split(' AS ')[1] }} {{ value.split(' AS ')[0].split('::')[1] }}
            {% endset %}
        {% endfor %}
        {% do current_cols.extend(new_cols) %}
    {% endif %}
    -- end if
    -- {% do log(keys_list) %}
    -- {% do log(current_cols) %}
    {{ return(current_cols | join(', ')) }}

{% endmacro %}



--- This macro to read column names from main and sub jsons
{% macro get_keys_from_dict(input_dict, main_key='') %}
    {% set my_dict = {} %}
        {% for key, value in input_dict.items() %}
            {% if value is string %}
                {% set column_type = 'string' %}
                {% do my_dict.update({main_key  ~ '"' ~ key ~ '"': column_type}) %}
            {% elif value is number %}
                {% set column_type = 'string' %}
                {% do my_dict.update({main_key  ~ '"' ~ key ~ '"': column_type}) %}
            {% elif value is mapping %}
                {% set sub_dict_result = get_keys_from_dict(value, main_key= main_key ~ key ~ ':')  %}
                {% do my_dict.update(sub_dict_result) %}
            {% elif value is iterable %}
                {% set column_type = 'array' %}
                {% do my_dict.update({main_key  ~ '"' ~ key ~ '"': column_type}) %}
            {% else %}
                {% set column_type = 'string' %}
                {% do my_dict.update({main_key  ~ '"' ~ key ~ '"': column_type}) %}
            {% endif %}
            
        {% endfor %}
        {{ return(my_dict) }}
{% endmacro %}

--- To checkt the table is available or not
{% macro check_table_availability_with_parameters(db_name, schema_name, table_name) %}
    {% set query %}
        BEGIN;
        SELECT count(*) as result
        FROM {{ db_name }}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{{ table_name }}'
            AND TABLE_SCHEMA = '{{ schema_name }}';
        COMMIT;
    {% endset %}
    {% set result_data = run_query(query) %}
    {% if execute %}
        {% set final_result = result_data.columns[0][0] %}
        -- {% do log(final_result) %}
        {{ return(final_result) }}
    {% endif %}
    
{% endmacro %}

--- This macro to compare two lists and find differences
{% macro list_diff(current_cols, keys_list) %}
    {% set intersection = [] %}
    {% for item in keys_list %}
        {% if item not in current_cols %}
            {% do intersection.append(item) %}
        {% endif %}
    {% endfor %}
    -- {% do log(intersection) %}
    {{ return(intersection) }}
{% endmacro %}