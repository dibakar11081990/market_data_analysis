/*
  COPY COMMAND FROM
*/

{%- macro issue_copy_stmt_fromfile(sql) -%}
    {{ log("ISSUING COPY STATEMENT " ~ this) }}
    {{ sql }}
{%- endmacro -%}

{% materialization copy_definition, adapter='snowflake' %}
    -- identifier.
    {%- set identifier = this -%}


    --------------------------------------------------------------------------------------------------------------------

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}


    --------------------------------------------------------------------------------------------------------------------

    -- build model
    {%- call statement('main') -%}
      {{ issue_copy_stmt_fromfile(sql) }}
    {%- endcall -%}


   --------------------------------------------------------------------------------------------------------------------

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [identifier]  }) }}

{%- endmaterialization %}
