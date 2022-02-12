{%- macro generate_schema_name_for_env(custom_schema_name=none) -%}
    {%- set default_schema = target.schema -%}
    {%- if (( target.name == 'prod' or
              target.name == 'dev' or
              target.name == 'default') and
              (custom_schema_name is not none)) -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro -%}

{%- macro generate_schema_name(schema_name, node) -%}
    {{ generate_schema_name_for_env(schema_name) }}
{%- endmacro -%}