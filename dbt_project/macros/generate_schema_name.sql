{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}

{#
  Vaikimisi dbt lisaks schema nimele target.schema eesliite (nt "public_marts").
  See makro kirjutab käitumise üle nii, et kasutatakse otse custom_schema_name väärtust.
  Tulemus: marts.dim_indeksid (mitte public_marts.dim_indeksid)
#}
