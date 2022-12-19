{# logic to use the sample seed data or full demo data -#}

{% if var('full_data_override') == false -%}

select * from {{ ref('eligibility_sample') }}

{%- else -%}

select * from {{ source('demo', 'eligibility') }}

{%- endif %}