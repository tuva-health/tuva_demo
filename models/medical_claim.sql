{# logic to use the sample seed data or full demo data -#}

{% if var('full_data_override') == false -%}

select * from {{ ref('medical_claim_sample') }}

{%- else -%}

select * from {{ source('demo','medical_claim') }}

{%- endif %}