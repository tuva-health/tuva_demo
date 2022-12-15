{% if var('tuva_database') == 'tuva_claims_demo_sample' -%}

select * from {{ ref('eligibility_sample') }}

{%- else -%}

select * from {{ source('demo', 'eligibility_full') }}

{%- endif -%}