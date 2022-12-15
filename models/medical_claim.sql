{% if var('tuva_database') == 'tuva_claims_demo_sample' -%}

select * from {{ ref('medical_claim_sample') }}

{%- else -%}

select * from {{ source('demo','medical_claim_full') }}

{%- endif -%}