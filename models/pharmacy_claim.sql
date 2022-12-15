{% if var('tuva_database') == 'tuva_claims_demo_sample' -%}

select * from {{ ref('pharmacy_claim_sample') }}

{%- else -%}

select * from {{ source('demo','pharmacy_claim_full') }}

{%- endif -%}