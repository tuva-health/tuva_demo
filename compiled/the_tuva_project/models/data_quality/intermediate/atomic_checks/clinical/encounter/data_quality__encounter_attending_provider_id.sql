

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'ATTENDING_PROVIDER_ID' AS field_name
    , case when term.npi is not null then 'valid'
          when m.attending_provider_id is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.attending_provider_id is not null and term.npi is null
          then 'Attending provider ID does not join to Terminology provider table'
          else null end as invalid_reason
    , cast(attending_provider_id as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m
left join tuva_synthetic.terminology.provider term on m.attending_provider_id = term.npi