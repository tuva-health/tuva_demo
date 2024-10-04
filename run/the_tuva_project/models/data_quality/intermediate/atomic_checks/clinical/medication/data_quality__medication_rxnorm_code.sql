
  
    

        create or replace transient table tuva_synthetic.data_quality.medication_rxnorm_code
         as
        (


SELECT
      m.data_source
    , coalesce(m.dispensing_date,cast('1900-01-01' as date)) as source_date
    , 'MEDICATION' AS table_name
    , 'Medication ID' as drill_down_key
    , coalesce(medication_id, 'NULL') AS drill_down_value
    , 'RXNORM_CODE' as field_name
    , case when term.rxcui is not null then 'valid'
           when m.rxnorm_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.rxnorm_code is not null and term.rxcui is null
           then 'RX norm code does not join to Terminology rxnorm_to_atc table'
           else null end as invalid_reason
    , cast(rxnorm_code as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.medication m
left join tuva_synthetic.terminology.rxnorm_to_atc term on m.rxnorm_code = term.rxcui
        );
      
  