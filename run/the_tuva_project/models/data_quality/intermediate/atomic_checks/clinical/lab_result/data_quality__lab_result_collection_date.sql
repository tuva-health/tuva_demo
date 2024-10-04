
  
    

        create or replace transient table tuva_synthetic.data_quality.lab_result_collection_date
         as
        (


SELECT
      m.data_source
    , coalesce(m.result_date,cast('1900-01-01' as date)) as source_date
    , 'LAB_RESULT' AS table_name
    , 'Lab Result ID' as drill_down_key
    , coalesce(lab_result_id, 'NULL') AS drill_down_value
    , 'COLLECTION_DATE' AS field_name
    , case
        when m.collection_date > cast(substring('2024-10-04 19:08:24.316902+00:00',1,10) as date) then 'invalid'
        when m.collection_date <= cast('1901-01-01' as date) then 'invalid'
        when m.collection_date > m.result_date then 'invalid'
        when m.collection_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.collection_date > cast(substring('2024-10-04 19:08:24.316902+00:00',1,10) as date) then 'future'
        when m.collection_date <= cast('1901-01-01' as date) then 'too old'
        when m.collection_date > m.result_date then 'Collection date after result date'
        else null
    end as invalid_reason
    , cast(collection_date as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.lab_result m
        );
      
  