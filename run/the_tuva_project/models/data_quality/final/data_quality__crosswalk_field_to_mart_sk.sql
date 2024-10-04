
  
    

        create or replace transient table tuva_synthetic.data_quality.crosswalk_field_to_mart_sk
         as
        (



with results as (

    select distinct
        table_name as input_layer_table_name
      , claim_type
      , field_name
      , cast(NULL as TEXT) AS mart_name
    from tuva_synthetic.data_quality.data_quality_detail

    union all

    select
        input_layer_table_name
      , claim_type
      , field_name
      , mart_name
    from tuva_synthetic.data_quality._value_set_crosswalk_field_to_mart

)

, final as (

    select
        input_layer_table_name
      , claim_type
      , field_name
      , mart_name
      , DENSE_RANK () OVER (ORDER BY input_layer_table_name, claim_type, field_name) as table_claim_type_field_sk
	, '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
    from results
    group by
        input_layer_table_name
      , claim_type
      , field_name
      , mart_name

)

select * from final
        );
      
  