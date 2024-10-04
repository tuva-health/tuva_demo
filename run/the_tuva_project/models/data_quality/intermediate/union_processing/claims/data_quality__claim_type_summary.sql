
  
    

        create or replace transient table tuva_synthetic.data_quality.claim_type_summary
         as
        (select
    null as data_source,
    null as claim_type,
    null as claim_count,
    null as paid_amount
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
limit 0
        );
      
  