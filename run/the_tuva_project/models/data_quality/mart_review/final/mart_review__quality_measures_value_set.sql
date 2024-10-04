
  
    

        create or replace transient table tuva_synthetic.data_quality.mart_review__quality_measures_value_set
         as
        (

select *    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.quality_measures._value_set_measures p
        );
      
  