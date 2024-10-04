
  
    

        create or replace transient table tuva_synthetic.data_quality.mart_review__quality_measures_summary_long
         as
        (


select *
from tuva_synthetic.quality_measures.summary_long s
        );
      
  