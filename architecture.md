
  # Architecture & Unity Catalog
  - dv_dev {datavault - development catalog}
  - dv_prod {datavault - production catalog}
    - **Schemas Structure**
      - `dv_{env}_landing` {reference to storage via volume}
  |------------------To Revise this part -------------------------------------------------------| 
      #### Observation, we manage the SCD, Landing and staging in same place with applied RDV
      - `dv_{env}_bronze_{source}` - {raw ingested from s3} - delta parquet files (SCD0)
      - `dv_{env}_stg_{source}` - Standardized staging + hashing [Todo]
      - `dv_{env}_rdv_{source}` - Raw Data Vault Tables [HUB, LINK, SATELLITE]
  |---------------------------------------------------------------------------------------------|
      - `dv_{env}_bdv_{source}` - Business Data Vault Tables [HUB, LINK, SATELLITE] with Point in Time (PIT), Bridges, Derived Stats
      - `dv_{env}_dm_{source}`  - Data Marts - (Star Schemas: Dims/Facts)
      - `dv_{env}_ops`          - Operations - logging, audit, DQ results, config
     
## 1.2 Data Dictionaries inside Unity Catalog for Data Vault 
  - "Native" UC Documentation (however, this has some contraints) - Todo - Thorough analysis
  - "Central querable dictionary table"
      - `dv_{env}_ops.data_dictionary` [metadata of table in documenation]
