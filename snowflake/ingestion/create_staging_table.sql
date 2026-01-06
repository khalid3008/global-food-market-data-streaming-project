CREATE OR REPLACE STAGE Food_Market_DB.RAW.S3_STAGE
  URL = 's3://khalid-global-food-market-data-firehose-dest/data/'
  STORAGE_INTEGRATION = FM_S3_INT
  FILE_FORMAT = Food_Market_DB.RAW.CSV_FORMAT;

LIST @Food_Market_DB.RAW.S3_STAGE;