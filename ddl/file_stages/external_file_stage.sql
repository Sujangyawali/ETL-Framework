CREATE STAGE RDW_STG.rdw_internal_stage
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

snowsql -a ap-south-1.snow -u sgyawali -d RDW_DWH_DB -s RDW_STG
Snowflake@123
SSS