-- BATCH TABLE
INSERT INTO RDW_CFG.RDW_C_BATCH_DATE VALUES (CURRENT_DATE, 1);

-- FIRST EXTRACTION SCRIPT
INSERT INTO RDW_CFG.RDW_EXT_C_BATCH_SCRIPTS  
(SCRIPT_ID, SCRIPT_NAME, SCRIPT_DESCRIPTION)
VALUES (1, 'sales_ex',' Script to extract sales data from MySql database.');

-- FELLOW EXTRACTION SCRIPTS
INSERT INTO RDW_CFG.RDW_EXT_C_BATCH_SCRIPTS  
(SCRIPT_ID, SCRIPT_NAME, SCRIPT_DESCRIPTION)
SELECT MAX(SCRIPT_ID) + 1 SCRIPT_ID ,
'category_extraction' AS SCRIPT_NAME,
'Extraction script for Product category from Rapid API' AS SCRIPT_DESCRIPTION
FROM RDW_CFG.RDW_EXT_C_BATCH_SCRIPTS
;
INSERT INTO RDW_CFG.RDW_EXT_C_BATCH_SCRIPTS  
(SCRIPT_ID, SCRIPT_NAME, SCRIPT_DESCRIPTION)
SELECT MAX(SCRIPT_ID) + 1 SCRIPT_ID ,
'store_closure_ex' AS SCRIPT_NAME,
'Extraction script to fetch file from SFTP having information regarding closed stores' AS SCRIPT_DESCRIPTION
FROM RDW_CFG.RDW_EXT_C_BATCH_SCRIPTS
;