CREATE OR REPLACE VIEW RDW_STG_V.V_STG_STRE_CLZ(
    LOC_ID
    ,LOC_DESC
    ,INCDNT_DATE
    ,INCDNT_TIME
    ,REPORT_DATE
) AS ( 
        SELECT
            LOC_ID
            ,LOC_NAME
            ,INCDNT_DATE
            ,INCDNT_TIME
            ,REPORT_DATE
        FROM RDW_LND.LND_STORE_CLOSURE
    );
    