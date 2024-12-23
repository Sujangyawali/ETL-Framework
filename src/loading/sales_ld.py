import os
from lib.logger import Logger
from config.env_setup import *
from lib.snowflake import SnowflakeDatabase
from lib.script_execution_logger import ScriptExeLog


script_name = os.path.basename(__file__)
script_name = os.path.splitext(script_name)[0]
log = Logger(script_name)

VIEWS_TABLES = {
    "SOURCE_VIEW": "V_STG_LND_SALES",
    "TEMP_TABLE_SOURCE_VIEW": "V_TMP_RDW_TMP_SALES",
    "TEMP_TABLE": "RDW_TMP_SALES",
    "TARGET_TABLE": "RDW_TMP_SALES"
}

sf_object = SnowflakeDatabase(log)
sf_object.connect()

script_exe_log_object = ScriptExeLog(sf_object, script_name, LOAD_SCRIPT_TABLE, LOAD_BATCH_LOG_TABLE)

if not script_exe_log_object.is_script_audited():
    sf_object.end_connection()
    log.close()
    raise Exception(f"Script is not audited into {LOAD_SCRIPT_TABLE} table, please audit the script first to run..")
if script_exe_log_object.get_script_exe_status():
    sf_object.end_connection()
    log.close()
    raise Exception(f"Log is existing for the Current Batch. Clear log record from {LOAD_BATCH_LOG_TABLE} and re-run the script.")
else:
    try:
        script_exe_log_object.insert_script_to_log()
        sf_object.turncate_table(TEMP_SCHEMA, VIEWS_TABLES['TEMP_TABLE'])
        query_insert_into_temp_table = f"""
                                        INSERT INTO {TEMP_SCHEMA}.{VIEWS_TABLES['TEMP_TABLE']} (
                                        TXTN_ID
                                        ,STR_KEY
                                        ,REG_KEY
                                        ,SLS_KEY
                                        ,ITEM_KEY
                                        ,TXTN_LINE_ID
                                        ,DAY
                                        ,EMP_ID
                                        ,CUSTMR_ID
                                        ,LOY_CRD_NUM
                                        ,PAYMNT_MTHD
                                        ,TXTN_STATUS
                                        ,RTRN_FLG
                                        ,PROMO_CDE
                                        ,RCPT_NUM
                                        ,SLS_QTY
                                        ,PRICE
                                        ,DISCNT
                                        ,TAX
                                        ,SLS_AMT
                                        )
                                        SELECT
                                            TXTN_ID
                                            ,STR_KEY
                                            ,REG_KEY
                                            ,SLS_KEY
                                            ,ITEM_KEY
                                            ,TXTN_LINE_ID
                                            ,DAY
                                            ,EMP_ID
                                            ,CUSTMR_ID
                                            ,LOY_CRD_NUM
                                            ,PAYMNT_MTHD
                                            ,TXTN_STATUS
                                            ,RTRN_FLG
                                            ,PROMO_CDE
                                            ,RCPT_NUM
                                            ,SLS_QTY
                                            ,PRICE
                                            ,DISCNT
                                            ,TAX
                                            ,SLS_AMT
                                        FROM {TEMP_VIEW_SCHEMA}.{VIEWS_TABLES['TEMP_TABLE_SOURCE_VIEW']}
                                        """
        sf_object.insert_into_table(VIEWS_TABLES['TEMP_TABLE_SOURCE_VIEW'], VIEWS_TABLES['TEMP_TABLE'], query_insert_into_temp_table)
        query_update_target_using_temp = f"""
                                    UPDATE {VIEWS_TABLES['TARGET_TABLE']} TGT 
                                    SET TGT.EMP_ID = SRC.EMP_ID,
                                    TGT.CUSTMR_ID = SRC.CUSTMR_ID,
                                    TGT.LOY_CRD_NUM = SRC.LOY_CRD_NUM,
                                    TGT.PAYMNT_MTHD = SRC.PAYMNT_MTHD,
                                    TGT.TXTN_STATUS = SRC.TXTN_STATUS,
                                    TGT.PROMO_CDE = SRC.PROMO_CDE,
                                    TGT.RCPT_NUM = SRC.RCPT_NUM,
                                    TGT.SLS_QTY = SRC.SLS_QTY,
                                    TGT.PRICE = SRC.PRICE,
                                    TGT.DISCNT = SRC.DISCNT,
                                    TGT.TAX = SRC.TAX,
                                    TGT.SLS_AMT = SRC.SLS_AMT
                                    TGT.RCD_UPDATE_TS = CURRENT_TIMESTAMP
                                    FROM {VIEWS_TABLES['TEMP_TABLE']} SRC
                                    WHERE SRC.TXTN_ID = TGT.TXTN_ID 
                                    AND SRC.TXTN_LINE_ID = TGT.TXTN_LINE_ID
                                    AND SRC.DAY = TGT.DAY
                                    AND SRC.STR_KEY = TGT.STR_KEY
                                    AND SRC.ITEM_KEY = TGT.ITEM_KEY
                                    AND SRC.RTRN_FLG = TGT.RTRN_FLG
                                    -- AND (COALESCE(TO_CHAR(SRC.EMP_ID),'0') <> COALESCE(TO_CHAR(TGT.EMP_ID),'0')  -- Prevent to update target record if exactly same record found between target and temp
                                    -- OR COALESCE(TO_CHAR(SRC.CUSTMR_ID),'0') <> COALESCE(TO_CHAR(TGT.CUSTMR_ID))
                                    """
        sf_object.update_table(VIEWS_TABLES['TARGET_TABLE'], query_update_target_using_temp)
        query_insert_target_using_temp = f"""
                                        INSERT INTO {VIEWS_TABLES['TARGET_TABLE']} (
                                            TXTN_ID
                                            ,TXTN_LINE_KEY
                                            ,STR_KEY
                                            ,REG_KEY
                                            ,CHNL_KEY
                                            ,ITEM_KEY
                                            ,TXTN_LINE_ID
                                            ,DAY
                                            ,EMP_ID
                                            ,CUSTMR_ID
                                            ,LOY_CRD_NUM
                                            ,PAYMNT_MTHD
                                            ,TXTN_STATUS
                                            ,RTRN_FLG
                                            ,PROMO_CDE
                                            ,RCPT_NUM
                                            ,SLS_QTY
                                            ,PRICE
                                            ,DISCNT
                                            ,TAX
                                            ,SLS_AMT
                                            ,RCD_INSERT_TS
                                            ,RCD_UPDATE_TS
                                        )
                                        SELECT
                                            TXTN_ID
                                            ,(SELECT MAX(TXN_LINE_KEY) FROM {VIEWS_TABLES['TARGET_TABLE']}) + RANK()  OVER (ORDER BY TXTN_LINE_ID,DAY,STR_KEY,ITEM_KEY,RTRN_FLG) AS TXTN_LINE_KEY
                                            ,STR_KEY
                                            ,REG_KEY
                                            ,SLS_KEY
                                            ,ITEM_KEY
                                            ,TXTN_LINE_ID
                                            ,DAY
                                            ,EMP_ID
                                            ,CUSTMR_ID
                                            ,LOY_CRD_NUM
                                            ,PAYMNT_MTHD
                                            ,TXTN_STATUS
                                            ,RTRN_FLG
                                            ,PROMO_CDE
                                            ,RCPT_NUM
                                            ,SLS_QTY
                                            ,PRICE
                                            ,DISCNT
                                            ,TAX
                                            ,SLS_AMT
                                            ,CURRENT_TIMESTAMP RCD_INSERT_TS
                                            ,CURRENT_TIMESTAMP RCD_UPDATE_TS
                                        FROM {VIEWS_TABLES['TEMP_TABLE']} SRC
                                        WHERE (TXTN_ID, TXTN_LINE_ID, DAY, STR_KEY, ITEM_KEY, RTRN_FLG)
                                        NOT IN (SELECT TXTN_ID, TXTN_LINE_ID, DAY, STR_KEY, ITEM_KEY, RTRN_FLG FROM {VIEWS_TABLES['TARGET_TABLE']}
                                        """
        sf_object.insert_into_table({VIEWS_TABLES['TEMP_TABLE']}, VIEWS_TABLES['TARGET_TABLE'], query_insert_target_using_temp)
        log.log_message(f"Loading completed")
        script_exe_log_object.update_success_status()
    except Exception as e:
        script_exe_log_object.update_error_status()
        log.log_message(f"Error loading data into target table: {e}")
        raise Exception(f"Error loading data into target table: {e}")
    finally:
        sf_object.end_connection()
        log.close()

