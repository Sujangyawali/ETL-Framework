import os
from lib.logger import Logger
from config.env_setup import *
from lib.snowflake import SnowflakeDatabase
from lib.script_execution_logger import ScriptExeLog


script_name = os.path.basename(__file__)
script_name = os.path.splitext(script_name)[0]
log = Logger(script_name)

VIEWS_TABLES = {
    "SOURCE_VIEW": "V_STG_STRE_CLZ",
    "TEMP_TABLE_SOURCE_VIEW": "V_TMP_RDW_TMP_STRE_CLZ",
    "TEMP_TABLE": "RDW_TMP_STRE_CLZ",
    "TARGET_TABLE": "RDW_TGT_STRE_CLZ"
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
                                        LOC_ID
                                        ,LOC_DESC
                                        ,INCDNT_DATE
                                        ,INCDNT_TIME
                                        ,REPORT_DATE
                                        )
                                        SELECT
                                            LOC_ID
                                            ,LOC_DESC
                                            ,INCDNT_DATE
                                            ,INCDNT_TIME
                                            ,REPORT_DATE
                                        FROM {TEMP_VIEW_SCHEMA}.{VIEWS_TABLES['TEMP_TABLE_SOURCE_VIEW']}
                                        """
        sf_object.insert_into_table(VIEWS_TABLES['TEMP_TABLE_SOURCE_VIEW'], VIEWS_TABLES['TEMP_TABLE'], query_insert_into_temp_table)
        query_update_target_using_temp = f"""
                                    UPDATE {TARGET_SCHEMA}.{VIEWS_TABLES['TARGET_TABLE']} TGT 
                                    SET TGT.LOC_DESC = SRC.LOC_DESC,
                                    TGT.INCDNT_DATE = SRC.INCDNT_DATE,
                                    TGT.INCDNT_TIME = SRC.INCDNT_TIME,
                                    TGT.REPORT_DATE = SRC.REPORT_DATE,
                                    TGT.RCD_UPDATE_TS = CURRENT_TIMESTAMP
                                    FROM {TEMP_SCHEMA}.{VIEWS_TABLES['TEMP_TABLE']} SRC
                                    WHERE SRC.LOC_ID = TGT.LOC_ID 
                                    """
        sf_object.update_table(VIEWS_TABLES['TARGET_TABLE'], query_update_target_using_temp)
        query_insert_target_using_temp = f"""
                                        INSERT INTO {TARGET_SCHEMA}.{VIEWS_TABLES['TARGET_TABLE']} (
                                            LOC_ID
                                            ,LOC_KEY
                                            ,LOC_DESC
                                            ,INCDNT_DATE
                                            ,INCDNT_TIME
                                            ,REPORT_DATE
                                            ,RCD_INSERT_TS
                                            ,RCD_UPDATE_TS
                                        )
                                        SELECT
                                            LOC_ID
                                            ,(SELECT COALESCE(MAX(LOC_KEY),0) FROM {TARGET_SCHEMA}.{VIEWS_TABLES['TARGET_TABLE']}) + RANK()  OVER (ORDER BY LOC_ID) AS LOC_KEY
                                            ,LOC_DESC
                                            ,INCDNT_DATE
                                            ,INCDNT_TIME
                                            ,REPORT_DATE
                                            ,CURRENT_TIMESTAMP AS RCD_INSERT_TS
                                            ,CURRENT_TIMESTAMP AS RCD_UPDATE_TS
                                        FROM {TEMP_SCHEMA}.{VIEWS_TABLES['TEMP_TABLE']} SRC
                                        WHERE (LOC_ID)
                                        NOT IN (SELECT LOC_ID FROM {TARGET_SCHEMA}.{VIEWS_TABLES['TARGET_TABLE']})
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

