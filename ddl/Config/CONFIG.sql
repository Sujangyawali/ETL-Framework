-- BATCH DAY
CREATE TABLE RDW_CFG.RDW_C_BATCH_DATE (
    BATCH_DATE DATE PRIMARY KEY,
    BATCH_ID BIGINT
);

-- EXTRACTION LOG
CREATE TABLE RDW_CFG.RDW_EXT_C_BATCH_LOG (
    BATCH_DATE      DATE,
    SCRIPT_ID             INTEGER,
    SCRIPT_NAME           VARCHAR(255) NOT NULL,
    STATUS             VARCHAR(50) NOT NULL,
    START_TIME         TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    END_TIME           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- EXTRACTION SCRIPTS
CREATE OR REPLACE TABLE RDW_CFG.RDW_EXT_C_BATCH_SCRIPTS (
    SCRIPT_ID           INTEGER,
    SCRIPT_NAME         VARCHAR(100) NOT NULL,
    SCRIPT_DESCRIPTION  VARCHAR(255),
    CREATED_AT          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE           BOOLEAN DEFAULT TRUE,
    SCHEDULE_FREQUENCY  VARCHAR(20) DEFAULT 'DAILY'
);

-- LOADING LOG
CREATE TABLE RDW_CFG.RDW_LD_C_BATCH_LOG (
    BATCH_DATE      DATE,
    SCRIPT_ID             INTEGER,
    SCRIPT_NAME           VARCHAR(255) NOT NULL,
    STATUS             VARCHAR(50) NOT NULL,
    START_TIME         TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    END_TIME           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- LOADING SCRIPTS
CREATE OR REPLACE TABLE RDW_CFG.RDW_LD_C_BATCH_SCRIPTS (
    SCRIPT_ID           INTEGER,
    SCRIPT_NAME         VARCHAR(100) NOT NULL,
    SCRIPT_DESCRIPTION  VARCHAR(255),
    CREATED_AT          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE           BOOLEAN DEFAULT TRUE,
    SCHEDULE_FREQUENCY  VARCHAR(20) DEFAULT 'DAILY'
);
