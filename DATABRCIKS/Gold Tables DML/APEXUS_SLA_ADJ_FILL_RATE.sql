-- Databricks notebook source
TRUNCATE TABLE psas_di_qat.340b_gold.T_APEXUS_CODE_KEY_01;
INSERT INTO psas_di_qat.340b_gold.T_APEXUS_CODE_KEY_01
SELECT DISTINCT APCOKEY.Lead_No AS Lead_No, APCOKEY.Org_ID_Seq, APCOKEY.Contract_Name AS Contract_Name, APCOKEY.Material, APCOKEY.EAN_UPC, APCOKEY.Material_Description AS Material_Description, APCOKEY.Original_Valid_From AS Original_Valid_From, APCOKEY.Original_Valid_To AS Original_Valid_To, APCOKEY.Bid_Price AS Bid_Price, APCOKEY.GPO_Chbk_Ref_No AS GPO_Chbk_Ref_No
FROM psas_di_qat.`340b_slvr`.T_APEXUS_CODE_KEY AS APCOKEY 
WHERE (APCOKEY.Lead_No Is Not Null);

CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_APEXUS_CODE_KEY_01 AS SELECT * FROM psas_di_qat.340b_gold.T_APEXUS_CODE_KEY_01;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.340b_gold.T_APEXUS_WAC_ACCT_BY_LEAD_04;
INSERT INTO psas_di_qat.340b_gold.T_APEXUS_WAC_ACCT_BY_LEAD_04
SELECT DISTINCT APWAACBYLE.Lead_No AS Lead_No, APWAACBYLE.Org_ID_Seq, APWAACBYLE.Contract_Name AS Contract_Name, APWAACBYLE.Material, APWAACBYLE.EAN_UPC, APWAACBYLE.Material_Description AS Material_Description, APWAACBYLE.Original_Valid_From AS Original_Valid_From, APWAACBYLE.Original_Valid_To AS Original_Valid_To, APWAACBYLE.Bid_Price AS Bid_Price, APWAACBYLE.GPO_Chbk_Ref_No AS GPO_Chbk_Ref_No
FROM psas_di_qat.`340b_slvr`.T_APEXUS_WAC_ACCT_BY_LEAD AS APWAACBYLE;

CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_APEXUS_WAC_ACCT_BY_LEAD_04 AS SELECT * FROM psas_di_qat.340b_gold.T_APEXUS_WAC_ACCT_BY_LEAD_04;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.340b_gold.T_APEXUS_PHS_ACCT_BY_LEAD_02;
INSERT INTO psas_di_qat.340b_gold.T_APEXUS_PHS_ACCT_BY_LEAD_02
SELECT DISTINCT APPHACBYLE.Lead_No AS Lead_No, APPHACBYLE.Org_ID_Seq, APPHACBYLE.Contract_Name AS Contract_Name, APPHACBYLE.Material, APPHACBYLE.EAN_UPC, APPHACBYLE.Material_Description AS Material_Description, APPHACBYLE.Original_Valid_From AS Original_Valid_From, APPHACBYLE.Original_Valid_To AS Original_Valid_To, APPHACBYLE.Bid_Price AS Bid_Price, APPHACBYLE.GPO_Chbk_Ref_No AS GPO_Chbk_Ref_No
FROM psas_di_qat.`340b_slvr`.T_APEXUS_PHS_ACCT_BY_LEAD AS APPHACBYLE 
WHERE (APPHACBYLE.Lead_No Is Not Null);

CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_APEXUS_PHS_ACCT_BY_LEAD_02 AS SELECT * FROM psas_di_qat.340b_gold.T_APEXUS_PHS_ACCT_BY_LEAD_02;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.340b_gold.T_WAC_ACCOUNTS_05;
INSERT INTO psas_di_qat.340b_gold.T_WAC_ACCOUNTS_05
SELECT VAPCOKEY.Lead_No, VAPCOKEY.Org_ID_Seq, VAPCOKEY.Contract_Name, VAPCOKEY.Material, VAPCOKEY.EAN_UPC, VAPCOKEY.Material_Description, VAPCOKEY.Original_Valid_From, VAPCOKEY.Original_Valid_To, VAPCOKEY.Bid_Price, VAPCOKEY.GPO_Chbk_Ref_No
FROM psas_di_qat.340b_gold.T_APEXUS_CODE_KEY_01 AS VAPCOKEY 
UNION SELECT VAPWAACBYLE.Lead_No, VAPWAACBYLE.Org_ID_Seq, VAPWAACBYLE.Contract_Name, VAPWAACBYLE.Material, VAPWAACBYLE.EAN_UPC, VAPWAACBYLE.Material_Description, VAPWAACBYLE.Original_Valid_From, VAPWAACBYLE.Original_Valid_To, VAPWAACBYLE.Bid_Price, VAPWAACBYLE.GPO_Chbk_Ref_No
FROM psas_di_qat.340b_gold.T_APEXUS_WAC_ACCT_BY_LEAD_04 AS VAPWAACBYLE;

CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_WAC_ACCOUNTS_05 AS SELECT * FROM psas_di_qat.340b_gold.T_WAC_ACCOUNTS_05;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.340b_gold.T_PHS_ACCOUNTS_03;

INSERT INTO psas_di_qat.340b_gold.T_PHS_ACCOUNTS_03 
SELECT 
    VAPCOKEY.Lead_No, 
    VAPCOKEY.Org_ID_Seq, 
    VAPCOKEY.Contract_Name, 
    VAPCOKEY.Material, 
    VAPCOKEY.EAN_UPC, 
    VAPCOKEY.Material_Description, 
    VAPCOKEY.Original_Valid_From AS Original_Valid_From, 
    VAPCOKEY.Original_Valid_To AS Original_Valid_To, 
    VAPCOKEY.Bid_Price, 
    VAPCOKEY.GPO_Chbk_Ref_No
FROM 
    psas_di_qat.340b_gold.T_APEXUS_CODE_KEY_01 AS VAPCOKEY 

UNION 

SELECT 
    VAPPHACBYLE.Lead_No, 
    VAPPHACBYLE.Org_ID_Seq, 
    VAPPHACBYLE.Contract_Name, 
    VAPPHACBYLE.Material, 
    VAPPHACBYLE.EAN_UPC, 
    VAPPHACBYLE.Material_Description, 
    VAPPHACBYLE.Original_Valid_From AS Original_Valid_From, 
    VAPPHACBYLE.Original_Valid_To AS Original_Valid_To, 
    VAPPHACBYLE.Bid_Price, 
    VAPPHACBYLE.GPO_Chbk_Ref_No
FROM 
    psas_di_qat.340b_gold.T_APEXUS_PHS_ACCT_BY_LEAD_02 AS VAPPHACBYLE;

    CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_PHS_ACCOUNTS_03 AS SELECT * FROM psas_di_qat.340b_gold.T_PHS_ACCOUNTS_03;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.340b_gold.T_WAC_ACCT_BY_LEAD_CHECK_04B;
INSERT INTO psas_di_qat.340b_gold.T_WAC_ACCT_BY_LEAD_CHECK_04B
SELECT DISTINCT VAPWAACBYLE.Lead_No, LUPHLEAD.LEAD_Unformatted, LUPHLEAD.WAC
FROM psas_di_qat.340b_gold.T_APEXUS_WAC_ACCT_BY_LEAD_04 AS VAPWAACBYLE LEFT JOIN psas_di_qat.340b_slvr.T_LUTL_PHS_LEADS AS LUPHLEAD ON VAPWAACBYLE.Lead_No = LUPHLEAD.LEAD_Unformatted
ORDER BY VAPWAACBYLE.Lead_No;

CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_WAC_ACCT_BY_LEAD_CHECK_04B AS SELECT * FROM psas_di_qat.340b_gold.T_WAC_ACCT_BY_LEAD_CHECK_04B;


-- COMMAND ----------


TRUNCATE TABLE psas_di_qat.340b_gold.T_PHS_ACCT_BY_LEAD_CHECK_02B;
INSERT INTO psas_di_qat.340b_gold.T_PHS_ACCT_BY_LEAD_CHECK_02B
SELECT DISTINCT VAPPHACBYLE.Lead_No, LUPHLEAD.LEAD_Unformatted, LUPHLEAD.PHS
FROM  psas_di_qat.`340b_gold`.T_APEXUS_PHS_ACCT_BY_LEAD_02  AS VAPPHACBYLE LEFT JOIN  psas_di_qat.`340b_slvr`.T_LUTL_PHS_LEADS AS LUPHLEAD ON VAPPHACBYLE.Lead_No = LUPHLEAD.LEAD_Unformatted
ORDER BY VAPPHACBYLE.Lead_No;

CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_PHS_ACCT_BY_LEAD_CHECK_02B AS SELECT * FROM psas_di_qat.340b_gold.T_PHS_ACCT_BY_LEAD_CHECK_02B;
