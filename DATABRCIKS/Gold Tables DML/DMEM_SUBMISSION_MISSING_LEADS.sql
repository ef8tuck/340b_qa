-- Databricks notebook source
DELETE FROM psas_di_qat.`340b_slvr`.T_DMEM_LIST;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.`340b_slvr`.T_DMEM_LIST;
INSERT INTO psas_di_qat.`340b_slvr`.T_DMEM_LIST( CUST_ACCT_ID, CNTRCT_LEAD_ID, LEAD_NAME, LEAD_TYPE, PRTY_CONT, MARKUP_CONT, CONT_PREFD )
SELECT VPSD00.CUST_ACCT_ID, VPSD00.CNTRCT_LEAD_ID, VPSD00.LEAD_NAME, VPSD00.LEAD_TYPE, VPSD00.PRTY_CONT, VPSD00.MARKUP_CONT, VPSD00.CONT_PREFD
FROM (SELECT CA.CUST_ACCT_ID, CUST_ACCT_NAM, CE.CNTRCT_LEAD_ID, L.LEAD_NAME,
L.LEAD_TYPE, CE.PRTY_CONT, CE.MARKUP_CONT, CE.CONT_PREFD
FROM psas_di_qat.340b_brnz.t_iw_cust_acct CA INNER JOIN psas_di_qat.340b_brnz.t_dcon_cont_elig CE
ON CA.CUST_ACCT_ID  = CE.CUST_ACCT_ID 
INNER JOIN psas_di_qat.`340b_brnz`.T_CMS_LEAD L
ON CE.CNTRCT_LEAD_ID = L.CNTRC_LEAD_ID
INNER JOIN psas_di_qat.`340b_brnz`.T_DM_VSTX_CUST X ON CA.CUST_ACCT_ID = X.CUST_ACCT_ID
WHERE (X.MCK_KU_CHNL_TYP = '20' OR (X.MCK_KU_CHNL_TYP = '50' AND SUBSTRING(CA.CUST_ACCT_NAM, LENGTH(CA.CUST_ACCT_NAM) - 2, 3) = 'PHS')) AND CA.ACTIVE_CUST_IND = 'A' AND CA.CUST_CHN_ID <> '989')  VPSD00;

-- COMMAND ----------

DELETE FROM psas_di_qat.`340b_slvr`.T_DMEM_LIST AS A WHERE EXISTS( SELECT 1 FROM psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS B WHERE B.CUST_ACCT_ID = A.CUST_ACCT_ID AND (B.ACCT_CLASSIFICATION != '003' AND B.ACCT_CLASSIFICATION not in ('004','005')) );


-- COMMAND ----------

DELETE FROM psas_di_qat.`340b_slvr`.T_MT_LEAD_LIST;

---04 AQ_Lead_List
INSERT INTO psas_di_qat.`340b_slvr`.T_MT_LEAD_LIST ( CNTRCT_LEAD_ID, CNTRCT_LEAD_NAME, CNTRCT_LEAD_TYPE )
SELECT DISTINCT A.CNTRCT_LEAD_ID AS CNTRCT_LEAD_ID, A.LEAD_NAME AS CNTRCT_LEAD_NAME, A.LEAD_TYPE AS CNTRCT_LEAD_TYPE
FROM psas_di_qat.`340b_slvr`.T_DMEM_LIST AS A WHERE A.CNTRCT_LEAD_ID IS NOT NULL;

-- COMMAND ----------

DELETE FROM psas_di_qat.`340b_slvr`.T_MT_ACCOUNT_LIST;

---06 AQ_Account_List
INSERT INTO psas_di_qat.`340b_slvr`.T_MT_ACCOUNT_LIST ( CUST_ACCT_ID, CUST_ACCT_NAME, CUST_CHN_ID, ENTITY_TYPE, EXPANSION_ENTITY, ACCT_CLASSIFICATION )
SELECT A.CUST_ACCT_ID, A.CUST_ACCT_NAME, A.CUST_CHN_ID, A.ENTITY_TYPE, B.EXPANSION_ENTITY, A.ACCT_CLASSIFICATION 
FROM psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS A INNER JOIN psas_di_qat.`340b_slvr`.T_LUTL_ENTITY AS B ON A.ENTITY_TYPE = B.ENTITY_TYPE
WHERE (A.CUST_CHN_ID != '989') AND (A.ACCT_CLASSIFICATION='003' OR A.ACCT_CLASSIFICATION in ('004','005')) AND (UPPER(A.STATUS)='ACTIVE');

-- COMMAND ----------

DELETE FROM psas_di_qat.`340b_slvr`.T_MAINLEAD;

----08 AQ_MainLead
INSERT INTO psas_di_qat.`340b_slvr`.T_MAINLEAD(CUST_ACCT_ID, CNTRCT_LEAD_ID)
SELECT DISTINCT A.CUST_ACCT_ID, B.CNTRCT_LEAD_ID
FROM psas_di_qat.`340b_slvr`.T_MT_ACCOUNT_LIST AS A INNER JOIN psas_di_qat.`340b_slvr`.T_DMEM_LIST AS B ON A.CUST_ACCT_ID = B.CUST_ACCT_ID WHERE (B.CNTRCT_LEAD_ID='178018' OR B.CNTRCT_LEAD_ID='181126');



-- COMMAND ----------

MERGE INTO psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS A
USING psas_di_qat.`340b_slvr`.T_MAINLEAD AS B
ON A.CUST_ACCT_ID = B.CUST_ACCT_ID
WHEN MATCHED AND (A.MAIN_LEAD IS NULL OR A.MAIN_LEAD != B.CNTRCT_LEAD_ID) THEN
UPDATE SET A.MAIN_LEAD = B.CNTRCT_LEAD_ID;

-- COMMAND ----------

--# Data should be clean in T_MT_WAC_DMEM or not
--03 DQ_WAC_DMEM
DELETE  FROM psas_di_qat.`340b_slvr`.T_MT_WAC_DMEM;
   
--04 AQ_WAC_Accounts_DMEM_PVP
INSERT INTO psas_di_qat.`340b_slvr`.T_MT_WAC_DMEM ( CUST_ACCT_ID, LEAD, LEAD_NAME, ENTITY_TYPE )
SELECT A.CUST_ACCT_ID, A.LEAD,A.LEAD_NAME, A.ENTITY_TYPE
FROM (SELECT A.CUST_ACCT_ID, A.CUST_ACCT_NAME, B.LEAD, B.LEAD_NAME, A.PHS_340B_ID, PVP_PARTICIPATION_FLAG, A.ENTITY_TYPE
FROM  psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS A , (SELECT DISTINCT A.LEAD, A.LEAD_NAME, A.PVP_FLAG, A.WAC, A.EXPANSION
FROM psas_di_qat.`340b_slvr`.T_LUTL_PHS_LEADS AS A 
WHERE (A.LEAD != '293388' AND A.WAC ='Yes'))  B 
WHERE ((A.PVP_PARTICIPATION_FLAG='Y') AND A.ENTITY_TYPE IN ('CAN','DSH','PED') 
       AND (A.CUST_CHN_ID != '989') AND (A.ACCT_CLASSIFICATION='003') AND (UPPER(A.STATUS)='ACTIVE') 
       AND (CHARINDEX('A34', A.CUST_ACCT_NAME)>0))) A INNER JOIN (SELECT DISTINCT A.LEAD, A.LEAD_NAME, A.PVP_FLAG, A.WAC, A.EXPANSION
FROM psas_di_qat.`340b_slvr`.T_LUTL_PHS_LEADS AS A 
WHERE (A.LEAD != '293388' AND A.WAC ='Yes')) B ON (A.PVP_PARTICIPATION_FLAG = B.PVP_FLAG) AND (A.LEAD = B.LEAD);






-- COMMAND ----------

------------------05 Q_WAC_DMEM_Missing
TRUNCATE TABLE psas_di_qat.`340b_gold`.T_WAC_DMEM_MISSING_05;
INSERT INTO psas_di_qat.`340b_gold`.T_WAC_DMEM_MISSING_05  
SELECT DISTINCT A.CUST_ACCT_ID, C.CUST_ACCT_NAME, C.ZX_BLOCK, C.SALES_CURMTH, C.HRSA_START_DATE, C.HRSA_TERM_DATE, C.PVP_PARTICIPATION_FLAG AS PVP_FLAG, C.PVP_ELIGIBILITY_DATE, A.LEAD, A.LEAD_NAME, C.PHS_340B_ID, A.Expansion_Entity, A.ENTITY_TYPE, C.STATUS, C.CUST_CHN_ID FROM psas_di_qat.`340b_slvr`.T_MT_WAC_DMEM AS A LEFT JOIN psas_di_qat.`340b_slvr`.T_DMEM_LIST AS B ON ((A.LEAD = B.CNTRCT_LEAD_ID) AND (A.CUST_ACCT_ID = B.CUST_ACCT_ID)) INNER JOIN psas_di_qat.`340b_gold`.t_phs_audit_2 AS C ON A.CUST_ACCT_ID = C.CUST_ACCT_ID
WHERE (UPPER(C.STATUS)='ACTIVE' AND (C.CUST_CHN_ID != '989') AND (B.CUST_ACCT_ID IS NULL) AND (B.CNTRCT_LEAD_ID IS NULL))
ORDER BY A.CUST_ACCT_ID;


-- COMMAND ----------

DELETE FROM psas_di_qat.`340b_slvr`.T_MT_PHS_DMEM;

INSERT INTO psas_di_qat.`340b_slvr`.T_MT_PHS_DMEM ( CUST_ACCT_ID, LEAD, LEAD_NAME, Expansion_Entity, ENTITY_TYPE )
SELECT A.CUST_ACCT_ID, A.LEAD, A.LEAD_NAME, A.Expansion_Entity, A.ENTITY_TYPE
FROM (SELECT A.CUST_ACCT_ID, A.CUST_ACCT_NAME,REPLACE(UPPER(A.PVP_PARTICIPATION_FLAG),'TRUE','Y') AS PVP_PARTICIPATION_FLAG, B.LEAD, B.LEAD_NAME, A.PHS_340B_ID, CASE
    WHEN ENTITY_TYPE='RRC' THEN 'Y'
	WHEN ENTITY_TYPE='SCH' THEN 'Y'
	WHEN ENTITY_TYPE='CAN' THEN 'Y'
	WHEN ENTITY_TYPE='CAH' THEN 'Y'
	ELSE 'N'
END AS Expansion_Entity, A.ENTITY_TYPE
FROM psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS A, (SELECT DISTINCT A.LEAD, A.LEAD_NAME, A.PVP_Flag, A.PHS, A.Expansion
FROM psas_di_qat.`340b_slvr`.T_LUTL_PHS_LEADS AS A 
WHERE ((A.LEAD != '293388') AND (A.PHS='Yes'))) B 
WHERE ((A.CUST_CHN_ID != '989') AND (A.ACCT_CLASSIFICATION in ('004','005')) AND (UPPER(A.STATUS)='ACTIVE')))  A INNER JOIN 
(SELECT DISTINCT A.LEAD, A.LEAD_NAME, A.PVP_Flag, A.PHS, A.Expansion
FROM psas_di_qat.`340b_slvr`.T_LUTL_PHS_LEADS AS A 
WHERE ((A.LEAD != '293388') AND (A.PHS='Yes'))) B ON (A.LEAD = B.LEAD) AND ((A.Expansion_Entity = B.Expansion) OR (A.PVP_PARTICIPATION_FLAG = B.PVP_Flag));



-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PHS_DMEM_MISSING_06;
INSERT INTO psas_di_qat.`340b_gold`.T_PHS_DMEM_MISSING_06 
SELECT A.CUST_ACCT_ID, C.CUST_ACCT_NAME, C.ZX_BLOCK, C.SALES_CURMTH, C.HRSA_START_DATE, C.HRSA_TERM_DATE, C.PVP_PARTICIPATION_FLAG AS PVP_Flag, C.PVP_ELIGIBILITY_DATE, A.LEAD, A.LEAD_NAME, C.PHS_340B_ID, A.Expansion_Entity, A.ENTITY_TYPE, C.COMMENTS, C.STATUS
FROM psas_di_qat.`340b_slvr`.T_MT_PHS_DMEM AS A 
LEFT JOIN psas_di_qat.`340b_slvr`.T_DMEM_LIST AS B 
ON (A.LEAD = B.CNTRCT_LEAD_ID) AND (A.CUST_ACCT_ID = B.CUST_ACCT_ID) 
INNER JOIN psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS C 
ON A.CUST_ACCT_ID = C.CUST_ACCT_ID
WHERE ((UPPER(C.STATUS)='ACTIVE') AND (B.CUST_ACCT_ID IS NULL) AND (B.CNTRCT_LEAD_ID IS NULL))
ORDER BY A.CUST_ACCT_ID;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.`340b_gold`.T_WACA34_DMEM_SUBMISSION_06;
INSERT INTO psas_di_qat.340B_gold.T_WACA34_DMEM_SUBMISSION_06 
SELECT DISTINCT '' AS PPM, 'A' AS ADD, A.CUST_ACCT_ID, A.CUST_ACCT_NAME, B.LEAD, B.LEAD_NAME, '1' AS PRIORITY, 'Z' AS PREFERENCE, '' AS MARK_UP, '' AS CHANGE_BY, '' AS CHANGE_ON, 'N' AS BLOCK_ID, 'N' AS CONTRACT_BLOCK, A.SALES_CURMTH, A.HRSA_TERM_DATE, A.PVP_ELIGIBILITY_DATE, A.ZX_BLOCK, C.OPENED_FOR_RETURNS
FROM psas_di_qat.`340b_gold`.T_WAC_DMEM_MISSING_05 AS A 
INNER JOIN psas_di_qat.`340b_slvr`.T_LUTL_PHS_LEADS AS B 
ON (A.LEAD = B.LEAD) 
INNER JOIN psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS C 
ON A.CUST_ACCT_ID = C.CUST_ACCT_ID;

-- COMMAND ----------


TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PHS_DMEM_SUBMISSION_07;
INSERT INTO psas_di_qat.`340b_gold`.T_PHS_DMEM_SUBMISSION_07
SELECT DISTINCT A.CUST_ACCT_ID, A.CUST_ACCT_NAME, B.LEAD, B.LEAD_NAME, B.DMEM_Prio, B.DMEM_Pref, A.PHS_340B_ID, A.SALES_CURMTH, A.HRSA_START_DATE, A.HRSA_TERM_DATE, A.PVP_ELIGIBILITY_DATE, A.ZX_BLOCK, C.OPENED_FOR_RETURNS, C.ROUTE, C.STOP, C.RX_BILL_PLAN, C.NATIONAL_GRP_CD, C.NATIONAL_GRP_NAME
FROM psas_di_qat.`340b_gold`.T_PHS_DMEM_MISSING_06 AS A 
INNER JOIN psas_di_qat.`340b_slvr`.T_LUTL_PHS_LEADS AS B 
ON A.LEAD = B.LEAD 
INNER JOIN psas_di_qat.`340b_gold`.T_PHS_AUDIT_2  AS C 
ON A.CUST_ACCT_ID = C.CUST_ACCT_ID;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.`340b_gold`.T_LUQ_ACCOUNT_DMEM_ADHOC;
INSERT INTO psas_di_qat.`340b_gold`.T_LUQ_ACCOUNT_DMEM_ADHOC 
SELECT DISTINCT C.CUST_ACCT_ID, C.CUST_ACCT_NAME, B.CNTRCT_LEAD_ID, B.LEAD_NAME AS T_DMEM_LIST_LEAD_NAME, B.LEAD_TYPE, B.MARKUP_CONT, B.CONT_PREFD, D.LEAD_NAME AS LUTL_PHS_LEADS_LEAD_NAME 
FROM psas_di_qat.`340b_slvr`.T_LUQ_ACCOUNT AS A 
INNER JOIN psas_di_qat.`340b_slvr`.T_DMEM_LIST AS B 
ON A.Account = B.CUST_ACCT_ID 
INNER JOIN psas_di_qat.`340b_gold`.T_PHS_AUDIT_2  AS C 
ON A.Account = C.CUST_ACCT_ID 
LEFT JOIN psas_di_qat.`340b_slvr`.T_LUTL_PHS_LEADS AS D 
ON B.CNTRCT_LEAD_ID = D.LEAD;

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.`340b_gold`.T_LUQ_ACCOUNT_DMEM_SUBMISSION_WACA3;
INSERT INTO psas_di_qat.`340b_gold`.T_LUQ_ACCOUNT_DMEM_SUBMISSION_WACA3
SELECT DISTINCT B.PPM, B.ADD, B.CUST_ACCT_ID, B.CUST_ACCT_NAME, B.LEAD, B.LEAD_NAME, B.PRIORITY, B.PREFERENCE, B.MARK_UP, B.CHANGE_BY, B.CHANGE_ON, B.BLOCK_ID, B.CONTRACT_BLOCK, B.SALES_CURMTH, B.HRSA_TERM_DATE, B.PVP_ELIGIBILITY_DATE, B.ZX_BLOCK
FROM psas_di_qat.`340b_slvr`.T_LUQ_ACCOUNT AS A 
INNER JOIN psas_di_qat.`340b_gold`.T_WACA34_DMEM_SUBMISSION_06 AS B 
ON A.ACCOUNT = B.CUST_ACCT_ID;

-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_qat.`340b_gold`.V_PHS_DMEM_MISSING_06 AS SELECT * FROM psas_di_qat.`340b_gold`.T_PHS_DMEM_MISSING_06;
CREATE OR REPLACE VIEW psas_di_qat.`340b_gold`.V_WACA34_DMEM_SUBMISSION_06 AS SELECT * FROM psas_di_qat.`340b_gold`.T_WACA34_DMEM_SUBMISSION_06;
CREATE OR REPLACE VIEW psas_di_qat.`340b_gold`.V_PHS_DMEM_SUBMISSION_07 AS SELECT * FROM psas_di_qat.`340b_gold`.T_PHS_DMEM_SUBMISSION_07;
CREATE OR REPLACE VIEW psas_di_qat.`340b_gold`.V_LUQ_ACCOUNT_DMEM_ADHOC AS SELECT * FROM psas_di_qat.`340b_gold`.T_LUQ_ACCOUNT_DMEM_ADHOC;
CREATE OR REPLACE VIEW psas_di_qat.`340b_gold`.V_LUQ_ACCOUNT_DMEM_SUBMISSION_WACA3 AS SELECT * FROM psas_di_qat.`340b_gold`.T_LUQ_ACCOUNT_DMEM_SUBMISSION_WACA3;
