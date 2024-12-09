-- Databricks notebook source
TRUNCATE TABLE psas_di_qat.340b_gold.T_PHS_Accounts_Generics_Duplicates_01;
INSERT INTO psas_di_qat.340b_gold.T_PHS_Accounts_Generics_Duplicates_01
SELECT T_PHS_Accounts_Generics.Accounts
FROM psas_di_qat.340b_slvr.T_PHS_Accounts_Generics AS T_PHS_Accounts_Generics
GROUP BY T_PHS_Accounts_Generics.Accounts
HAVING Count(T_PHS_Accounts_Generics.Accounts)>1;

CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_PHS_Accounts_Generics_Duplicates_01 AS SELECT * FROM psas_di_qat.340b_gold.T_PHS_Accounts_Generics_Duplicates_01;


-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.340b_gold.T_PHS_Accounts_Generics_Coding_02;
INSERT INTO psas_di_qat.340b_gold.T_PHS_Accounts_Generics_Coding_02
SELECT DISTINCT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.ACCT_CLASSIFICATION, T_PHS_AUDIT.HRSA_TERM_DATE, T_PHS_AUDIT.PVP_PARTICIPANT_ID, T_PHS_AUDIT.PVP_PARTICIPATION_FLAG, T_PHS_AUDIT.PVP_ELIGIBILITY_DATE, T_PHS_AUDIT.STATUS, T_PHS_AUDIT.PVP_CODING, 
IFF(psas_di_qat.340b_gold.T_PHS_Accounts_Generics_Duplicates_01.Accounts is null,'','Yes') AS Duplicate
FROM (psas_di_qat.340b_slvr.T_PHS_Accounts_Generics AS T_PHS_Accounts_Generics INNER JOIN psas_di_qat.340b_gold.T_PHS_AUDIT_2 AS T_PHS_AUDIT ON T_PHS_Accounts_Generics.Accounts = T_PHS_AUDIT.CUST_ACCT_ID) LEFT JOIN psas_di_qat.340b_gold.T_PHS_Accounts_Generics_Duplicates_01 ON T_PHS_AUDIT.CUST_ACCT_ID = T_PHS_Accounts_Generics_Duplicates_01.Accounts
ORDER BY T_PHS_AUDIT.CUST_ACCT_ID;

 CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_PHS_Accounts_Generics_Coding_02 AS SELECT * FROM psas_di_qat.340b_gold.T_PHS_Accounts_Generics_Coding_02;

