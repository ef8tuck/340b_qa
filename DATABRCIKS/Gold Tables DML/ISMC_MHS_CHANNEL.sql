-- Databricks notebook source
-- MAGIC %md
-- MAGIC This uses  data source from PSAS_DB

-- COMMAND ----------

TRUNCATE TABLE psas_di_qat.340b_gold.T_ISMC_MHS_AUDIT;
INSERT INTO psas_di_qat.340b_gold.T_ISMC_MHS_AUDIT
SELECT CA.CUST_ACCT_ID, CA.CUST_ACCT_NAM, CA.ACCT_CHN_ID, V.MCK_KU_CHNL_TYP, V.MCK_KU_ACCT_CLAS, 
CA.NATL_GRP_CD, M.NATL_GRP_NAM 
FROM psas_di_qat.340b_brnz.t_iw_cust_acct CA INNER JOIN psas_di_qat.340b_brnz.t_dm_vstx_cust V 
ON CA.CUST_ACCT_ID = V.CUST_ACCT_ID 
INNER JOIN psas_di_qat.340b_gold.mhs_rna_cust_acct M 
ON M.CUST_ACCT_ID = CA.CUST_ACCT_ID 
WHERE ACTIVE_CUST_IND = 'A'  
AND CA.ACCT_CHN_ID <> '000' 
AND ((V.MCK_KU_ACCT_CLAS = '001' AND V.MCK_KU_CHNL_TYP = '20') OR 
(V.MCK_KU_ACCT_CLAS IN ('003','004','005') AND V.MCK_KU_CHNL_TYP = '30'));



-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_qat.340b_gold.V_ISMC_MHS_AUDIT as select * from psas_di_qat.340b_gold.T_ISMC_MHS_AUDIT;
