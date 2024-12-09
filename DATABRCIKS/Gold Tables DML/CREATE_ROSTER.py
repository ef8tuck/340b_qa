# Databricks notebook source
# MAGIC %sql
# MAGIC DELETE FROM psas_di_qat.`340b_slvr`.T_MPB_PHS_WAC;
# MAGIC
# MAGIC --02 AQ_MPB_PHS_WAC
# MAGIC INSERT INTO psas_di_qat.`340b_slvr`.T_MPB_PHS_WAC(CUSTOMER) SELECT CUSTOMER FROM psas_di_qat.`340b_brnz`.T_MPB_PHS_WAC_TEMP WHERE CUSTOMER Is Not Null And CUSTOMER<>'';
# MAGIC
# MAGIC --03 UQ_FormatAccountNumber
# MAGIC UPDATE psas_di_qat.`340b_slvr`.T_MPB_PHS_WAC SET CUSTOMER = Concat(Left('000000',6-Len(CUSTOMER)), CUSTOMER) WHERE CUSTOMER Is Not Null And CUSTOMER <> '';

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS 
# MAGIC SELECT CUST_ACCT_ID, CUST_NAME, STORE_NUM, DEA_FAMILY, MARKETING_CAMPAIGN, HOME_DC_ID, DEA_NUM, HIN_BASE_CD, HIN_DEPT_CD, HIN_LOCATION_CD, HIN_NUM, IFF(LEFT(ID_340B,4)='0000','',ID_340B) AS 340B_ID, ATTENTION_NAME_DELY, ADDR_LINE_1_DELY, ADDR_LINE_2_DELY, CITY_NAME_DELY, STATE_CD_DELY, ZIP_CD_DELY, ATTENTION_NAME_INV, ADDR_LINE_1_INV, ADDR_LINE_2_INV, CITY_NAME_INV, STATE_CD_INV, ZIP_CD_INV, CUST_CHAIN_ID, CUST_CHAIN_NAME, NATIONAL_GROUP_CD, NATIONAL_GROUP_NAME, NATIONAL_SUB_GROUP_CD, NATIONAL_SUB_GROUP_NAME, REGION_NUM, REGION_NAME, DISTRICT_NUM, DISTRICT_NAME, RX_BILL_PLAN_CD, BUSINESS_TYPE_CD, DISTRIBUTION_CHANNEL, SALES_TERRITORY_ID, PRIMARY_CUST_ID, IFF(PROMO_SPEC_PRC_1<>'',LEFT(PROMO_SPEC_PRC_1,2),'') AS PROMO1, IFF(PROMO_SPEC_PRC_2<>'',LEFT(PROMO_SPEC_PRC_2,2),'') AS PROMO2, IFF(PROMO_SPEC_PRC_3<>'',LEFT(PROMO_SPEC_PRC_3,2),'') AS PROMO3, IFF(PROMO_SPEC_PRC_4<>'',LEFT(PROMO_SPEC_PRC_4,2),'') AS PROMO4, IFF(PROMO_SPEC_PRC_5<>'',LEFT(PROMO_SPEC_PRC_5,2),'') AS PROMO5, IFF(PROMO_SPEC_PRC_6<>'',LEFT(PROMO_SPEC_PRC_6,2),'') AS PROMO6, ACCOUNT_CLASSIFICATION, ACCOUNT_CLASSIFICATION_DESCRIPTION
# MAGIC FROM psas_di_qat.`340b_slvr`.T_PHS_ACCOUNTS;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_Promo1;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_Promo1 
# MAGIC SELECT DISTINCT T_PHS_ACCOUNTS.Cust_Acct_ID, T_PHS_ACCOUNTS.Promo1
# MAGIC FROM psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS INNER JOIN psas_di_qat.`340b_slvr`.T_TPV AS T_TPV ON T_PHS_ACCOUNTS.Promo1 = T_TPV.TPV;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_Promo2;
# MAGIC INSERT INTO  psas_di_qat.`340b_gold`.T_Promo2
# MAGIC SELECT DISTINCT T_PHS_ACCOUNTS.Cust_Acct_ID, T_PHS_ACCOUNTS.Promo2
# MAGIC FROM  psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS INNER JOIN  psas_di_qat.`340b_slvr`.T_TPV AS T_TPV ON T_PHS_ACCOUNTS.Promo2 = T_TPV.TPV;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_Promo3;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_Promo3
# MAGIC SELECT DISTINCT T_PHS_ACCOUNTS.Cust_Acct_ID, T_PHS_ACCOUNTS.Promo3
# MAGIC FROM psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS INNER JOIN psas_di_qat.`340b_slvr`.T_TPV AS T_TPV ON T_PHS_ACCOUNTS.Promo3 = T_TPV.TPV;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 13 Q_Promo4
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_Promo4;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_Promo4
# MAGIC SELECT DISTINCT T_PHS_ACCOUNTS.Cust_Acct_ID, T_PHS_ACCOUNTS.Promo4
# MAGIC FROM psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS INNER JOIN psas_di_qat.`340b_slvr`.T_TPV AS T_TPV ON T_PHS_ACCOUNTS.Promo4 = T_TPV.TPV;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_Promo5;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_Promo5
# MAGIC SELECT DISTINCT T_PHS_ACCOUNTS.Cust_Acct_ID, T_PHS_ACCOUNTS.Promo5
# MAGIC FROM psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS INNER JOIN psas_di_qat.`340b_slvr`.T_TPV AS T_TPV ON T_PHS_ACCOUNTS.Promo5 = T_TPV.TPV;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_Promo6;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_Promo6 
# MAGIC SELECT DISTINCT T_PHS_ACCOUNTS.Cust_Acct_ID, T_PHS_ACCOUNTS.Promo6
# MAGIC FROM psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS INNER JOIN psas_di_qat.`340b_slvr`.T_TPV AS T_TPV ON T_PHS_ACCOUNTS.Promo6 = T_TPV.TPV;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PromoIDs;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PromoIDs
# MAGIC SELECT T_PHS_ACCOUNTS.Cust_Acct_ID, T_Promo1.Promo1 || T_Promo2.Promo2 || T_Promo3.Promo3 || T_Promo4.Promo4 || T_Promo5.Promo5 || T_Promo6.Promo6 AS PromoIDs
# MAGIC FROM (((((psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS LEFT JOIN psas_di_qat.`340b_gold`.T_Promo1 ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_Promo1.Cust_Acct_ID) LEFT JOIN psas_di_qat.`340b_gold`.T_Promo2 ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_Promo2.Cust_Acct_ID) LEFT JOIN psas_di_qat.`340b_gold`.T_Promo3 ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_Promo3.Cust_Acct_ID) LEFT JOIN psas_di_qat.`340b_gold`.T_Promo4 ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_Promo4.Cust_Acct_ID) LEFT JOIN psas_di_qat.`340b_gold`.T_Promo5 ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_Promo5.Cust_Acct_ID) LEFT JOIN psas_di_qat.`340b_gold`.T_Promo6 ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_Promo6.Cust_Acct_ID
# MAGIC WHERE (((T_Promo1.Promo1 || T_Promo2.Promo2 || T_Promo3.Promo3 || T_Promo4.Promo4 || T_Promo5.Promo5 || T_Promo6.Promo6) Is Not Null));

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PromoIDs_17;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PromoIDs_17
# MAGIC SELECT T_PromoIDs.Cust_Acct_ID, Left(PromoIDs,2) AS 3rd_Party_Vendor,Len(PromoIDs) AS StringSize, IFF(StringSize>2,SUBSTR(PromoIDs,3,2),'') AS Secondary_Vendor, 
# MAGIC IFF(StringSize>4,SUBSTR(PromoIDs,5,2),'') AS Other_Vendors
# MAGIC FROM psas_di_qat.`340b_gold`.T_PromoIDs;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PromoIDs_FINAL;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PromoIDs_FINAL
# MAGIC SELECT T_PromoIDs_17.Cust_Acct_ID, IFF(T_PromoIDs_17.`3rd_Party_Vendor`='M2' And T_PromoIDs_17.Secondary_Vendor<>'',T_PromoIDs_17.Secondary_Vendor,T_PromoIDs_17.`3rd_Party_Vendor`) AS `3rd_Party_Vendor1`, IFF(T_PromoIDs_17.`3rd_Party_Vendor`='M2' And T_PromoIDs_17.Secondary_Vendor<>'',T_PromoIDs_17.`3rd_Party_Vendor`,T_PromoIDs_17.Secondary_Vendor) AS Secondary_Vendor1, T_PromoIDs_17.Other_Vendors
# MAGIC FROM psas_di_qat.`340b_gold`.T_PromoIDs_17;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PromoIDs_FINAL2;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PromoIDs_FINAL2
# MAGIC SELECT T_PromoIDs_FINAL.Cust_Acct_ID, IFF(Secondary_Vendor1='CA',Secondary_Vendor1,"3rd_Party_Vendor1") AS 3rd_Party_Vendor, IFF(Secondary_Vendor1='CA',"3rd_Party_Vendor1",Secondary_Vendor1) AS Secondary_Vendor, T_PromoIDs_FINAL.Other_Vendors
# MAGIC FROM psas_di_qat.`340b_gold`.T_PromoIDs_FINAL;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_Remove_585;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_Remove_585
# MAGIC SELECT T_PHS_ACCOUNTS.Cust_Acct_ID, T_PHS_ACCOUNTS.Cust_Chain_ID, T_PHS_ACCOUNTS.National_Group_Cd
# MAGIC FROM psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS
# MAGIC WHERE (((T_PHS_ACCOUNTS.Cust_Chain_ID)='585') AND ((T_PHS_ACCOUNTS.National_Group_Cd)='0497' Or (T_PHS_ACCOUNTS.National_Group_Cd)='0498' Or (T_PHS_ACCOUNTS.National_Group_Cd)='0499'));

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_Members(CUST_ACCT_ID, CUST_NAME, PVP_MEMBER_NAME, COVERED_ENTITY_PRIMARY, PHARMACY_NAME, CONTRACT_PHARMACY, CONTRACT_PHARMACY_PRIMARY, DEA_FAMILY, HOME_DC_ID, DEA_NUM, HIN_BASE_CD, HIN_DEPT_CD, HIN_LOCATION_CD, HIN_NUM, 340B_ID, PVP_PARTICIPANT_ID, PVP_PARTICIPATION_FLAG, PVP_EFFECTIVE_DATE, PVP_EXPIRATION_DATE, ATTENTION_NAME_DELY, ADDR_LINE_1_DELY, ADDR_LINE_2_DELY, CITY_NAME_DELY, STATE_CD_DELY, ZIP_CD_DELY, ATTENTION_NAME_INV, ADDR_LINE_1_INV, ADDR_LINE_2_INV, CITY_NAME_INV, STATE_CD_INV, ZIP_CD_INV, CUST_CHAIN_ID, CUST_CHAIN_NAME, NATIONAL_GROUP_CD, NATIONAL_GROUP_NAME, NATIONAL_SUB_GROUP_CD, NATIONAL_SUB_GROUP_NAME, REGION_NUM, REGION_NAME, DISTRICT_NUM, DISTRICT_NAME, RX_BILL_PLAN_CD, BUSINESS_TYPE_CD, DISTRIBUTION_CHANNEL, SALES_TERRITORY_ID, PRIMARY_CUST_ID, THIRD_PARTY_VENDOR, SECONDARY_VENDOR, OTHER_VENDORS, ACCOUNT_CLASSIFICATION, ACCOUNT_CLASSIFICATION_DESCRIPTION, RECEIVED_DSCSA_AGREEMENT, EXTENDED_TO_MPB_YN) 
# MAGIC SELECT DISTINCT T_PHS_ACCOUNTS.Cust_Acct_ID, T_PHS_ACCOUNTS.Cust_Name, T_PHS_AUDIT.COVERED_ENTITY_NAME AS PVP_Member_Name, T_Pharmacy_Type_Data.CE_Primary AS Covered_Entity_Primary, T_Pharmacy_Type_Data.Pharmacy_Name AS Pharmacy_Name, 
# MAGIC IFF(Pharmacy_Type='Contract','Y','N') AS CONTRACT_PHARMACY, 
# MAGIC T_Pharmacy_Type_Data.CP_Primary AS CONTRACT_PHARMACY_PRIMARY, 
# MAGIC T_PHS_ACCOUNTS.DEA_Family AS DEA_FAMILY , T_PHS_ACCOUNTS.Home_DC_ID AS HOME_DC_ID , T_PHS_ACCOUNTS.DEA_Num AS DEA_NUM, 
# MAGIC T_PHS_ACCOUNTS.HIN_Base_Cd AS HIN_BASE_CD , T_PHS_ACCOUNTS.HIN_Dept_Cd AS HIN_DEPT_CD , T_PHS_ACCOUNTS.HIN_Location_Cd AS HIN_LOCATION_CD, 
# MAGIC T_PHS_ACCOUNTS.HIN_Num AS HIN_NUM , Trim("340B_ID") AS 340B_ID, 
# MAGIC IFF(PVP_PARTICIPATION_FLAG='Y',T_PHS_AUDIT.PVP_PARTICIPANT_ID,'') AS PVP_Participant_ID,IFF(PVP_PARTICIPATION_FLAG='Y','Y','N') AS PVP_PARTICIPATION_FLAG, 
# MAGIC IFF(PVP_PARTICIPATION_FLAG='Y',PVP_ELIGIBILITY_DATE,NULL) AS PVP_EFFECTIVE_DATE , T_PHS_AUDIT.PVP_EXPIRATION_DATE AS PVP_EXPIRATION_DATE, 
# MAGIC T_PHS_ACCOUNTS.ATTENTION_NAME_DELY AS ATTENTION_NAME_DELY , T_PHS_ACCOUNTS.ADDR_LINE_1_DELY AS ADDR_LINE_1_DELY , 
# MAGIC T_PHS_ACCOUNTS.ADDR_LINE_2_DELY AS ADDR_LINE_2_DELY ,T_PHS_ACCOUNTS.CITY_NAME_DELY AS CITY_NAME_DELY , T_PHS_ACCOUNTS.STATE_CD_DELY AS STATE_CD_DELY , 
# MAGIC T_PHS_ACCOUNTS.ZIP_CD_DELY AS ZIP_CD_DELY , T_PHS_ACCOUNTS.ATTENTION_NAME_INV AS ATTENTION_NAME_INV , T_PHS_ACCOUNTS.ADDR_LINE_1_INV AS ADDR_LINE_1_INV , 
# MAGIC T_PHS_ACCOUNTS.ADDR_LINE_2_INV AS ADDR_LINE_2_INV , T_PHS_ACCOUNTS.CITY_NAME_INV AS CITY_NAME_INV , T_PHS_ACCOUNTS.STATE_CD_INV AS STATE_CD_INV , 
# MAGIC T_PHS_ACCOUNTS.ZIP_CD_INV AS ZIP_CD_INV , T_PHS_ACCOUNTS.Cust_Chain_ID AS CUST_CHAIN_ID , T_PHS_ACCOUNTS.Cust_Chain_Name AS CUST_CHAIN_NAME , 
# MAGIC T_PHS_ACCOUNTS.National_Group_Cd AS NATIONAL_GROUP_CD , T_PHS_ACCOUNTS.National_Group_Name AS NATIONAL_GROUP_NAME , T_PHS_ACCOUNTS.National_Sub_Group_Cd AS NATIONAL_SUB_GROUP_CD , 
# MAGIC T_PHS_ACCOUNTS.National_Sub_Group_Name AS NATIONAL_SUB_GROUP_NAME , 
# MAGIC T_PHS_ACCOUNTS.Region_Num AS REGION_NUM , T_PHS_ACCOUNTS.Region_Name AS REGION_NAME , 
# MAGIC T_PHS_ACCOUNTS.District_Num AS DISTRICT_NUM , T_PHS_ACCOUNTS.District_Name AS DISTRICT_NAME , T_PHS_ACCOUNTS.Rx_Bill_Plan_Cd AS RX_BILL_PLAN_CD , 
# MAGIC T_PHS_ACCOUNTS.Business_Type_Cd AS BUSINESS_TYPE_CD , T_PHS_ACCOUNTS.Distribution_Channel AS DISTRIBUTION_CHANNEL , 
# MAGIC T_PHS_ACCOUNTS.Sales_Territory_ID AS SALES_TERRITORY_ID , T_PHS_ACCOUNTS.Primary_Cust_ID AS PRIMARY_CUST_ID , 
# MAGIC T_PromoIDs_FINAL2.3rd_Party_Vendor AS THIRD_PARTY_VENDOR , T_PromoIDs_FINAL2.Secondary_Vendor AS SECONDARY_VENDOR , 
# MAGIC T_PromoIDs_FINAL2.Other_Vendors AS OTHER_VENDORS , T_PHS_ACCOUNTS.Account_Classification AS ACCOUNT_CLASSIFICATION , 
# MAGIC T_PHS_ACCOUNTS.Account_Classification_Description AS ACCOUNT_CLASSIFICATION_DESCRIPTION , 
# MAGIC T_PHS_AUDIT.DSCSA_RECEIVED AS RECEIVED_DSCSA_AGREEMENT ,IFF(T_MPB_PHS_WAC.MPB_EXTENDED = NULL,'NO','YES') AS EXTENDED_TO_MPB_YN
# MAGIC FROM (((((psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS INNER JOIN psas_di_qat.`340b_GOLD`.T_PHS_AUDIT_2 AS T_PHS_AUDIT ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_PHS_AUDIT.CUST_ACCT_ID) LEFT JOIN psas_di_qat.`340b_slvr`.T_MPB_PHS_WAC AS T_MPB_PHS_WAC ON 
# MAGIC T_PHS_ACCOUNTS.Cust_Acct_ID = T_MPB_PHS_WAC.CUSTOMER) LEFT JOIN psas_di_qat.`340b_gold`.T_Remove_585 ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_Remove_585.Cust_Acct_ID) 
# MAGIC LEFT JOIN psas_di_qat.`340b_gold`.T_PromoIDs_FINAL2 ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_PromoIDs_FINAL2.Cust_Acct_ID) 
# MAGIC LEFT JOIN psas_di_qat.`340b_slvr`.T_LUTL_NON_ORDERING_ACCOUNTS AS T_LUTL_NON_ORDERING_ACCOUNTS ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_LUTL_NON_ORDERING_ACCOUNTS.Cust_Acct_ID) 
# MAGIC LEFT JOIN psas_di_qat.`340b_slvr`.T_Pharmacy_Type_Data AS T_Pharmacy_Type_Data ON T_PHS_ACCOUNTS.Cust_Acct_ID = T_Pharmacy_Type_Data.Cust_Acct
# MAGIC WHERE (((T_PHS_ACCOUNTS.Cust_Chain_ID) Not In ('000','770')) AND ((T_PHS_ACCOUNTS.Business_Type_Cd)<>'31') AND ((T_PHS_ACCOUNTS.Account_Classification) in ('4','5')) AND ((T_Remove_585.Cust_Chain_ID) Is Null) AND ((T_LUTL_NON_ORDERING_ACCOUNTS.ID) Is Null));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE VIEW psas_di_qat.`340b_gold`.V_PHS_MEMBERSHIPROSTER_MEMBERS
# MAGIC -- AS
# MAGIC -- SELECT * FROM psas_di_qat.`340b_slvr`.T_PHS_MEMBERSHIPROSTER_MEMBERS;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_Members AS target
# MAGIC USING psas_di_qat.`340b_brnz`.t_opace_daily_public AS source
# MAGIC ON target.340B_ID = source.340B_ID
# MAGIC WHEN MATCHED AND target.PVP_Member_Name IS NULL THEN
# MAGIC   UPDATE SET target.PVP_Member_Name = source.Entity_Name;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_WAC;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_WAC (CUST_ACCT_ID, CUST_NAME, 340B_ID, PVP_PARTICIPANT_ID, PVP_PARTICIPATION_FLAG, PVP_MEMBER_NAME, PVP_EFFECTIVE_DATE, PVP_EXPIRATION_DATE, STORE_NUM, DEA_FAMILY, MARKETING_CAMPAIGN, HOME_DC_ID, DEA_NUM, HIN_BASE_CD, HIN_DEPT_CD, HIN_LOCATION_CD, HIN_NUM, ATTENTION_NAME_DELY, ADDR_LINE_1_DELY, ADDR_LINE_2_DELY, CITY_NAME_DELY, STATE_CD_DELY, ZIP_CD_DELY, ATTENTION_NAME_INV, ADDR_LINE_1_INV, ADDR_LINE_2_INV, CITY_NAME_INV, STATE_CD_INV, ZIP_CD_INV, CUST_CHAIN_ID, CUST_CHAIN_NAME, NATIONAL_GROUP_CD, NATIONAL_GROUP_NAME, NATIONAL_SUB_GROUP_CD, NATIONAL_SUB_GROUP_NAME, REGION_NUM, REGION_NAME, DISTRICT_NUM, DISTRICT_NAME, RX_BILL_PLAN_CD, BUSINESS_TYPE_CD, DISTRIBUTION_CHANNEL, SALES_TERRITORY_ID, PRIMARY_CUST_ID, 3RD_PARTY_VENDOR, SECONDARY_VENDOR, OTHER_VENDORS, ACCOUNT_CLASSIFICATION, ACCOUNT_CLASSIFICATION_DESCRIPTION)
# MAGIC SELECT DISTINCT T_PHS_ACCOUNTS.CUST_ACCT_ID AS CUST_ACCT_ID ,T_PHS_ACCOUNTS.CUST_NAME AS CUST_NAME ,T_PHS_AUDIT.PHS_340B_ID AS 340B_ID ,T_PHS_AUDIT.PVP_PARTICIPANT_ID AS PVP_PARTICIPANT_ID ,T_PHS_AUDIT.PVP_PARTICIPATION_FLAG AS PVP_PARTICIPATION_FLAG ,T_PHS_AUDIT.PVP_MEMBER_NAME AS PVP_MEMBER_NAME ,IFF(PVP_PARTICIPATION_FLAG='Y',PVP_ELIGIBILITY_DATE,NULL) AS PVP_EFFECTIVE_DATE ,T_PHS_AUDIT.PVP_EXPIRATION_DATE AS PVP_EXPIRATION_DATE ,T_PHS_ACCOUNTS.STORE_NUM AS STORE_NUM ,T_PHS_ACCOUNTS.DEA_FAMILY AS DEA_FAMILY ,T_PHS_ACCOUNTS.MARKETING_CAMPAIGN AS MARKETING_CAMPAIGN ,T_PHS_ACCOUNTS.HOME_DC_ID AS HOME_DC_ID ,T_PHS_ACCOUNTS.DEA_NUM AS DEA_NUM ,T_PHS_ACCOUNTS.HIN_BASE_CD AS HIN_BASE_CD ,T_PHS_ACCOUNTS.HIN_DEPT_CD AS HIN_DEPT_CD ,T_PHS_ACCOUNTS.HIN_LOCATION_CD AS HIN_LOCATION_CD ,T_PHS_ACCOUNTS.HIN_NUM AS HIN_NUM ,T_PHS_ACCOUNTS.ATTENTION_NAME_DELY AS ATTENTION_NAME_DELY ,T_PHS_ACCOUNTS.ADDR_LINE_1_DELY AS ADDR_LINE_1_DELY ,T_PHS_ACCOUNTS.ADDR_LINE_2_DELY AS ADDR_LINE_2_DELY ,T_PHS_ACCOUNTS.CITY_NAME_DELY AS CITY_NAME_DELY ,T_PHS_ACCOUNTS.STATE_CD_DELY AS STATE_CD_DELY ,T_PHS_ACCOUNTS.ZIP_CD_DELY AS ZIP_CD_DELY ,T_PHS_ACCOUNTS.ATTENTION_NAME_INV AS ATTENTION_NAME_INV ,T_PHS_ACCOUNTS.ADDR_LINE_1_INV AS ADDR_LINE_1_INV ,T_PHS_ACCOUNTS.ADDR_LINE_2_INV AS ADDR_LINE_2_INV ,T_PHS_ACCOUNTS.CITY_NAME_INV AS CITY_NAME_INV ,T_PHS_ACCOUNTS.STATE_CD_INV AS STATE_CD_INV ,T_PHS_ACCOUNTS.ZIP_CD_INV AS ZIP_CD_INV ,T_PHS_ACCOUNTS.CUST_CHAIN_ID AS CUST_CHAIN_ID ,T_PHS_ACCOUNTS.CUST_CHAIN_NAME AS CUST_CHAIN_NAME ,T_PHS_ACCOUNTS.NATIONAL_GROUP_CD AS NATIONAL_GROUP_CD ,T_PHS_ACCOUNTS.NATIONAL_GROUP_NAME AS NATIONAL_GROUP_NAME ,T_PHS_ACCOUNTS.NATIONAL_SUB_GROUP_CD AS NATIONAL_SUB_GROUP_CD ,T_PHS_ACCOUNTS.NATIONAL_SUB_GROUP_NAME AS NATIONAL_SUB_GROUP_NAME ,T_PHS_ACCOUNTS.REGION_NUM AS REGION_NUM ,T_PHS_ACCOUNTS.REGION_NAME AS REGION_NAME ,T_PHS_ACCOUNTS.DISTRICT_NUM AS DISTRICT_NUM ,T_PHS_ACCOUNTS.DISTRICT_NAME AS DISTRICT_NAME ,T_PHS_ACCOUNTS.RX_BILL_PLAN_CD AS RX_BILL_PLAN_CD ,T_PHS_ACCOUNTS.BUSINESS_TYPE_CD AS BUSINESS_TYPE_CD ,T_PHS_ACCOUNTS.DISTRIBUTION_CHANNEL AS DISTRIBUTION_CHANNEL ,T_PHS_ACCOUNTS.SALES_TERRITORY_ID AS SALES_TERRITORY_ID ,T_PHS_ACCOUNTS.PRIMARY_CUST_ID AS PRIMARY_CUST_ID ,T_PromoIDs_FINAL2.3rd_Party_Vendor AS 3RD_PARTY_VENDOR ,T_PromoIDs_FINAL2.SECONDARY_VENDOR AS SECONDARY_VENDOR ,T_PromoIDs_FINAL2.OTHER_VENDORS AS OTHER_VENDORS ,T_PHS_ACCOUNTS.ACCOUNT_CLASSIFICATION AS ACCOUNT_CLASSIFICATION ,T_PHS_ACCOUNTS.ACCOUNT_CLASSIFICATION_DESCRIPTION AS ACCOUNT_CLASSIFICATION_DESCRIPTION
# MAGIC FROM ((psas_di_qat.`340b_gold`.T_PHS_ACCOUNTS AS T_PHS_ACCOUNTS INNER JOIN psas_di_qat.`340b_GOLD`.T_PHS_AUDIT_2 AS T_PHS_AUDIT ON T_PHS_ACCOUNTS.CUST_ACCT_ID = T_PHS_AUDIT.CUST_ACCT_ID) LEFT JOIN psas_di_qat.`340b_slvr`.T_LUTL_NON_ORDERING_ACCOUNTS AS T_LUTL_NON_ORDERING_ACCOUNTS ON T_PHS_ACCOUNTS.CUST_ACCT_ID = T_LUTL_NON_ORDERING_ACCOUNTS.CUST_ACCT_ID) LEFT JOIN psas_di_qat.`340b_gold`.T_PromoIDs_FINAL2 ON T_PHS_ACCOUNTS.CUST_ACCT_ID = T_PromoIDs_FINAL2.CUST_ACCT_ID
# MAGIC WHERE (((T_PHS_ACCOUNTS.CUST_CHAIN_ID)<>'989') AND ((T_PHS_ACCOUNTS.BUSINESS_TYPE_CD)<>'31') AND ((T_PHS_ACCOUNTS.ACCOUNT_CLASSIFICATION)='003') AND ((T_PHS_AUDIT.STATUS)='ACTIVE') AND ((CHARINDEX('TEMPLATE',T_PHS_ACCOUNTS.CUST_NAME))=0) AND ((T_LUTL_NON_ORDERING_ACCOUNTS.ID) IS NULL));
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_WAC AS target
# MAGIC USING psas_di_qat.340b_brnz.T_OPACE_DAILY_PUBLIC AS source
# MAGIC ON target.340B_ID = source.340B_ID
# MAGIC WHEN MATCHED AND target.PVP_Member_Name IS NULL THEN
# MAGIC   UPDATE SET target.PVP_Member_Name = source.Entity_Name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Smirti view needs to be deleted.
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_NON_ORDERING_ACCOUNTS ;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_NON_ORDERING_ACCOUNTS 
# MAGIC SELECT T_LUTL_NON_ORDERING_ACCOUNTS.Cust_Acct_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.STATUS
# MAGIC FROM psas_di_qat.`340b_slvr`.T_LUTL_NON_ORDERING_ACCOUNTS AS T_LUTL_NON_ORDERING_ACCOUNTS LEFT JOIN psas_di_qat.340b_gold.T_PHS_AUDIT_2 AS T_PHS_AUDIT ON T_LUTL_NON_ORDERING_ACCOUNTS.Cust_Acct_ID = T_PHS_AUDIT.CUST_ACCT_ID;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PHS_MembershipRoster_WAC_PotentialMissing ;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PHS_MembershipRoster_WAC_PotentialMissing 
# MAGIC SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.SALES_ADMIN, T_PHS_AUDIT.MHS_SALES_REP, T_PHS_AUDIT.PHS_340B_ID, 
# MAGIC T_PHS_AUDIT.ACCT_CLASSIFICATION, T_PHS_AUDIT.ACTIVATION_DATE, T_PHS_AUDIT.COMMENTS, T_PHS_AUDIT.LEADS_WAC, T_PHS_AUDIT.ACCT_NAME_A34, 
# MAGIC T_PHS_AUDIT.ACCT_NAME_C34, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.CHAIN_NAM, T_PHS_AUDIT.HOME_DC_ID, T_PHS_AUDIT.STATUS, 
# MAGIC T_PHS_MembershipRoster_WAC.PVP_Participant_ID, T_PHS_MembershipRoster_WAC.PVP_Participation_Flag, T_PHS_MembershipRoster_WAC.PVP_Effective_Date, 
# MAGIC T_PHS_MembershipRoster_WAC.PVP_Expiration_Date
# MAGIC FROM (psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS T_PHS_AUDIT  LEFT JOIN psas_di_qat.340B_gold.T_NON_ORDERING_ACCOUNTS ON T_PHS_AUDIT.CUST_ACCT_ID = T_NON_ORDERING_ACCOUNTS.Cust_Acct_ID) 
# MAGIC LEFT JOIN psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_WAC AS T_PHS_MembershipRoster_WAC ON T_PHS_AUDIT.CUST_ACCT_ID = T_PHS_MembershipRoster_WAC.Cust_Acct_ID
# MAGIC WHERE (((T_PHS_AUDIT.CUST_CHN_ID) Not In ('989','60')) AND ((T_PHS_AUDIT.HOME_DC_ID) Not In ('8589','8199')) AND ((T_PHS_AUDIT.STATUS)='ACTIVE') 
# MAGIC -- Ajay Added this comments
# MAGIC --AND ((CHARINDEX('Y', ACCT_NAME_A34 || ACCT_NAME_C34 || LEADS_WAC))<>0) 
# MAGIC AND ((T_NON_ORDERING_ACCOUNTS.CUST_ACCT_NAME) Is Null) 
# MAGIC AND ((T_PHS_MembershipRoster_WAC.Cust_Name) Is Null))
# MAGIC ORDER BY T_PHS_AUDIT.CUST_CHN_ID;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PHS_MembershipRoster_Members_PotentialMissing;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PHS_MembershipRoster_Members_PotentialMissing 
# MAGIC SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.ROUTE, 
# MAGIC T_PHS_AUDIT.STOP, T_PHS_AUDIT.UPDATE_SAP AS Created, T_PHS_AUDIT.SALES_CURMTH, T_PHS_AUDIT.ACCT_CLASSIFICATION AS Acct_Class, T_PHS_AUDIT.COMMENTS, 
# MAGIC T_PHS_AUDIT.ACCT_NAME_PHS, T_PHS_AUDIT.LEADS_PHS, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.CHAIN_NAM, T_PHS_AUDIT.HOME_DC_ID, T_PHS_AUDIT.STATUS
# MAGIC FROM (psas_di_qat.`340b_gold`.T_PHS_AUDIT_2 AS T_PHS_AUDIT  LEFT JOIN psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_Members AS T_PHS_MembershipRoster_Members ON T_PHS_AUDIT.CUST_ACCT_ID = T_PHS_MembershipRoster_Members.Cust_Acct_ID) LEFT JOIN psas_di_qat.`340b_gold`.T_NON_ORDERING_ACCOUNTS 
# MAGIC ON T_PHS_AUDIT.CUST_ACCT_ID = T_NON_ORDERING_ACCOUNTS.Cust_Acct_ID
# MAGIC WHERE (((T_PHS_AUDIT.ACCT_NAME_PHS)='Y') AND ((T_PHS_AUDIT.CUST_CHN_ID) Not In ('314','585','989','060','770')) AND ((T_PHS_AUDIT.HOME_DC_ID) 
# MAGIC Not In ('8589','8199')) AND ((T_PHS_AUDIT.STATUS)='ACTIVE') AND ((T_PHS_MembershipRoster_Members.Cust_Name) Is Null) AND ((T_NON_ORDERING_ACCOUNTS.CUST_ACCT_NAME) Is Null)) OR (((T_PHS_AUDIT.LEADS_PHS)='Y') 
# MAGIC AND ((T_PHS_AUDIT.CUST_CHN_ID) Not In ('314','585','989','060','770')) AND ((T_PHS_AUDIT.HOME_DC_ID) Not In ('8589','8199')) 
# MAGIC AND ((T_PHS_AUDIT.STATUS)='ACTIVE') AND ((T_PHS_MembershipRoster_Members.Cust_Name) Is Null) AND ((T_NON_ORDERING_ACCOUNTS.CUST_ACCT_NAME) Is Null))
# MAGIC ORDER BY T_PHS_AUDIT.UPDATE_SAP;

# COMMAND ----------

# MAGIC %sql
# MAGIC --09 Q_PHS_Count
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PHS_Count;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PHS_Count
# MAGIC SELECT Count(T_PHS_MembershipRoster_Members.CUST_ACCT_ID) AS PHS_TOTAL
# MAGIC FROM psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_Members AS T_PHS_MembershipRoster_Members;
# MAGIC
# MAGIC --10 Q_WAC_Count
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_WAC_Count;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_WAC_Count
# MAGIC SELECT Count(T_PHS_MembershipRoster_WAC.CUST_ACCT_ID) AS WAC_TOTAL
# MAGIC FROM psas_di_qat.`340b_slvr`.T_PHS_MembershipRoster_WAC AS T_PHS_MembershipRoster_WAC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --11a PTQ_Termed_Accounts_PreviousMonth
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PTQ_Termed_Accounts_PreviousMonth;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PTQ_Termed_Accounts_PreviousMonth 
# MAGIC SELECT DISTINCT COUNT(CA.CUST_ACCT_ID) AS TERMED_ACCTS_PREVIOUS_MONTH
# MAGIC FROM psas_di_qat.`340b_brnz`.T_DM_VSTX_CUST_ORG V
# MAGIC INNER JOIN psas_di_qat.`340b_brnz`.T_IW_CUST_ACCT CA
# MAGIC ON V.CUST_ACCT_ID = CA.CUST_ACCT_ID
# MAGIC INNER JOIN psas_di_qat.`340b_brnz`.T_DM_VSTX_CUST VC
# MAGIC ON V.CUST_ACCT_ID = VC.CUST_ACCT_ID
# MAGIC WHERE VC.MCK_KU_ACCT_CLAS in ('004','005') AND CA.ACTIVE_CUST_IND = 'I' AND CA.CUST_CHN_ID <> '989'
# MAGIC AND V.TERM_DT BETWEEN DATE_TRUNC('MONTH', ADD_MONTHS(CURRENT_TIMESTAMP(),-1)) AND LAST_DAY(ADD_MONTHS(CURRENT_TIMESTAMP(),-1));

# COMMAND ----------

# MAGIC %sql
# MAGIC --11b PTQ_Termed_Accounts_CurrentMonth
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_PTQ_Termed_Accounts_CurrentMonth;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_PTQ_Termed_Accounts_CurrentMonth
# MAGIC SELECT DISTINCT COUNT(CA.CUST_ACCT_ID) AS TERMED_ACCTS_CURRENT_MONTH
# MAGIC FROM psas_di_qat.`340b_brnz`.T_DM_VSTX_CUST_ORG V
# MAGIC INNER JOIN psas_di_qat.340b_brnz.t_iw_cust_acct CA
# MAGIC ON V.CUST_ACCT_ID = CA.CUST_ACCT_ID
# MAGIC INNER JOIN psas_di_qat.340b_brnz.t_dm_vstx_cust VC
# MAGIC ON V.CUST_ACCT_ID = VC.CUST_ACCT_ID
# MAGIC WHERE VC.MCK_KU_ACCT_CLAS in ('004','005') AND CA.ACTIVE_CUST_IND = 'I' AND CA.CUST_CHN_ID <> '989'
# MAGIC AND V.TERM_DT BETWEEN DATE_TRUNC('MONTH', ADD_MONTHS(CURRENT_TIMESTAMP(),0)) AND LAST_DAY(ADD_MONTHS(CURRENT_TIMESTAMP(),0));

# COMMAND ----------

# MAGIC %sql
# MAGIC --12 Q_Totals_Month_To_Date
# MAGIC TRUNCATE TABLE psas_di_qat.`340b_gold`.T_Totals_Month_To_Date;
# MAGIC INSERT INTO psas_di_qat.`340b_gold`.T_Totals_Month_To_Date 
# MAGIC SELECT T_PHS_Count.PHS_Total, T_WAC_Count.WAC_Total, T_PTQ_Termed_Accounts_PreviousMonth.TERMED_ACCTS_PREVIOUS_MONTH, T_PTQ_Termed_Accounts_CurrentMonth.TERMED_ACCTS_CURRENT_MONTH
# MAGIC FROM psas_di_qat.`340b_gold`.T_PHS_Count, psas_di_qat.`340b_gold`.T_WAC_Count, psas_di_qat.`340b_gold`.T_PTQ_Termed_Accounts_PreviousMonth, psas_di_qat.`340b_gold`.T_PTQ_Termed_Accounts_CurrentMonth;
