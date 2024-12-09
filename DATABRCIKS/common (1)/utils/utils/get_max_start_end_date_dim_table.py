# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Get Max src_load_date from copa actuals table 
#   Purpose             :   This module performs below operation:
#                               a. Get max effective date and expiration date from  dimension q tables
#                               b. return the effective date and expiration date
#                                
#   Input               :   Source table name
#   Output              :   
#   Pre-requisites      :   
#   Last changed on     :   12 Mar, 2024
#   Last changed by     :   Prakhar Chauhan
#   Reason for change   :   NA
# ######################################################################################################################

# COMMAND ----------

target_table_identifier = dbutils.widgets.get('target_table_identifier')
# TODO Run it only for Q table: Add if block in ADF pipeline to filer the notebook call

# COMMAND ----------

# DBTITLE 1,Dimension Table
last_start_date_load_landing_val = spark.sql(f"select max(EFFECTIVE_DATE) as last_start_date_load_landing from {target_table_identifier} where EFFECTIVE_DATE is not null").collect()[0]["last_start_date_load_landing"]
last_end_date_loading_val = spark.sql(f"select max(EXPIRATION_DATE) as last_end_date_load_landing from {target_table_identifier} where EXPIRATION_DATE != to_date('99991231', 'yyyyMMdd') and EXPIRATION_DATE is not null").collect()[0]["last_end_date_load_landing"]
update_date = {"last_start_date_load_landing": str(last_start_date_load_landing_val),"last_end_date_loading": str(last_end_date_loading_val)}
dbutils.notebook.exit(update_date)
