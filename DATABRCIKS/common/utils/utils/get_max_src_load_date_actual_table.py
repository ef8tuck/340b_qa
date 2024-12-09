# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Get Max src_load_date from copa actuals table 
#   Purpose             :   This module performs below operation:
#                               a. Get max src_laod_date from table 
#                               b. return the src_laod_date
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

# COMMAND ----------

# DBTITLE 1,Dimension Table
src_load_date = spark.sql(f"select max(SRC_LOAD_DT_BEX) as src_load_date from {target_table_identifier} where SRC_LOAD_DT_BEX is not null").collect()[0]["src_load_date"]
update_date = {"src_load_date": str(src_load_date)}
dbutils.notebook.exit(update_date)
