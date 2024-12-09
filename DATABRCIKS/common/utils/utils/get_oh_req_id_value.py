# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Get Max incremental column value from peoplesoft gold layer tables
#   Purpose             :   This module performs below operation:
#                               a. Get Max incremental column value from peoplesoft gold layer tables 
#                               b. return max value of incremental column
#                                
#   Input               :   Source table name
#   Output              :   
#   Pre-requisites      :   
#   Last changed on     :   20 May, 2024
#   Last changed by     :   Nikhil Deshpande
#   Reason for change   :   NA
# ######################################################################################################################

# COMMAND ----------

target_table_identifier = dbutils.widgets.get('target_table_identifier')
delta_column_identifier = dbutils.widgets.get('source_delta_col').replace('"','')

# COMMAND ----------

last_source_delta_col_val = spark.sql(f"select max(`{delta_column_identifier}`) as max_source_delta_col from {target_table_identifier} where {delta_column_identifier} is not null").collect()[0]["max_source_delta_col"]
dbutils.notebook.exit(last_source_delta_col_val)
