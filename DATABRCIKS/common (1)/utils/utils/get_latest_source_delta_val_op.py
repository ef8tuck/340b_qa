# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Get Max incremental column value from peoplesoft gold layer tables
#   Purpose             :   This module performs below operation:
#                               a. Get Max incremental column value from peoplesoft gold layer transactional tables 
#                               b. return max value of incremental column
#                               c. Update source_delta_op column for t_ps_ledger table
#                                
#   Input               :   Source table name
#   Output              :   
#   Pre-requisites      :   
#   Last changed on     :   20 Aug, 2024
#   Last changed by     :   Raghav Haralalka
#   Reason for change   :   Update based on the requirements PSAS EDP Architecture
# ######################################################################################################################

# COMMAND ----------

target_table_identifier = dbutils.widgets.get('target_table_identifier')
if dbutils.widgets.get('source_delta_col') != '':
    delta_column_identifier = dbutils.widgets.get('source_delta_col')
source_delta_op = dbutils.widgets.get('source_delta_op')

# COMMAND ----------

output = {
    'source_delta_val' : '',
    'source_delta_op' : ''
}

last_source_delta_col_val = spark.sql(f"select max({delta_column_identifier}) as max_source_delta_col from {target_table_identifier} where {delta_column_identifier} is not null").collect()[0]["max_source_delta_col"]

if str(type(last_source_delta_col_val)) == "<class 'datetime.datetime'>":
    last_source_delta_col_val = last_source_delta_col_val.strftime('%Y-%m-%d')
else:
    last_source_delta_col_val = int(last_source_delta_col_val)

output['source_delta_val'] = last_source_delta_col_val

print(output)
dbutils.notebook.exit(output)
