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
#   Last changed on     :   20 May, 2024
#   Last changed by     :   Debangon Basak
#   Reason for change   :   NA
# ######################################################################################################################

# COMMAND ----------

column_mapping = {
    'PROCESS_INSTANCE' : 'PRCSS_INSTNC',
    'EFFDT' : 'EFF_DT',
    'DTTM_STAMP_SEC' : 'DTTM_STAMP_SEC'
}

# COMMAND ----------

target_table_identifier = dbutils.widgets.get('target_table_identifier')
if dbutils.widgets.get('source_delta_col') != '':
    delta_column_identifier = column_mapping[dbutils.widgets.get('source_delta_col')]
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

if target_table_identifier.split(".")[-1] == 't_ps_ledger':
    latest_source_delta_col_op = spark.sql(f"select max(concat(FISC_YR, ACCNTNG_PRD)) as max_source_delta_op from {target_table_identifier} where {delta_column_identifier} is not null").collect()[0][0]
    if len(str(latest_source_delta_col_op)) == 5:
        latest_source_delta_col_op = latest_source_delta_col_op[:-1] + '0' + latest_source_delta_col_op[-1]    
    if source_delta_op != '':
        if int(source_delta_op) < int(latest_source_delta_col_op):
            if int(str(source_delta_op)[-2:]) == 1:
                fiscal_year = int(str(source_delta_op)[:-2])-1
                accounting_period = 12
            else:
                fiscal_year = int(str(source_delta_op)[:-2])
                accounting_period = int(str(source_delta_op)[-2:])-1
            
            print(f"select max({delta_column_identifier}) as max_source_delta_col from {target_table_identifier} where {delta_column_identifier} is not null and FISC_YR = {fiscal_year} and ACCNTNG_PRD = {accounting_period}")

            last_source_delta_col_val = spark.sql(f"select max({delta_column_identifier}) as max_source_delta_col from {target_table_identifier} where {delta_column_identifier} is not null and FISC_YR = {fiscal_year} and ACCNTNG_PRD = {accounting_period} ").collect()[0]["max_source_delta_col"]
            last_source_delta_col_val = last_source_delta_col_val.strftime('%Y-%m-%d')
    output['source_delta_val'] = last_source_delta_col_val
    output['source_delta_op'] = latest_source_delta_col_op

        
print(output)
dbutils.notebook.exit(output)
