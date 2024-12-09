# Databricks notebook source
# MAGIC %run ./sql_execution_utility

# COMMAND ----------

table_ddl_file_path = dbutils.widgets.get("table_ddl_file_path")
target_table_identifier = dbutils.widgets.get("target_table_identifier")
delta_path = dbutils.widgets.get("delta_path")

# COMMAND ----------

#Python Imports
import traceback
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

"""
Purpose     :   create target_table
Input       :   
"""
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
STATUS_KEY = "status"
ERROR_KEY = "error"

def create_table():
    current_table_name = ""
    global delta_catalog 
    global delta_database 
    global delta_table
    global delta_path

    try:
        status_message = "Starting method to get_target_table_details"
    
        target_table_identifier = dbutils.widgets.get("target_table_identifier")
        delta_catalog = target_table_identifier.split('.')[0]
        delta_database = target_table_identifier.split('.')[1] 
        delta_table = target_table_identifier.split('.')[2]    
    
        # create the table before writing the df to adls
        options = {
            "unity_catalog_name":delta_catalog,
            "schema_name":delta_database,
            "delta_table_name":delta_table,
            "delta_table_location":delta_path
        }

        if table_ddl_file_path == None or table_ddl_file_path == "":
            status_message= "No Table ddl path provided, exiting the Notebook run."

        else:
            execute_file_from_path(table_ddl_file_path,options)
    
        return {STATUS_KEY: SUCCESS_KEY}

    except Exception as e:
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


# COMMAND ----------

# trigger
output = create_table()
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception()
