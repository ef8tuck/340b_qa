# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   DML Executor 
#   Purpose             :   This module performs below operation:
#                               a. read strign from an input 
#                               b. split the string into valid DMLs
#                               c. Execute the DMLs
#                                
#   Input               :   DML string and other inputs
#   Output              :   
#   Pre-requisites      :   
#   Last changed on     :   22 Feb, 2024
#   Last changed by     :   Prakhar Chauhan
#   Reason for change   :   NA
# ######################################################################################################################

# COMMAND ----------

# DBTITLE 1,Importing Libraries
from datetime import datetime
import json
current_timestamp = datetime.utcnow()
from logging_utility import *
from pyspark.sql import *
import traceback

# COMMAND ----------

# DBTITLE 1,Notebook Constants
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
ERROR_KEY = "error"

# COMMAND ----------

# DBTITLE 1,Declare Variables
try:
    # Get Databricks Context
    databricks_run_context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())
    print(f"Databricks run context: {databricks_run_context}")
    try:
        databricks_job_id = databricks_run_context["attributes"]["jobId"]
    except Exception:
        databricks_job_id = databricks_run_context["attributes"]["jobGroup"]
    databricks_run_id = databricks_run_context["attributes"]["currentRunId"]
    notebook_location = databricks_run_context["attributes"]["notebook_path"]

    options = json.loads(dbutils.widgets.get('options'))
    source_table_identifier = dbutils.widgets.get('source_table_identifier')
    target_table_identifier = dbutils.widgets.get('target_table_identifier')
    options['DATABRICKS_JOB_ID'] = databricks_job_id
    options['DATABRICKS_RUN_ID'] = databricks_run_id
    options['source_table_name'] = source_table_identifier
    options['target_table_name'] = target_table_identifier
    StartTime = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    adfRunId = dbutils.widgets.get('adf_run_id')
    adfJobId = dbutils.widgets.get('adf_job_id')
    LAYER = dbutils.widgets.get('LAYER')
    environment = dbutils.widgets.get('env')
    gold_dml = dbutils.widgets.get('gold_dml')
    catalog_name = 'fdp_'+environment
    options['catalog_name'] = catalog_name
    # Add logging variables
    LOGGING_INGESTION_MASTER_TABLE_NAME = f"{dbutils.widgets.get('logging_table_name')}"
    database_host = dbutils.widgets.get("database_host")
    database_port = dbutils.widgets.get("database_port")
    database_name = dbutils.widgets.get("database_name")
    db_secret_scope = dbutils.widgets.get("db_secret_scope")
    pipeline_name = dbutils.widgets.get("pipeline_name")
    process_name = dbutils.widgets.get("process_name")
    interface_name = dbutils.widgets.get("interface_name")
    user_secret_key = dbutils.secrets.get(scope=db_secret_scope,key = dbutils.widgets.get("user_secret_key"))
    pw_secret_key = dbutils.secrets.get(scope=db_secret_scope,key = dbutils.widgets.get("pw_secret_key"))
    logging_input_params_insert = {}
    logging_connection_params = {}
except:
        raise Exception()

# COMMAND ----------

# DBTITLE 1,Creating Database Connection 
def prepare_sql_connection_info():
    """
    Purpose     :   Prepare SQL Connection Parameters
    Input       :   
                    
    Output      :   Return status Success and connection parmeters/Failed and error message
    """

    status_message = "Starting method to prepare logging table connection info"
    global logging_connection_params
    
    try:
        logging_connection_params["database_host"] = database_host
        logging_connection_params["database_port"] = database_port
        logging_connection_params["database_name"] = database_name
        logging_connection_params["table"] = LOGGING_INGESTION_MASTER_TABLE_NAME
        logging_connection_params["db_secret_scope"] = db_secret_scope
        logging_connection_params["user"] = user_secret_key
        logging_connection_params["password"] = pw_secret_key

        print("Successfully created logging table connection info")
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: logging_connection_params}
    except Exception as e:
        status_message = "Failed method to prepare logging table connection info"
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, 'ERROR_KEY': error_message}

def prepare_logging_info():
    """
    Purpose     :   Prepare SQL Connection Parameters
    Input       :   
                    
    Output      :   Return status Success and logging parameters/Failed and error message
    """
    global logging_input_params_insert
    try:
        status_message = "Starting method to prepare logging info"
        print(status_message)
        
        logging_input_params_insert['source_table_identifier'] = source_table_identifier
        logging_input_params_insert['target_table_identifier'] = target_table_identifier
        logging_input_params_insert["StartTime"] = current_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logging_input_params_insert["LAYER"] = LAYER
        logging_input_params_insert["environment"] = environment
        logging_input_params_insert["pipeline_name"] = pipeline_name
        logging_input_params_insert["process_name"] = process_name
        logging_input_params_insert["interface_name"] = interface_name
        logging_input_params_insert["NotebookLocation"] = notebook_location
        logging_input_params_insert["DatabricksJobId"] = databricks_job_id
        logging_input_params_insert["DatabricksRunId"] = databricks_run_id

        print("Successfully created logging info")                
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: logging_input_params_insert}
    except Exception as e:
        status_message = "Failed method to prepare logging info"
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,SQL Execution Functions
def execute_query(sql_content):
    # Execute the SQL query using spark.sql()
    try:
        result_df = spark.sql(sql_content)

        # Display the actual values by calling the show() method
        return result_df
    except Exception as e:
        print(f"Error: {str(e)}")
        raise Exception() 

# Main function
def execute_sql_query():

    # File path to the SQL file
    sql_content = gold_dml
    status_message = "Starting execution of SQL"
    print(status_message)

    try:
        # Read SQL file
        ## Reading SQL File and separating queries separated by a ';' 
        sql_content_list = [command.strip() for command in sql_content.split(';') if command.strip()]
        for sql_command in sql_content_list:
            sql_command = sql_command.strip()

            if sql_command != "":
            # Replace placeholders in the SQL content with values from options
                if options:
                    for key, value in options.items():
                        print(key,value)
                        sql_command = sql_command.replace(f"$${key}", str(value))
                print(sql_command)          
                # Execute DML
                result_df = execute_query(sql_command)
                print('dml_status :')
                result_df.show()
    
            print("DML execution completed successfully.")
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: result_df}
    except Exception as e:
        status_message = "Failed execution of SQL"
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,Main
"""
#         Purpose     :   This function reads the configuration from given config file path
#         Input       :   config file path
#         Output      :   Return status SUCCESS/FAILED
"""
print("Started data ingestion from Silver to Gold")

##Initializing Logging connection details
connection_params = prepare_sql_connection_info()
print(connection_params)
if connection_params[STATUS_KEY] == FAILED_KEY:
    raise Exception()

output = prepare_logging_info()

if output[STATUS_KEY] == FAILED_KEY:
    raise Exception()

output = execute_sql_query()
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception()   
