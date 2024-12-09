# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Landing To Bronze Ingestion 
#   Purpose             :   This module performs below operation:
#                               a. read cloudfiles from autoloader 
#                               b. write data to bronze tables
#                                
#   Input               :   Source file path, target table name and other inputs
#   Output              :   
#   Pre-requisites      :   
#   Last changed on     :   22 Feb, 2024, 27 Sep 2024
#   Last changed by     :   Prakhar Chauhan, Vijay Maheshwaram 
#   Reason for change   :   NA,  provided more information in the Exception to understand what went wrong. Modify the Exception to include a message in 8.Main method.
# ######################################################################################################################

# COMMAND ----------

# DBTITLE 1,Importing Libraries/Functions
import json
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from logging_utility import *
import traceback

# COMMAND ----------

# DBTITLE 1,Notebook Constants
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
ERROR_KEY = "error"
EXECUTION_STATUS_KEY = "ExecutionStatus"
END_TS_KEY = "EndTime"
LAYER = "bronze"
# Housekeeping column names
DATABRIKCS_RUN_ID_COLUMN = "DATABRICKS_RUN_ID"
DATABRIKCS_JOB_ID_COLUMN = "DATABRICKS_JOB_ID"

# COMMAND ----------

# DBTITLE 1,Declaring variables
print("Initializing variables: Start of variable declaration process")
# Get Databricks Context
databricks_run_context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())
print(f"Databricks run context: {databricks_run_context}")
try:
    DATABRICKS_JOB_ID = databricks_run_context["attributes"]["jobId"]
except Exception:
   DATABRICKS_JOB_ID = databricks_run_context["attributes"]["jobGroup"]
DATABRICKS_RUN_ID = databricks_run_context["attributes"]["currentRunId"]
notebook_location = databricks_run_context["attributes"]["notebook_path"]

# Logging details
LOGGING_INGESTION_MASTER_TABLE_NAME = f"{dbutils.widgets.get('logging_table_name')}"
database_host = dbutils.widgets.get("database_host")
database_port = dbutils.widgets.get("database_port")
database_name = dbutils.widgets.get("database_name")
db_secret_scope = dbutils.widgets.get("db_secret_scope")
user_secret_key = dbutils.secrets.get(scope=db_secret_scope,key = dbutils.widgets.get("user_secret_key"))
pw_secret_key = dbutils.secrets.get(scope=db_secret_scope,key = dbutils.widgets.get("pw_secret_key"))
ADF_RUN_ID = dbutils.widgets.get("run_id")
ADF_JOB_ID = dbutils.widgets.get("job_id")
process_name = dbutils.widgets.get("process_name")
interface_name = dbutils.widgets.get("interface_name")

# Application Variables
environment = dbutils.widgets.get("env")
target_table_identifier = dbutils.widgets.get("target_table_identifier")
source_table_identifier = dbutils.widgets.get("source_table_identifier")
source_file_path = dbutils.widgets.get("source_file_path")
checkpoint_file_path = dbutils.widgets.get("checkpoint_path")
bad_records_file_path = dbutils.widgets.get("bad_record_path")
schema_file_path = dbutils.widgets.get("schema_path")
write_mode = dbutils.widgets.get("bronze_write_mode")
pipeline_start_time = datetime.strptime(dbutils.widgets.get("pipeline_start_time")[:-4] + dbutils.widgets.get("pipeline_start_time")[-1],"%Y-%m-%dT%H:%M:%S.%fZ") # can be removed

# Databricks table params
# delta_catalog = None # can be removed
# delta_database = None # can be removed
# delta_table = None # can be removed
current_timestamp = datetime.utcnow()

logging_input_params_insert = {}
logging_connection_params = {}
start_time = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
print("Variables finalized: Completed variable declaration process")

# COMMAND ----------

# DBTITLE 1,Get target table details
# def get_target_table_details(): # can be removed
#     """
#     Purpose     :   create get_target_table_details
#     Input       :   
#                     .
#     Output      :   Return status SUCCESS/FAILED
#     """
#     current_table_name = ""
#     global delta_catalog 
#     global delta_database 
#     global delta_table
#     global delta_path

#     try:
#         status_message = "Starting method to get_target_table_details"
#         print(status_message)
        
#         delta_catalog = target_table_identifier.split('.')[0]
#         delta_database = target_table_identifier.split('.')[1] 
#         delta_table = target_table_identifier.split('.')[2]
#         delta_path = checkpoint_file_path.split('/checkpoint')[0]
#         return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: target_table_identifier}
#     except Exception as e:
#         error_message = status_message + ": " + str(traceback.format_exc())
#         print(error_message)
#         return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,Prepare logging info
def prepare_sql_connection_info():
    """
    Purpose     :   Prepare SQL Connection Parameters
    Input       :   
                    
    Output      :   Return status Success and connection parmeters/Failed and error message
    """
    global logging_connection_params
    
    try:
        status_message = "Starting method to prepare logging table connection info"
        print(status_message)
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
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

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
        logging_input_params_insert['SourceTable'] = source_table_identifier
        logging_input_params_insert['TargetTable'] = target_table_identifier
        logging_input_params_insert["StartTime"] = current_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logging_input_params_insert["adfRunId"] = ADF_RUN_ID
        logging_input_params_insert["adfJobId"] = ADF_JOB_ID
        logging_input_params_insert["Layer"] = LAYER
        logging_input_params_insert["Env"] = environment
        logging_input_params_insert["process_name"] = process_name
        logging_input_params_insert["interface_name"] = interface_name
        logging_input_params_insert["NotebookLocation"] = notebook_location
        logging_input_params_insert["DatabricksJobId"] = DATABRICKS_JOB_ID
        logging_input_params_insert["DatabricksRunId"] = DATABRICKS_RUN_ID
        
        print("Successfully created logging info")
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: logging_input_params_insert}
    except Exception as e:
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,Source to target data copy
def source_to_target_data_copy(source_file_path):
        """
        Purpose     :   create source_to_target_data_copy
        Input       :   
                        
        Output      :   Return status SUCCESS/FAILED
        """
        global write_mode
        global create_table
        global logging_input_params_insert
        global ingestion_details
        global file_list
        try:
            status_message = "Starting method to source_to_target_data_copy"
            print(status_message)
            
            file_list = []
            columns_to_drop = ["day", "hr", "pipeline_name"]
            print(f"Starting write to bronze table: {target_table_identifier}")
            spark.sql(f"update {target_table_identifier} set is_active='N' where is_active='Y' ")

            streaming_query = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "parquet") \
            .option('badRecordsPath', bad_records_file_path) \
            .option("cloudFiles.schemaLocation", schema_file_path) \
            .option("ignoreDeletes", "true") \
            .load(source_file_path) \
            .withColumn(DATABRIKCS_JOB_ID_COLUMN, lit(DATABRICKS_JOB_ID)) \
            .withColumn(DATABRIKCS_RUN_ID_COLUMN, lit(DATABRICKS_RUN_ID)) \
            .withColumn('input_file_name', col("_metadata.file_path")) \
            .drop(*columns_to_drop) \
            .writeStream \
            .outputMode(write_mode) \
            .option("checkpointLocation", checkpoint_file_path) \
            .trigger(once=True) \
            .toTable(target_table_identifier)

            streaming_query.awaitTermination()
            
            is_active_count = spark.sql(f"select count(1) as count from {target_table_identifier} where is_active='Y'").collect()[0]["count"]

            if is_active_count > 0:
                spark.sql(f"delete from {target_table_identifier} where is_active='N'")
            else
                spark.sql(f"update {target_table_identifier} set is_active='Y' where is_active='N' ")   

            target_table_count = spark.sql(f"select count(1) as count from {target_table_identifier} where DATABRICKS_RUN_ID = {DATABRICKS_RUN_ID} and DATABRICKS_JOB_ID = {DATABRICKS_JOB_ID}").collect()[0]["count"]
            logging_input_params_insert["SourceTableCount"] = streaming_query.lastProgress["sources"][0]["numInputRows"]
            logging_input_params_insert["TargetTableCount"] = target_table_count
            end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            logging_input_params_insert[END_TS_KEY] = end_time
            logging_input_params_insert[EXECUTION_STATUS_KEY] = SUCCESS_KEY
            
            insert_data_response = insert_data_to_server(spark,logging_input_params_insert,logging_connection_params)
            return {STATUS_KEY: SUCCESS_KEY}
        except Exception as e:
            error_message = status_message + ": " + str(traceback.format_exc())
            print(error_message)
            end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            logging_input_params_insert["ErrorMessage"] = error_message
            logging_input_params_insert[END_TS_KEY] = end_time
            logging_input_params_insert[EXECUTION_STATUS_KEY] = FAILED_KEY
            
            insert_data_response = insert_data_to_server(spark,logging_input_params_insert,logging_connection_params)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,Main method
"""
#         Purpose     :   This function reads the configuration from given config file path
#         Input       :   config file path
#         Output      :   Return status SUCCESS/FAILED
#         """
print("Started data ingestion from landing to deltalake")

print("Source file to be ingested: " + source_file_path)

# # trigger  # can be removed
# output = get_target_table_details()
# if output[STATUS_KEY] == FAILED_KEY:
#     raise Exception()

# trigger 
output = prepare_sql_connection_info()
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception("Failed to prepare SQL connection info")

# trigger 
output =prepare_logging_info()
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception("Failed to prepare logging info")

# trigger 
output = source_to_target_data_copy(source_file_path)
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception("Failed to copy data from source to target")
