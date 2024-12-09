# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Silver To Gold Ingestion 
#   Purpose             :   This module performs below operation:
#                               a. read silver table as a stream 
#                               b. Execute DMLs to load data in gold tables
#                                
#   Input               :   Source table name, target table name,gold_dml and other inputs
#   Output              :   
#   Pre-requisites      :   
#   Last changed on     :   22 Feb, 2024
#   Last changed by     :   Prakhar Chauhan
#   Reason for change   :   NA
# ######################################################################################################################

# COMMAND ----------

# DBTITLE 1,Importing Functions/Libraries
import json
from datetime import datetime
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from logging_utility import *
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Notebook Constants
# Defining constants and column names for data processing and housekeeping
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
ERROR_KEY = "error"
EXECUTION_STATUS_KEY = "ExecutionStatus"
END_TS_KEY = "EndTime"
LAYER = "gold"
# Housekeeping column names
DATABRICKS_RUN_ID_COLUMN = "DATABRICKS_RUN_ID"
DATABRICKS_JOB_ID_COLUMN = "DATABRICKS_JOB_ID"
TEMP_TABLE_NAME = "silver_table"
BUSINESS_UNIT_COLUMN = "BUSINESS_UNIT"
SEGMENT_NAME_COLUMN = "SEGMENT_NAME"
SOURCE_SYSTEM_NAME_COLUMN = "SOURCE_SYSTEM_NAME"

# COMMAND ----------

# DBTITLE 1,Declaring variables
print("Initializing variables: Start of variable declaration process")


# Get Databricks Context
databricks_run_context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())
print(f"Databricks run context: {databricks_run_context}")
try:
    databricks_job_id = databricks_run_context["attributes"]["jobId"]
except Exception:
    databricks_job_id = databricks_run_context["attributes"]["jobGroup"]
databricks_run_id = databricks_run_context["attributes"]["currentRunId"]
notebook_location = databricks_run_context["attributes"]["notebook_path"]

# Options are table level and dynamically generated
options = json.loads(dbutils.widgets.get('options'))
options["DATABRICKS_JOB_ID"] = databricks_job_id
options["DATABRICKS_RUN_ID"] = databricks_run_id

pipeline_name = dbutils.widgets.get("pipeline_name")
adf_run_id = dbutils.widgets.get("run_id")
adf_job_id = dbutils.widgets.get("job_id")

# Logging details
LOGGING_INGESTION_MASTER_TABLE_NAME = f"{dbutils.widgets.get('logging_table_name')}"
database_host = dbutils.widgets.get("database_host")
database_port = dbutils.widgets.get("database_port")
database_name = dbutils.widgets.get("database_name")
db_secret_scope = dbutils.widgets.get("db_secret_scope")
user_secret_key = dbutils.secrets.get(scope=db_secret_scope,key = dbutils.widgets.get("user_secret_key"))
pw_secret_key = dbutils.secrets.get(scope=db_secret_scope,key = dbutils.widgets.get("pw_secret_key"))
process_name = dbutils.widgets.get("process_name")
interface_name = dbutils.widgets.get("interface_name")

# Application Variables
environment = dbutils.widgets.get("env")
catalog_name = 'fdp_'+environment
options['catalog_name'] = catalog_name
target_table_identifier = dbutils.widgets.get("target_table_identifier")
source_table_identifier = dbutils.widgets.get("source_table_identifier")
source_file_path = dbutils.widgets.get("source_file_path")
pipeline_start_time = datetime.strptime(dbutils.widgets.get("pipeline_start_time")[:-4] + dbutils.widgets.get("pipeline_start_time")[-1],"%Y-%m-%dT%H:%M:%S.%fZ")

try:
    gold_last_processed_version = dbutils.widgets.get("gold_last_processed_version")
    if gold_last_processed_version == '' or gold_last_processed_version == None:
        gold_last_processed_version = 0
    else:
        gold_last_processed_version =  int(gold_last_processed_version) 
except Exception as e:
    gold_last_processed_version = 0

silver_latest_version = spark.sql(f"SELECT MAX(version) FROM (DESCRIBE HISTORY {source_table_identifier})").collect()[0][0]

current_timestamp = datetime.utcnow()

logging_input_params_insert = {}
logging_connection_params = {}

target_table_name = (target_table_identifier.split(".")[2]).upper()

checkpoint_file_path_gold = dbutils.widgets.get("checkpoint_file_path_gold")
start_time = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")

gold_dml = dbutils.widgets.get('gold_dml')
business_unit = dbutils.widgets.get("business_unit")
segment_name = dbutils.widgets.get("segment_name")
source_system_name = dbutils.widgets.get("source_system_name")
is_dimension = dbutils.widgets.get("is_dimension")
print("Variables finalized: Completed variable declaration process")

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
        logging_input_params_insert["adfRunId"] = adf_run_id
        logging_input_params_insert["adfJobId"] = adf_job_id
        logging_input_params_insert["Layer"] = LAYER
        logging_input_params_insert["Env"] = environment
        logging_input_params_insert["pipeline_name"] = pipeline_name
        logging_input_params_insert["process_name"] = process_name
        logging_input_params_insert["interface_name"] = interface_name
        logging_input_params_insert["NotebookLocation"] = notebook_location
        logging_input_params_insert["DatabricksJobId"] = databricks_job_id
        logging_input_params_insert["DatabricksRunId"] = databricks_run_id

        print("Successfully created logging info")                
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: logging_input_params_insert}
    except Exception as e:
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,Process DML foreach batch
def process_batch(df, epoch_id):
    try:
        # Register DataFrames as temporary SQL tables
        TEMP_TABLE_NAME = options['target_table_name'].split('.')[-1]+"_silver_table_temp"
        df.createOrReplaceTempView(TEMP_TABLE_NAME)
        options['silver_temp_table_name'] = TEMP_TABLE_NAME
        # Define multiple Spark SQL statements
        # Read DML from widgets
        sql_statements = gold_dml
        print(sql_statements)
        # Reading SQL File and separating queries separated by a ';' 
        sql_content_list = [command.strip() for command in sql_statements.split(';') if command.strip()]
        print(sql_content_list)
        # sql_content_list = sql_content.split(';')
        for sql_command in sql_content_list:
            sql_command = sql_command.strip()
            
            if sql_command != "":
                # Replace placeholders in the SQL content with values from options
                if options:
                    for key, value in options.items():
                        print(key,value)
                        sql_command = sql_command.replace(f"$${key}", str(value))
                print(sql_command)

                df.sparkSession.sql(sql_command)
    except Exception as e:
        print(f"Error: {str(e)}")
        raise Exception()


# COMMAND ----------

# DBTITLE 1,Updated copy function
def source_to_target_data_load():
    """
    Purpose     :   create source_to_target_data_copy
    Input       :   dq dataframe with valid/invalid records
                    
    Output      :   Return status SUCCESS/FAILED
    """
            
    global write_mode
    global logging_input_params_insert

    try:
        status_message = "Starting method to perform data copy"
        print(status_message)
        print("check",checkpoint_file_path_gold)
        print("tgt tbl",target_table_identifier)
        print("Checking if silver table version is equal to gold last processed version.")
        if silver_latest_version != gold_last_processed_version:
            # Read Silver table into stream 
            # Increment the gold_last_processed_version value while reading data from table
            df = spark.readStream.option("readChangeData", "true").option("startingVersion", gold_last_processed_version+1).option("ignoreDeletes", "true").table(source_table_identifier)
            # .filter(col("_change_type") != 'update_preimage')

            options["target_table_name"] = target_table_identifier
            
            streaming_query = df\
                .withColumn(BUSINESS_UNIT_COLUMN, lit(business_unit))\
                .withColumn(SEGMENT_NAME_COLUMN, lit(segment_name))\
                .withColumn(SOURCE_SYSTEM_NAME_COLUMN, lit(source_system_name))\
                .writeStream \
                .foreachBatch(process_batch) \
                .outputMode("append")\
                .option("checkpointLocation", checkpoint_file_path_gold)\
                .option("skipChangeCommits", "true")\
                .trigger(availableNow=True)\
                .start()

            #wait for writestream to terminate before executing further statements
            streaming_query.awaitTermination()
            print(f"Successfully merged  records to the gold table : {target_table_identifier}")
        else:
            print("Skipped data copy batch processing as silver table latest version is equal to gold last processed version.")  

        print("Extarcting counts of source table and target table")
        logging_input_params_insert["SourceTableCount"] = spark.sql(f"select count(1) as count from {source_table_identifier} where ADF_RUN_ID IN (select distinct ADF_RUN_ID from {target_table_identifier} where DATABRICKS_RUN_ID = {databricks_run_id} and DATABRICKS_JOB_ID = {databricks_job_id})").collect()[0]["count"]
        logging_input_params_insert["TargetTableCount"] = spark.sql(f"select count(1) as count from {target_table_identifier} where DATABRICKS_RUN_ID = {databricks_run_id} and DATABRICKS_JOB_ID = {databricks_job_id}").collect()[0]["count"]

        end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logging_input_params_insert[END_TS_KEY] = end_time
        logging_input_params_insert[EXECUTION_STATUS_KEY] = SUCCESS_KEY
        print("logging end result")
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

# DBTITLE 1,Get Last Processed Version
def last_cdc_processed_version():
    """
    Module Name         :   Get last updated version from source table 
    Purpose             :   This module performs below operation:
                            a. read history of a table 
                            b. get max commit version from history
                            c. return the version as int                               
    Input               :   Source table name
    Output              :   Last updated version as an integer
    """
    try:
        # Read the table name from the widget
        source_table_name = dbutils.widgets.get("source_table_identifier")
        
        # Get the last processed version
        latest_version = DeltaTable.forName(spark, source_table_name).history(1).head()["version"]
        last_updated_version = int(latest_version)
        print("Successfully retrieved last processed version:", last_updated_version)
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: last_updated_version}
    except Exception as e:
        error_message = "Error occurred while retrieving last processed version: " + str(e)
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,Main - To add DQ
"""
#         Purpose     :   This function reads the configuration from given config file path
#         Input       :   config file path
#         Output      :   Return status SUCCESS/FAILED
"""
print("Started data ingestion from Silver to Gold")

# Prepare SQL connection information 
connection_params = prepare_sql_connection_info()
print(connection_params)
if connection_params[STATUS_KEY] == FAILED_KEY:
    raise Exception()
 
 # Prepare logging information
output = prepare_logging_info()
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception()

# Trigger the data ingestion process from the source (Silver) to the target (Gold)
output = source_to_target_data_load()
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception()

# Get last updated version from source table
output = last_cdc_processed_version()
last_updated_version = {"last_updated_version_source_table": int(output[RESULT_KEY])}
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception()

dbutils.notebook.exit(last_updated_version)
