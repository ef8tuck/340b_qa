# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Bronze To Silver Ingestion 
#   Purpose             :   This module performs below operation:
#                               a. read table as a stream 
#                               b. Perform DQ checks
#                               c. write data to silver tables
#                                
#   Input               :   Source table name, target table name and other inputs
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
import traceback
from pyspark.sql.functions import lit
from logging_utility import *
from dq_utility import *

# COMMAND ----------

# DBTITLE 1,Notebook Constants
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
ERROR_KEY = "error"
STATUS_COLUMN = "status"
EXECUTION_STATUS_KEY = "ExecutionStatus"
END_TS_KEY = "EndTime"
LAYER = "silver"
# Housekeeping column names
DATABRICKS_RUN_ID_COLUMN = "DATABRICKS_RUN_ID"
DATABRICKS_JOB_ID_COLUMN = "DATABRICKS_JOB_ID"
DQ_STATUS_COLUMN_NAME = "dq_record_status"

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

adf_run_id = dbutils.widgets.get("run_id")
adf_job_id = dbutils.widgets.get("job_id")
pipeline_name = dbutils.widgets.get("pipeline_name")

# TODO : Make the option stable level and dynamic


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
target_table_identifier = dbutils.widgets.get("target_table_identifier")
source_table_identifier = dbutils.widgets.get("source_table_identifier")
source_file_path = dbutils.widgets.get("source_file_path")
pipeline_start_time = datetime.strptime(dbutils.widgets.get("pipeline_start_time")[:-4] + dbutils.widgets.get("pipeline_start_time")[-1],"%Y-%m-%dT%H:%M:%S.%fZ") # can be removed

current_timestamp = datetime.utcnow()

logging_input_params_insert = {}
logging_connection_params = {}

target_table_name = (target_table_identifier.split(".")[2]).upper()
dq_config_id = dbutils.widgets.get("dq_config_id")
dq_config_table_name = dbutils.widgets.get("dq_config_table_name")

checkpoint_file_path_silver = dbutils.widgets.get("checkpoint_file_path_silver")
checkpoint_file_path_silver_invalid = dbutils.widgets.get("checkpoint_file_path_silver_invalid")
md5_creation_flag = dbutils.widgets.get("md5_flag")

housekeeping_columns = dbutils.widgets.get('housekeeping_column_list').replace('"', '').split(',')
housekeeping_columns = [item.strip() for item in housekeeping_columns]


start_time = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")

silver_dml = dbutils.widgets.get('silver_dml')

##Creating Options for Table
options = json.loads(dbutils.widgets.get('options'))
options["DATABRICKS_JOB_ID"] = databricks_job_id
options["DATABRICKS_RUN_ID"] = databricks_run_id
options["target_table_name"] = target_table_identifier
enable_dq_checks = dbutils.widgets.get("enable_dq_checks")
print("Variables finalized: Completed variable declaration process")

# COMMAND ----------

# DBTITLE 1,Creating MD5 hash
def create_md5_column(dataframe):
    """
    Purpose     :   create MD5 hash
    Input       :   Dataframe
                    
    Output      :   Return Dataframe with MD5 hash column
    """
    try:
        ##Below columns have to be excluded when columns have to be concatenated for creating MD5 hash
        print(housekeeping_columns)
        housekeeping_columns.extend(['dq_details', 'dq_record_status'])
        columns_to_concatenate = [col for col in dataframe.columns if col not in housekeeping_columns]
        print("Creating MD5 hash")
        print(columns_to_concatenate)
        result_dataframe = dataframe.withColumn("concatenated",concat(*[coalesce(col(c), lit("null")) for c in columns_to_concatenate]))
        # Add a new column with the concatenated values
        result_dataframe = result_dataframe.withColumn("integration_key", md5(result_dataframe['concatenated']))
        result_dataframe = result_dataframe.drop("concatenated")
        print("MD5 hash created")
        return result_dataframe
    except Exception as e:
        print(str(traceback.format_exc()))
        raise Exception()

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
        logging_input_params_insert["adfRunId"] = str(adf_run_id)
        logging_input_params_insert["adfJobId"] = str(adf_job_id)
        logging_input_params_insert["Layer"] = LAYER
        logging_input_params_insert["Env"] = environment
        logging_input_params_insert["pipeline_name"] = pipeline_name
        logging_input_params_insert["process_name"] = process_name
        logging_input_params_insert["interface_name"] = interface_name
        logging_input_params_insert["NotebookLocation"] = notebook_location
        logging_input_params_insert["DatabricksJobId"] = str(databricks_job_id)
        logging_input_params_insert["DatabricksRunId"] = str(databricks_run_id)

        print("Successfully created logging info")                
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: logging_input_params_insert}
    except Exception as e:
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

def subset_columns_list(latest_records):
    global subset_columns
    try:
        print(f"Housekeeping columns list: {housekeeping_columns}")
        actual_columns = latest_records.columns
        print(f"Actuals columns list: {actual_columns}")
        subset_columns = [column for column in actual_columns if column not in housekeeping_columns]
        print(f"Subset columns list: {subset_columns}")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise Exception()

# COMMAND ----------

# DBTITLE 1,Process DML foreach batch
def process_batch(df, epoch_id):
    """
    Purpose     :   Execute DML
    Input       :   Bronze Layer Dataframe
                    
    Output      :   Return Silver Layer Dataframe
    """
    try:
        TEMP_TABLE_NAME = options['target_table_name'].split('.')[-1]+"_bronze_table_temp"
        df = df.dropDuplicates(subset = subset_columns)
        df.createOrReplaceTempView(TEMP_TABLE_NAME)
        options['bronze_temp_table_name'] = TEMP_TABLE_NAME
    
        # Define multiple Spark SQL statements
        sql_statements = silver_dml
        
        # Reading SQL File and separating queries separated by a ';' 
        sql_content_list = [command.strip() for command in sql_statements.split(';') if command.strip()]
        
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
        # Read Bronze table into stream using autoloader
        latest_records=spark.readStream.option("ignoreDeletes", "true").table(source_table_identifier)
        subset_columns_list(latest_records)
        
        columns_to_drop=["dq_details", "dq_record_status","record_load_time"]
        
        if enable_dq_checks == 'Y':
            columns_to_drop_invalid=["record_load_time","date_part","hour_part"]
            #Run DQ on the source table to identifybad records
            print("DQ Check Started")
            dq_config_df = fetchConfigDF(spark,dq_config_table_name, dq_config_id, connection_params['result'])
            dq_checks = get_dq_check(spark,dq_config_df)
            dq = next(iter(dq_checks.values()))
            print(f"DQ CHECKS VALUES: {dq}")
            df_out, dq_query = data_quality_runner(spark,latest_records, dq)
            print("DQ Check Completed")

            # Create MD5 hash column
            if md5_creation_flag == 'Y':
                df_out = create_md5_column(df_out)

            # Write valid 
            print(f"Writing records to valid_rec_tbl: {target_table_identifier}")
            streaming_query = df_out.\
            filter(col(DQ_STATUS_COLUMN_NAME) == "valid")\
                .drop(*columns_to_drop)\
                .writeStream \
                .foreachBatch(process_batch) \
                .outputMode("append")\
                .option("checkpointLocation", checkpoint_file_path_silver)\
                .option("ignoreDeletes", "true")\
                .trigger(availableNow=True)\
                .start()
            
            print(f"Writing records to invalid_rec_tbl: {target_table_identifier}")
            # Write invalid records
            streaming_query2 = df_out.\
            filter(col(DQ_STATUS_COLUMN_NAME) == "invalid")\
                .withColumn(DATABRICKS_JOB_ID_COLUMN, lit(databricks_job_id))\
                .withColumn(DATABRICKS_RUN_ID_COLUMN, lit(databricks_run_id))\
                .drop(*columns_to_drop_invalid)\
                .writeStream.outputMode("append")\
                .option("checkpointLocation", checkpoint_file_path_silver_invalid)\
                .option("ignoreDeletes", "true")\
                .option("tblproperties", "delta.feature.allowColumnDefaults=supported")\
                .trigger(availableNow=True)\
                .toTable(target_table_identifier + '_invalid')


            #wait for writestream to terminate before executing further statements
            streaming_query.awaitTermination()
            print(f"Successfully merged  records to the valid_rec_tbl: {target_table_identifier}")
            streaming_query2.awaitTermination()
            print(f"Successfully merged  records to the invalid_rec_tbl: {target_table_identifier + '_invalid'}")

            # Read table to generate DQ summary
            df_valid  = spark.read.table(target_table_identifier)
            df_invalid = spark.read.table(target_table_identifier + '_invalid')
            # Filter the dataframe for given Databricks run ID  and job ID
            df_valid = df_valid.filter((df_valid.DATABRICKS_JOB_ID == databricks_job_id) & (df_valid.DATABRICKS_RUN_ID == databricks_run_id)).withColumn("dq_record_status", lit("valid"))
            df_invalid = df_invalid.filter((df_invalid.DATABRICKS_JOB_ID == databricks_job_id) & (df_invalid.DATABRICKS_RUN_ID == databricks_run_id))
            # Pass it to DQ summary method
            dq_summary = get_dq_summary(spark, df_valid, df_invalid)
            # insert dq_summary in the details column
            logging_input_params_insert['details'] = str(dq_summary)
            
        else:
            # Create MD5 hash column
            if md5_creation_flag == 'Y':
                latest_records = create_md5_column(latest_records)
            streaming_query = latest_records\
                .drop(*columns_to_drop)\
                .writeStream \
                .foreachBatch(process_batch) \
                .outputMode("append")\
                .option("checkpointLocation", checkpoint_file_path_silver)\
                .option("ignoreDeletes", "true")\
                .trigger(availableNow=True)\
                .start()
            streaming_query.awaitTermination()
        
                
        print("Extracting counts of source table and target table")
        #target table count is extarcted using the databricks run and job id
        target_table_count = spark.sql(f"select count(1) as count from {target_table_identifier} where DATABRICKS_RUN_ID = {databricks_run_id} and DATABRICKS_JOB_ID = {databricks_job_id}").collect()[0]["count"]
        logging_input_params_insert["SourceTableCount"] = streaming_query.lastProgress["sources"][0]["numInputRows"]
        logging_input_params_insert["TargetTableCount"] = target_table_count

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

# DBTITLE 1,Main
"""
#         Purpose     :   This function reads the configuration from given config file path
#         Input       :   config file path
#         Output      :   Return status SUCCESS/FAILED
"""
print("Started data ingestion from Bronze to Silver")
 
connection_params = prepare_sql_connection_info()
print(connection_params)
if connection_params[STATUS_KEY] == FAILED_KEY:
    raise Exception()
 
output = prepare_logging_info()
# print(output)
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception()

# trigger
output = source_to_target_data_load()
if output[STATUS_KEY] == FAILED_KEY:
    raise Exception()
