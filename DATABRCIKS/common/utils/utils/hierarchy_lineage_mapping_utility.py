# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Hierarchy data lineage mapping in gold table
#   Purpose             :   This module performs below operation:
#                               a. Hierarchy data lineage mapping between gold table and mapping tables.
#                                
#   Input               :   Source table name, target table name,gold_dml and other inputs
#   Output              :   
#   Pre-requisites      :   
#   Last changed on     :   18 Jun, 2024
#   Last changed by     :   Yash Patil
#   Reason for change   :   NA
# ######################################################################################################################

# COMMAND ----------

# DBTITLE 1,Importing Functions/Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType
from pyspark.sql.functions import collect_set
import json
from datetime import datetime
import traceback
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from logging_utility import *
import ast
from pyspark.sql.functions import to_date

# COMMAND ----------

# DBTITLE 1,Notebook Constants
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCEEDED"
ERROR_KEY = "error"
EXECUTION_STATUS_KEY = "ExecutionStatus"
END_TS_KEY = "EndTime"
LAYER = "gold"
TEMP_TABLE_NAME = "silver_table"
# Housekeeping column names
DATABRICKS_RUN_ID_COLUMN = "DATABRICKS_RUN_ID"
DATABRICKS_JOB_ID_COLUMN = "DATABRICKS_JOB_ID"

# COMMAND ----------

# DBTITLE 1,Declaring Variables
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
adf_run_id = dbutils.widgets.get("adf_run_id")
adf_job_id = dbutils.widgets.get("adf_job_id")

# Logging details
LOGGING_INGESTION_MASTER_TABLE_NAME = f"{dbutils.widgets.get('logging_table_name')}"
lineage_tbl_name = dbutils.widgets.get("lineage_tbl_name")
database_host = dbutils.widgets.get("database_host")
database_port = dbutils.widgets.get("database_port")
database_name = dbutils.widgets.get("database_name")
db_secret_scope = dbutils.widgets.get("db_secret_scope")
user_secret_key = dbutils.secrets.get(scope=db_secret_scope,key = dbutils.widgets.get("user_secret_key"))
pw_secret_key = dbutils.secrets.get(scope=db_secret_scope,key = dbutils.widgets.get("pw_secret_key"))
process_name = dbutils.widgets.get("process_name")
# interface_name = dbutils.widgets.get("interface_name")
logging_input_params_insert = {}
logging_connection_params = {}

# Application Variables
environment = dbutils.widgets.get("env")
catalog_name = 'fdp_'+environment
options['catalog_name'] = catalog_name
table_identifier = dbutils.widgets.get("table_identifier")
# source_table_identifier = dbutils.widgets.get("source_table_identifier")
pipeline_start_time = datetime.strptime(dbutils.widgets.get("pipeline_start_time")[:-4] + dbutils.widgets.get("pipeline_start_time")[-1],"%Y-%m-%dT%H:%M:%S.%fZ")
current_timestamp = datetime.utcnow()
gold_dml = dbutils.widgets.get('gold_dml')
catgry = dbutils.widgets.get('category')
sub_catgry = dbutils.widgets.get('sub_category')

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
         logging_connection_params["lineage_tbl_name"] = lineage_tbl_name
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
        
        # logging_input_params_insert['SourceTable'] = source_table_identifier
        logging_input_params_insert['TargetTable'] = table_identifier
        logging_input_params_insert["StartTime"] = current_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logging_input_params_insert["adfRunId"] = adf_run_id
        logging_input_params_insert["adfJobId"] = adf_job_id
        logging_input_params_insert["Layer"] = LAYER
        logging_input_params_insert["Env"] = environment
        logging_input_params_insert["pipeline_name"] = pipeline_name
        logging_input_params_insert["process_name"] = process_name
        # logging_input_params_insert["interface_name"] = interface_name
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

def get_data_from_server(spark, configs):
  # Set up Params
  conn_params = configs
  driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  database_host = conn_params["database_host"]
  database_port = conn_params["database_port"]
  database_name = conn_params["database_name"]
  table = conn_params["lineage_tbl_name"]
  user = conn_params["user"]
  password = conn_params["password"]
  url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name}"

  try:
      config_df = (spark.read
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()
      )
      
      print("Successfully extracted config data")   
      # Return the status
      return {STATUS_KEY: SUCCESS_KEY}, config_df
  except Exception as e:
      error_message = status_message + ": " + str(traceback.format_exc())
      print(error_message)
      return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,Group/Sub-group lineage mapping
def category_lineage_map(df_tbl, config_df):
    """This function takes a hierarchy table as input (df), and lineage mapping (config_df) from Azure SQL DB and performs mapping on extracted lineage from hierarchy table df and compares lineage with config_df and populates category and Sub category columns.

    Args:
        df : The input DataFrame to be processed.
        config_df : df contains mapping to populate category and Sub category columns.

    Returns:
        mapped_df: Original dataframe with 2 appended columns category and Sub category.
    """
    try:
        category = catgry
        sub_category = sub_catgry
        status_message = f"Starting method to map {category} and {sub_category} lineage"
        print(status_message)

        mapped_data = []
        #Iterating over df rows
        for row in df_tbl.collect():
            new_row = row.asDict()
            row_values = []
            #Iterating over each column in a row
            for col_name in df_tbl.columns:
                if col_name.startswith('LVL_'):
                    value = row[col_name]
                    if not value.startswith('MCK1'):
                        row_values.append(value)
                    #Removing empty string objects and extra spaces from objects in the array
                    row_values = [val.strip().lower().replace("\r\n", "").replace("\r","").replace("\n","") for val in row_values if val]
            row_set = list(set(row_values))
            #Iterating over config_df rows to map lineages for categorys and sub-categorys
            for config_row in config_df.collect():
                #Initializing set objects for category and subcategory lineages
                if config_row["lineage"] is None:
                    lineage = set(ast.literal_eval("[]"))
                else:
                    lineage = set(ast.literal_eval(config_row["lineage"].lower()))
                #Checking if row contains any of the category or sub_category lineages from config_df
                if row["LVL_1"] == config_row["parent_node"]:
                    if lineage.issubset(row_set) and config_row[sub_category.lower()] is not None:
                        new_row[sub_category] = config_row[sub_category.lower()]
                        new_row[category] = config_row[category.lower()]
                        new_row["LEAF_NODE"] = new_row["LEAF_NODE"]
            #Appending the row to the mapped_data array
            mapped_data.append(new_row)
        mapped_df = spark.createDataFrame(mapped_data, df_tbl.schema)
        return {STATUS_KEY: SUCCESS_KEY}, mapped_df
    except Exception as e:
        status_message = "Error in method to map category and subcategory lineage"
        error_message = status_message + ": " + str(traceback.format_exc())
        print(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

# COMMAND ----------

# DBTITLE 1,Flatten Data and Perform Data Copy
def target_data_load(df):    
    """This function takes a Dataframe as input, and creates table in gold layer for mapped data

    Args:
        df: The input DataFrame to be loaded.

    Returns:
        Return status SUCCESS/FAILED
    """
    global logging_input_params_insert
    try:
        status_message = "Starting method to perform data truncate and load"
        print(status_message)
        options["table_identifier"] = table_identifier
        #Inserting the mapped data into a table
        df.createOrReplaceTempView('hierarchy_lineage_view')
        # Read DML from widgets
        sql_statements = gold_dml
        print(sql_statements)
        # Reading SQL File and separating queries separated by a ';' 
        sql_content_list = [command.strip() for command in sql_statements.split(';') if command.strip()]
        # sql_content_list = sql_content.split(';')
        for sql_command in sql_content_list:
            sql_command = sql_command.strip()
            if sql_command != "":
                # Replace placeholders in the SQL content with values from options
                if options:
                    for key, value in options.items():
                        sql_command = sql_command.replace(f"$${key}", str(value))
                df.sparkSession.sql(sql_command)

        print("Extarcting counts of source table and target table")
        logging_input_params_insert["TargetTableCount"] = spark.sql(f"select count(1) as count from {table_identifier} where DATABRICKS_RUN_ID = {databricks_run_id} and DATABRICKS_JOB_ID = {databricks_job_id}").collect()[0]["count"]

        #Sucess message logging 
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

# DBTITLE 1,main
def main():
    """
    Main function to execute the hierarchy lineage mapping process.
    Returns:
        None
    """
    try:
        print("Started hierarchy lineage mapping from gold hierarchy tbl")
        
        # Prepare SQL connection information 
        connection_params = prepare_sql_connection_info()
        print(connection_params)
        if connection_params[STATUS_KEY] == FAILED_KEY:
            raise Exception()

        # Prepare logging information
        output = prepare_logging_info()
        if output[STATUS_KEY] == FAILED_KEY:
            raise Exception()

        # Get lineage mapping data from server
        status, config_df = get_data_from_server(spark, logging_connection_params)
        if status[STATUS_KEY] == FAILED_KEY:
            raise Exception()

        # Get the mapped data
        df = spark.table(table_identifier)
        status, mapped_df = category_lineage_map(df, config_df)
        if status[STATUS_KEY] == FAILED_KEY:
            raise Exception()

        # Trigger
        output = target_data_load(mapped_df)
        if output[STATUS_KEY] == FAILED_KEY:
            raise Exception()
    
    except Exception as e:
        status_message = "Error in main function: "
        print(f"{status_message}{str(e)}")
        raise Exception()

# Call the main function 
if __name__ == "__main__":
    main()
