# Databricks notebook source
# DBTITLE 1,Importing Libraries
from pyspark.sql import *
import re

# COMMAND ----------

# DBTITLE 1,Read SQL File
# Function to read SQL file from the path
def read_file(file_path):
    with open(file_path, 'r') as file:
        file_content = file.read()
    return file_content

# COMMAND ----------

# DBTITLE 1,Execute SQL Query
def execute_query(sql_content):
    # Execute the SQL query using spark.sql()
    result_df = spark.sql(sql_content)

    # Display the actual values by calling the show() method
    return result_df

# COMMAND ----------

# DBTITLE 1,MAIN
# Main function
def execute_file_from_path(file_path,options=None):
    
    # File path to the SQL file
    sql_file_path = file_path
 
    try:

        # Read SQL file
        sql_content = read_file(sql_file_path)
        pattern = r';\s*$'
        sql_content_list = re.split(r';\n', sql_content)
        # sql_content_list = [command.strip() for command in sql_content.split(';') if command.strip()]
        print(sql_content_list)
        for sql_command in sql_content_list:
            sql_command = sql_command.strip()
            
            if sql_command != "":
        # Replace placeholders in the SQL content with values from options
                if options:
                    for key, value in options.items():
                        sql_command = sql_command.replace(f"$${key}", str(value))

                print(sql_command)                
                # Execute DML
                result_df = execute_query(sql_command)
                print('dml_status :')
                result_df.show()
 
        print("DML execution completed successfully.")
        # return(result_df)
    except Exception as e:
        print(f"Error: {str(e)}")
        raise Exception()
