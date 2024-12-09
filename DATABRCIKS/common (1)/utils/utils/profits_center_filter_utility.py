# Databricks notebook source
# Import Row for creating row objects in Spark DataFrames
# Import collect_list to aggregate values into a list within a DataFrame column
from pyspark.sql import Row
from pyspark.sql.functions import collect_list

# COMMAND ----------

# Retrieve the segment name from the notebook's widgets
segment_name = dbutils.widgets.get("segment_name").upper()
env = dbutils.widgets.get("env")
padding_length = int(dbutils.widgets.get("padding_length"))
 
# Execute SQL query to select distinct LEAF_NODE values for the given segment
df = spark.sql(f"""
    SELECT DISTINCT LEAF_NODE
    FROM fdp_{env}.psas_fdp_all_gold.t_msh_center_hier_level_leaf
    WHERE SEGMENT = '{segment_name}'
""")
 
# Collect the LEAF_NODE values into a list
profit_ctr_list = df.select(collect_list("LEAF_NODE")).first()[0]
 

# Format the list of LEAF_NODE values into a string suitable for SQL queries
padded_ctr_list = [num.zfill(padding_length) for num in profit_ctr_list]
profit_ctr_str = ', '.join([f"'{profit_ctr}'" for profit_ctr in padded_ctr_list])

 
# Enclose the formatted string in parentheses
formatted_str = f'({profit_ctr_str})'
 
# Exit the notebook and return the formatted string
dbutils.notebook.exit(formatted_str)
