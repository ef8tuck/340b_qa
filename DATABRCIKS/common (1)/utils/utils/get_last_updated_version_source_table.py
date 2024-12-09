# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Get last updated version from source table 
#   Purpose             :   This module performs below operation:
#                               a. read history of a table 
#                               b. get max commit version from history
#                               c. return the version as int
#                                
#   Input               :   Source table name
#   Output              :   
#   Pre-requisites      :   
#   Last changed on     :   12 Mar, 2024
#   Last changed by     :   Prakhar Chauhan
#   Reason for change   :   NA
# ######################################################################################################################

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Read the table name
source_table = dbutils.widgets.get("source_table_name")

# COMMAND ----------

# Get the last processed version
latest_version = int(str(DeltaTable.forName(spark, source_table).history(1).head()["version"]))
last_updated_version = {"last_updated_version_source_table": int(latest_version)}

# COMMAND ----------

dbutils.notebook.exit(last_updated_version)
