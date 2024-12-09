# Databricks notebook source
# ################################################## Module Information ################################################
#   Module Name         :   Reconcilation 
#   Purpose             :   This module performs below operation:
#                               a. Calculate, compares and validates the count for all layers for every table
#                               b. Publishes a recon summary of all the validation in an email
#                                
#   Input               :   Recon queries for all the tables
#   Output              :   Recon summary Email
#   Pre-requisites      :   
#   Last changed on     :   03 Apr, 2024
#   Last changed by     :   Prakhar Chauhan
#   Reason for change   :   Added Recon for Fact, P&F and Aggregates table
# ######################################################################################################################

# COMMAND ----------

# DBTITLE 1,Importing Libraries/Functions
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from collections import defaultdict
from IPython.display import HTML
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Declaring variables
pipeline_config_table = dbutils.widgets.get("pipeline_config_table_name")
audit_table = dbutils.widgets.get("logging_table_name")
dq_config_table = dbutils.widgets.get("dq_config_table_name")
database_host = dbutils.widgets.get("database_host")
database_port = dbutils.widgets.get("database_port")
database_name = dbutils.widgets.get("database_name")
process_name = dbutils.widgets.get("process_name")
db_secrets = dbutils.widgets.get("db_secret_scope")
db_user_key = dbutils.widgets.get("user_secret_key")
db_password_key = dbutils.widgets.get("pw_secret_key")
catalog_id = dbutils.widgets.get("catalog_id")
user = dbutils.secrets.get(scope=db_secrets,key=db_user_key)
password = dbutils.secrets.get(scope=db_secrets,key=db_password_key)

# COMMAND ----------

# DBTITLE 1,Notebook Constants
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name}"
summary = []
formatted_time = datetime.now().strftime("%d%m%Y %H:%M:%S")

# COMMAND ----------

# Define method to create dataframes for SQL database tables
def read_database_tables(spark, table_name):
  data_table = spark.read \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .load()
  return data_table

# Load SQL Database tables
pipeline_config_table_df = read_database_tables(spark, pipeline_config_table)
audit_table_df = read_database_tables(spark, audit_table)
dq_config_table_df = read_database_tables(spark, dq_config_table)

# Create temp views to run reconcilation
pipeline_config_table_df.createOrReplaceTempView("pipeline_config_table")
audit_table_df.createOrReplaceTempView("audit_table")
dq_config_table_df.createOrReplaceTempView("dq_config_table")


# Load all the Gold recon queries from pipeline config dataframe for the input process name
def load_recon_queries(spark, config_df):
  query_dict = dict(config_df.filter((config_df.interface_active == 'Y') & (config_df.process_name == process_name)).select("gold_table_name", "gold_recon_query").collect())
  return query_dict

# gold_recon_query = load_recon_queries(spark, pipeline_config_table_df) # Uncomment this part when recon queries are pushed to Pipeline config table

if process_name == 'BHP_Master_Table':
    gold_recon_query = {
        f"{catalog_id}.psas_fdp_usp_gold.t_sales_category" : {
            "silver_query" : f"select count(1) as count, 'SALES_CTGRY' as col from (select distinct SALES_CTGRY_ID from {catalog_id}.psas_fdp_usp_silver.t_sales_category) A", 
            "gold_query" : f"select count(1) as count, 'SALES_CTGRY' as col from (select distinct SALES_CTGRY_ID from {catalog_id}.psas_fdp_usp_gold.t_sales_category where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.t_buying_grp" : {
            "silver_query" : f"select count(1) as count, 'BUYING_GRP' as col from (select distinct BUYING_GRP_ID from {catalog_id}.psas_fdp_usp_silver.t_buying_grp) A", 
            "gold_query" : f"select count(1) as count, 'BUYING_GRP' as col from (select distinct BUYING_GRP_ID from {catalog_id}.psas_fdp_usp_gold.t_buying_grp where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.t_product_hierarchy_1" : {
            "silver_query" : f"select count(1) as count, 'PROD' as col from (select distinct PROD_ID from {catalog_id}.psas_fdp_usp_silver.t_product_hierarchy_1) A", 
            "gold_query" : f"select count(1) as count, 'PROD' as col from (select distinct PROD_ID from {catalog_id}.psas_fdp_usp_gold.t_product_hierarchy_1 where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.t_comp_code" : {
            "silver_query" : f"select count(1) as count, 'CMPNY' as col from (select distinct CMPNY_CD from {catalog_id}.psas_fdp_usp_silver.t_comp_code) A", 
            "gold_query" : f"select count(1) as count, 'CMPNY' as col from (select distinct CMPNY_CD from {catalog_id}.psas_fdp_usp_gold.t_comp_code where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.t_common_grp" : {
            "silver_query" : f"select count(1) as count, 'COMMON_GRP' as col from (select distinct COMMON_GRP_ID from {catalog_id}.psas_fdp_usp_silver.t_common_grp) A", 
            "gold_query" : f"select count(1) as count, 'COMMON_GRP' as col from (select distinct COMMON_GRP_ID from {catalog_id}.psas_fdp_usp_gold.t_common_grp where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.t_cust_chain_id" : {
            "silver_query" : f"select count(1) as count, 'CUST_CHAIN' as col from (select distinct CUST_CHAIN_ID from {catalog_id}.psas_fdp_usp_silver.t_cust_chain_id) A", 
            "gold_query" : f"select count(1) as count, 'CUST_CHAIN' as col from (select distinct CUST_CHAIN_ID from {catalog_id}.psas_fdp_usp_gold.t_cust_chain_id where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.t_common_entity" : {
            "silver_query" : f"select count(1) as count, 'PARNT' as col from (select distinct PARNT_ID from {catalog_id}.psas_fdp_usp_silver.t_common_entity) A", 
            "gold_query" : f"select count(1) as count, 'PARNT' as col from (select distinct PARNT_ID from {catalog_id}.psas_fdp_usp_gold.t_common_entity where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.q_customer" : {
            "silver_query" : f"select count(1) as count, 'CUSTOMER' as col from (select distinct CUST_ID, OBJ_VRS, EXPIRATION_DATE from {catalog_id}.psas_fdp_usp_silver.q_customer) A", 
            "gold_query" : f"select count(1) as count, 'CUSTOMER' as col from (select distinct CUST_ID, OBJ_VRS, EXPIRATION_DATE from {catalog_id}.psas_fdp_usp_gold.q_customer where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.t_cust_segment" : {
            "silver_query" : f"select count(1) as count, 'CUST_SGMT' as col from (select distinct CUST_SGMT_ID from {catalog_id}.psas_fdp_usp_silver.t_cust_segment) A", 
            "gold_query" : f"select count(1) as count, 'CUST_SGMT' as col from (select distinct CUST_SGMT_ID from {catalog_id}.psas_fdp_usp_gold.t_cust_segment where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.q_ven_compc" : {
            "silver_query" : f"select count(1) as count, 'CMPNY_CD' as col from (select distinct CMPNY_CD, VNDR_CMPNY_CD, OBJ_VRS, EXPIRATION_DATE from {catalog_id}.psas_fdp_usp_silver.q_ven_compc) A", 
            "gold_query" : f"select count(1) as count, 'CMPNY_CD' as col from (select distinct CMPNY_CD, VNDR_CMPNY_CD, OBJ_VRS, EXPIRATION_DATE from {catalog_id}.psas_fdp_usp_gold.q_ven_compc where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.t_manufacturer" : {
            "silver_query" : f"select count(1) as count, 'VENDOR' as col from (select distinct VENDOR_ID from {catalog_id}.psas_fdp_usp_silver.t_manufacturer) A", 
            "gold_query" : f"select count(1) as count, 'VENDOR' as col from (select distinct VENDOR_ID from {catalog_id}.psas_fdp_usp_gold.t_manufacturer where CURR_FLG <> 'NA') A"
            },
        f"{catalog_id}.psas_fdp_usp_gold.q_material" : {
            "silver_query" : f"select count(1) as count, 'MATERIAL' as col from (select distinct MATERIAL, OBJ_VRS, EXPIRATION_DATE from {catalog_id}.psas_fdp_usp_silver.q_material) A", 
            "gold_query" : f"select count(1) as count, 'MATERIAL' as col from (select distinct MATERIAL, OBJ_VRS, EXPIRATION_DATE from {catalog_id}.psas_fdp_usp_gold.q_material where CURR_FLG <> 'NA') A"
            }
    }

if process_name == 'BHP_Delta_DSO_Table':
    gold_recon_query = {
        f"{catalog_id}.psas_fdp_usp_gold.t_copa_actuals_combined" : {
            "silver_query" : f"select count(1) as count, 'SRC_RAW_DATA' as col from (select distinct COPA_DOC_NUM , COPA_DOC_ITEM_NUM , FISCAL_YR_PERIOD from {catalog_id}.psas_fdp_usp_silver.t_copa_raw_delta_dso) A", 
            "gold_query" : f"select count(1) as count, 'SRC_RAW_DATA' as col from (select distinct COPA_DOC_NUM , COPA_DOC_ITEM_NUM , FISCAL_YR_PERIOD from {catalog_id}.psas_fdp_usp_gold.t_copa_actuals_combined where SRC_LOAD_DT_RAW is not null OR SRC_LOAD_DT_RAW != '') A"
        }
    }

if process_name == 'BHP_COPA_BEX_ACTUALS':
    gold_recon_query = {
        f"{catalog_id}.psas_fdp_usp_gold.t_copa_actuals_combined" : {
            "silver_query" : f"select count(1) as count, 'SRC_BEX_DATA' as col from (select distinct COPA_DOC_NUM , COPA_DOC_ITEM_NUM , FISCAL_YR_PERIOD from {catalog_id}.psas_fdp_usp_silver.t_copa_bex_actuals) A", 
            "gold_query" : f"select count(1) as count, 'SRC_BEX_DATA' as col from (select distinct COPA_DOC_NUM , COPA_DOC_ITEM_NUM , FISCAL_YR_PERIOD from {catalog_id}.psas_fdp_usp_gold.t_copa_actuals_combined where SRC_LOAD_DT_BEX is not null OR SRC_LOAD_DT_BEX != '') A"
        }
    }

if process_name == 'BPC_Forecast_Plan':
    gold_recon_query = {
        f"{catalog_id}.psas_fdp_usp_gold.t_day7_forecast_and_plan" : {
            "silver_query" : f"select count(1) as count, 'DAY_7_FORECAST' as col from {catalog_id}.psas_fdp_usp_silver.t_day7_forecast_and_plan", 
            "gold_query" : f"select count(1) as count, 'DAY_7_FORECAST' as col from {catalog_id}.psas_fdp_usp_gold.t_day7_forecast_and_plan"
        },
        f"{catalog_id}.psas_fdp_usp_gold.t_day17_forecast_and_plan" : {
            "silver_query" : f"select count(1) as count, 'DAY_17_FORECAST' as col from {catalog_id}.psas_fdp_usp_silver.t_day17_forecast_and_plan", 
            "gold_query" : f"select count(1) as count, 'DAY_17_FORECAST' as col from {catalog_id}.psas_fdp_usp_gold.t_day17_forecast_and_plan"
        }
    }

if process_name == 'COPA_REPORTING_AGGREGATE':
    gold_recon_query = {
        f"{catalog_id}.psas_fdp_usp_gold.t_copa_actuals_aggregate" : {
            "silver_query" : f"select sum(GROSS_PROFIT) as count, 'GROSS_PROFIT' as col from {catalog_id}.psas_fdp_usp_gold.t_copa_actuals_combined where FISCAL_YR in ('2022', '2023', '2024')", 
            "gold_query" : f"select sum(GROSS_PROFIT) as count, 'GROSS_PROFIT' as col from {catalog_id}.psas_fdp_usp_gold.t_copa_actuals_aggregate where FISCAL_YR in ('2022', '2023', '2024')"
        },
        f"{catalog_id}.psas_fdp_usp_gold.t_copa_actuals_aggregate" : {
            "silver_query" : f"select sum(NET_SLS) as count, 'NET_SALES' as col from {catalog_id}.psas_fdp_usp_gold.t_copa_actuals_combined where FISCAL_YR in ('2022', '2023', '2024')", 
            "gold_query" : f"select sum(NET_SLS) as count, 'NET_SALES' as col from {catalog_id}.psas_fdp_usp_gold.t_copa_actuals_aggregate where FISCAL_YR in ('2022', '2023', '2024')"
        }
    }

# COMMAND ----------

def evaluate(source_count=None,landing_count=None,bronze_count=None,silver_count=None, gold_dict=None):
    status = []
    failure_flag = False
    if source_count is not None and landing_count is not None and bronze_count is not None and silver_count is not None:
        if source_count == landing_count:
            status.append('source to landing reconciliation is successfull')
        else:
            status.append('source to landing reconciliation is failed')
            failure_flag = True
        if landing_count == bronze_count:
            status.append('landing to bronze reconciliation is successfull')
        else:
            status.append('landing to bronze reconciliation is failed')
            failure_flag = True
        if bronze_count == silver_count:
            status.append('bronze to silver reconciliation is successfull')
        else:
            status.append('bronze to silver reconciliation is failed')
            failure_flag = True
        if gold_dict is not None:
            key = gold_dict['gold_key']
            if gold_dict['gold_count'] == gold_dict['silver_count']:
                status.append(f'silver to gold reconciliation for the count of {key} column is successfull')
            else:
                status.append(f'silver to gold reconciliation for the count of {key} column is failed')
                failure_flag = True
        if failure_flag:
            status.append('overall status failed')
        else:
            status.append('overall status success')
    return status


def get_count_from_audit(layer,table_name):
    # Get source table count for layer
    count = None
    if table_name is not None:
        query_source_count = f"select SourceTableCount as source_count, TargetTableCount as target_count from all_audit_records where layer = '{layer}' and SourceTable = '{table_name}'"
        if spark.sql(query_source_count).count() == 0:
            source_count, target_count = 0, 0
        else:
            source_count = spark.sql(query_source_count).collect()[0]["source_count"]
            target_count = spark.sql(query_source_count).collect()[0]["target_count"]
    else:
        source_count, target_count = 0, 0
    return source_count,target_count


def get_gold_count(target_table, process_name):
    # Get gold target table count for silver layer
    count_dict = {}
    
    if target_table is not None:
        print(target_table)
        silver_query = gold_recon_query[target_table]['silver_query']
        silver_result = spark.sql(silver_query).collect()[0]
        gold_query = gold_recon_query[target_table]['gold_query']
        gold_result = spark.sql(gold_query).collect()[0]
        count_dict = {'silver_key' : silver_result["col"],
                      'silver_count' : silver_result["count"],
                      'gold_key' : gold_result["col"],
                      'gold_count' : gold_result["count"]
        }
    return count_dict

# COMMAND ----------

def process_row(row):
    # row is a Row object, and you can access column values by name or index
    # For example, row["id"], row["name"], row["age"]   
    # Create a dictionary for the row

    gold_flag_col = 'is_gold_enable'
    row_dict = {}
    source_count,landing_count = get_count_from_audit("landing",row["source_table_name"])
    bronze_source_count,bronze_count = get_count_from_audit("bronze",row["source_table_name"]) 
    silver_source_count,silver_count = get_count_from_audit("silver",row["bronze_table_name"]) 
    gold_dict = None
    if row[gold_flag_col].lower() == 'y':
        gold_dict = get_gold_count(row["gold_table_name"], process_name)
        row_dict= {"dataset": row["landing_target_file_nm"],
                    "date_time" : formatted_time, 
                    "count_info" :{
                            "source" : {"identifier": row["source_table_name"], "count": source_count},
                            "landing": {"identifier": row["landing_target_full_path"] , "count": landing_count},
                            "bronze": {"identifier": row["bronze_table_name"] , "count": bronze_count },
                            "silver": {"identifier": row["silver_table_name"] , "count": silver_count },
                            "gold": {"identifier": row["gold_table_name"] , "count": gold_dict }
                            }
                    }
    else:
        row_dict = { "dataset": row["landing_target_file_nm"],
                    "date_time" : formatted_time,
                    "count_info" :{
                        "source" : {"identifier": row["source_table_name"], "count": source_count},
                        "landing": {"identifier": row["landing_target_full_path"] , "count": landing_count},
                        "bronze": {"identifier": row["bronze_table_name"] , "count": bronze_count },
                        "silver": {"identifier": row["silver_table_name"] , "count": silver_count }
                        }
                    }
    row_dict["summary"] = evaluate(source_count=source_count,landing_count=landing_count,bronze_count=bronze_count,silver_count=silver_count,gold_dict=gold_dict) 
    return row_dict

# COMMAND ----------

# Extarct table names from pipeline configs to run the reconciliation on
p_result = spark.sql(f"SELECT landing_target_file_nm, source_table_name, bronze_table_name, silver_table_name, landing_target_full_path, gold_table_name, is_gold_enable FROM pipeline_config_table where process_name = '{process_name}' and interface_active = 'Y'")
# in ('9','10','11','13')

# For each table in process get the latest records for landing,bronze and silver from audit table
query = f"SELECT * from (select *, row_number() over(partition by SourceTable,process_name,layer order by StartTime desc) rank FROM audit_table where lower(ExecutionStatus) != 'started' and process_name = '{process_name}') where rank=1"
all_audit_records = spark.sql(query)
all_audit_records.createOrReplaceTempView("all_audit_records")

# all_audit_records_collect = all_audit_records.collect()

p_result_collect = p_result.collect()
for row in p_result_collect:
    print(row)
    summary.append(process_row(row))

# Load Recon Summary in a table
schema = StructType([
    StructField("dataset", StringType(), True),
    StructField("date_time", StringType(), True),
    StructField("count_info", StructType([
        StructField("source", StructType([
            StructField("identifier", StringType(), True),
            StructField("count", StringType(), True)
        ]), True),
        StructField("landing", StructType([
            StructField("identifier", StringType(), True),
            StructField("count", StringType(), True)
        ]), True),
        StructField("bronze", StructType([
            StructField("identifier", StringType(), True),
            StructField("count", StringType(), True)
        ]), True),
        StructField("silver", StructType([
            StructField("identifier", StringType(), True),
            StructField("count", StringType(), True)
        ]), True),
        StructField("gold", StructType([
            StructField("identifier", StringType(), True),
            StructField("count", StructType([
                StructField("silver_key", StringType(), True),
                StructField("silver_count", StringType(), True),
                StructField("gold_key", StringType(), True),
                StructField("gold_count", StringType(), True)
        ]), True)
        ]), True),
    ]), True),
    StructField("summary", ArrayType(StringType(), True), True),
])

df = spark.createDataFrame(data=summary, schema=schema)
df.createOrReplaceTempView("recon_summary")
# delta_path = "abfss://cfod@stpsascfoddev01.dfs.core.windows.net/gold/psas/usp/sap/finance/recon_summary/"
# df.write.format("delta").option("mergeSchema", "true").mode("append").save(delta_path)
# ddl_query = f"""CREATE TABLE if not exists fdp_dev.psas_fdp_usp_gold.recon_summary USING DELTA LOCATION '{delta_path}' """ 
# spark.sql(ddl_query)
# Delta table Name fdp_dev.psas_fdp_usp_gold.recon_summary

# COMMAND ----------

display(
"""select dataset, date_time,
element_at(summary, size(summary)) as status,
count_info.source.count as source_count,
count_info.source.identifier as source_table,
count_info.landing.count as landing_count,
count_info.landing.identifier as landing_table,
count_info.bronze.count as bronze_count,
count_info.bronze.identifier as bronze_table,
count_info.silver.count as silver_count,
count_info.silver.identifier as silver_table,
count_info.gold.count as gold_count,
count_info.gold.identifier as gold_table,
summary
from recon_summary""")

# COMMAND ----------

table_data = []
def prepare_html_summary():
    global summary
    for entry in summary:
        dataset_name = entry["dataset"]
        
        # Format count_info as an HTML table with styling
        count_info_html = "<table style='border-collapse: collapse; width: 100%;'><tr style='border: 1px solid #ddd;'><th style='padding: 8px; text-align: left;'>Field</th><th style='padding: 8px; text-align: left;'>Value</th></tr>"
        for field, value in entry.items():
            print(field,value)
            if field == 'count_info':
                for f, v in value.items():
                    count_info_html += f"<tr style='border: 1px solid #ddd;'><td style='padding: 8px; text-align: left;'>{f}</td><td style='padding: 8px; text-align: left;'>{v['count']}</td></tr>"
                count_info_html += "</table>"
        
        summary = entry['summary']
        table_data.append({'Dataset': dataset_name, 'count_info': count_info_html, 'summary': summary})

    # Create the main HTML table with styling
    html_table = "<table style='border-collapse: collapse; width: 100%;'><tr style='border: 1px solid #ddd;'><th style='padding: 8px; text-align: left;'>Dataset</th><th style='padding: 8px; text-align: left;'>Count Info</th><th style='padding: 8px; text-align: left;'>Summary</th></tr>"
    for entry in table_data:
        html_table += f"<tr style='border: 1px solid #ddd;'><td style='padding: 8px; text-align: left;'>{entry['Dataset']}</td><td style='padding: 8px; text-align: left;'>{entry['count_info']}</td><td style='padding: 8px; text-align: left;'>{entry['summary']}</td></tr>"
    html_table += "</table>"
    return html_table

# COMMAND ----------

# Display the styled HTML table
html_table = prepare_html_summary()
HTML(html_table)

# COMMAND ----------

# Raising Exception in the notebook in case of any error 
response = {"summary": html_table}
dbutils.notebook.exit(response)
