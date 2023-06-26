# Databricks notebook source
#JPM#import sys
#JPM#import os
import json
#JPM#import re
#JPM#from datetime import datetime,timedelta
import uuid
#JPM#import shutil

#JPM#import pyspark
#JPM#from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
#JPM#import pyspark.sql.functions as F

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### IDF SDK

# COMMAND ----------

# MAGIC %run ../commons/CommonBase

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_Common_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_BaseOneBinary_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_Python_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_ETL_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_BaseThreeBinary_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_BaseTwoBinary_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_Extention_Module

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config File

# COMMAND ----------

dbutils.widgets.text("config_file_name", '', '')
config_file_path = dbutils.widgets.get('config_file_name') 

dbutils.widgets.text("filter_condition", '', '')
filter_condition = dbutils.widgets.get('filter_condition') 

config = json.loads(open(f"/dbfs/mnt/idf-config/{config_file_path}",'r').read())
#config = json.loads(open(f"/dbfs/mnt/wbaidf/configs/{config_file_path}",'r').read())
setup = json.loads(open(f"/dbfs/mnt/idf-config/setup-config/common-setup.json",'r').read())
#config = json.loads(open(f"/dbfs/mnt/wbaidf/PLT_IDF/configs/{config_file_path}",'r').read())
#setup = json.loads(open(f"/dbfs/mnt/wbaidf/configs/common-setup.json",'r').read())
#config = json.loads(open('/dbfs/mnt/wba-idf/configs/sample-customer-orders.json','r').read())
print (config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common

# COMMAND ----------

CURRENT_PARTITION_DATE = generateCurrentDate_fn()

report_path = config.get("report_path")
data_source = config.get("data_source")
data_source_map = dict(map(lambda l: (l.get("data_type","").upper(),l.get("type_specific_details",{})),data_source))
cards_map = dict(map(lambda l: (l.get("card_id",""),l), config.get("cards",[])))

_process_map = {}
_connection = None
overall_status = 0

return_value = {"message" : "ETL Driver OK"}

rulesenginePredefinedObj  = RulesEnginePredefinedClass()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

#spark.conf.set("spark.sql.shuffle.partition",160)
spark.conf.set("spark.sql.files.maxPartitionBytes", 1024*1024*256)
spark.conf.set("spark.sql.files.openCostInBytes", 1)
spark.conf.set("spark.databricks.sqldw.writeSemantics", "copy")
spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled","true")

if 'spark_params' in config:
  for param in config.get("spark_params",[]):
    for k, v in param.items():
      spark.conf.set(k,v)
    #for
  #for
#if
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.autoOptimize.optimizeWrite = true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code

# COMMAND ----------

dependency_graph = buildTreeDependencyGraph_fn(config.get("cards",[]))

for level1_dependency in dependency_graph:
  _tmp_process_map = {}
  
  for level2_dependency in level1_dependency:
    card = cards_map.get(level2_dependency)
    
    if card.get("card_type","").lower() == 'read_delta':
      readDataWithDeltaFormat_fn(spark, card.get('data_path'), card.get("output_table"))
    #if
    
    elif card.get("card_type","").lower() == 'write_delta':
      dataframe = spark.sql(card.get("query"))
      if dataframe == None or len(dataframe.take(1)) == 0:
        RECORDER_SPARK_ETL.append ("IDF_Exception :  {exception} : {trace}".format (exception ="Dataframe Empty", trace = "Check Data Source. No records found."))
        #raise Exception("Error Processing Config :: {config}".format(config=config_file_path))
      #if
      writeToS3WithFilter_fn(spark, dataframe, card.get('data_path'),CURRENT_PARTITION_DATE, card.get("partition_key_columns",None), "DELTA", card.get('mode','overwrite'))
      if is_last_card_fn(card.get("card_id",""),config.get("cards",[])):
        return_value[card.get("label","")] = dataframe.count()
      spark.sql(f"OPTIMIZE delta.`{card.get('data_path')}`")
    #elif
    
    elif card.get("card_type","").lower() == 'delete_delta':
      deleteDeltaData_fn(spark, card.get('table_name'), None, card.get("data_path"), card.get("query"))
      deltaTable = DeltaTable.forPath(spark, card.get("data_path"))
      #deltaTable.generate("symlink_format_manifest") 
    #if
    
    elif card.get("card_type","").lower() == 'clean_delta':
      cleanupDeltaTables_fn(spark, dataframe, card.get('data_path'))
    #if
    
    elif card.get("card_type","").lower() == 'read_table':
      readSparkSQLQuery(spark, card.get('temp_table_name'), card.get("query"), card.get("partition_column_list",""))
    #if
    
    elif card.get("card_type","").lower() == 'write_dataset':
      dataframe = spark.sql(card.get("query"))
      if dataframe == None or len(dataframe.take(1)) == 0:
        RECORDER_SPARK_ETL.append ("IDF_Exception :  {exception} : {trace}".format (exception ="Dataframe Empty", trace = "Check Data Source. No records found."))
        #raise Exception("Error Processing Config :: {config}".format(config=config_file_path))
      #if
      writeToS3WithFilter_fn(spark, dataframe, card.get('data_path'),CURRENT_PARTITION_DATE, card.get("partition_key_columns",None), "PARQUET", card.get('mode','append'))
      if is_last_card_fn(card.get("card_id",""),config.get("cards",[])):
        return_value[card.get("label","")] = dataframe.count()
    #if
    
    elif card.get("card_type","").lower() == 'read_dataset':
      if card.get('extraction_logic', None) == '*':
        final_list = ['*']
      #if
      else:
        extraction_days = getListOfDateWisePartition_fn(card.get('extraction_logic', None), date_format="YYYYMMDD", from_date = card.get("from_date", None))
        final_list = []

        if extraction_days:
          for item in extraction_days:
            try:
              dbutils.fs.ls("{base_path}/{item}".format(base_path = card.get("data_path"), item = item))
              final_list.append(item)
            #try
            except:
              pass
            #except
          #for
        #if
        else:
          final_list = None
        #else
      #else

      if card.get("data_type",'PARQUET').upper() == 'PARQUET' :
        exec("{table_df} = readParquetFilesFromFolder_fn(spark, '{base_path}', final_list)".format(table_df = card.get("table_name"), base_path= card.get("data_path"), final_list= final_list))
        exec("table_df={table_df}".format(table_df = card.get("table_name")))
        if table_df == None or len(table_df.take(1)) == 0:
          dbutils.notebook.exit({"message" : "Empty Source Folders. No Data Found."})
        #if
        exec("{table_df}.createOrReplaceTempView('{temp_table_name}')".format(table_df = card.get("table_name"), temp_table_name= card.get("table_name")))
        #exec("display({table_df})".format(table_df=card.get("table_name")))
      #if
        
      elif card.get("data_type",'PARQUET').upper() == 'CSV':
        exec("{table_df} = readCSVFilesWithFilterPartitionAndSchema_fn(spark, '{base_path}', final_list, None, True, '{seperator}')".format(table_df = card.get("table_name"), base_path= card.get("data_path"), final_list= final_list, seperator = card.get("extra_parameters","{}").get("seperator",",")))
        exec("table_df={table_df}".format(table_df = card.get("table_name")))
        if table_df == None or len(table_df.take(1)) == 0:
          dbutils.notebook.exit({"message" : "Empty Source Folders. No Data Found."})
        #if
        exec("{table_df}.createOrReplaceTempView('{temp_table_name}')".format(table_df = card.get("table_name"), temp_table_name= card.get("table_name")))
        #exec("display({table_df})".format(table_df=card.get("table_name")))
      #elif
      
    #elif
    
    elif card.get("card_type","").lower() == 'apply_sparksql_2_tmp':
      executeSparkSQLWithTmpDir_fn(spark, card.get("query"), card.get("table_name"), data_source_map.get('SPARK_SQL').get('tmp_path'))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_sparksql_4m_tmp':
      readFromSparkTmpAndExecuteQuery_fn(spark, card.get("query"), card.get("table_name"), card.get('temp_table_names'), data_source_map.get('SPARK_SQL').get('tmp_path'))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_filter':
      if filter_condition == "":
        filter_conditions = card.get("filter")
      #if
      else:
        filter_conditions = card.get("filter")
        filter_conditions.append(filter_condition)
      #else
      applySparkFilter_fn(spark, card.get("input_table"), card.get("output_table"), filter_conditions)
    #elif
    
    elif card.get("card_type","").lower() == 'apply_dedupe':
      applySparkDedupe_fn(spark, card.get("input_table"), card.get("output_table"), card.get("dedupe_by"), card.get("columns"))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_join':
        partition_left_col = card.get("partition_left_col").strip()
        if not partition_left_col:
          partition_left_col = 1
        #if
        partition_right_col = card.get("partition_right_col").strip()
        if not partition_right_col:
          partition_right_col = 1
        #if
        sparkJoin_fn(spark, card.get("temp_table_name"), card.get("left_table_name"), card.get("right_table_name"), partition_left_col, partition_right_col, card.get("join_type"), card.get("select_sql"), card.get("on_sql_condition"))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_group_by':
        groupByAndAggregateWithConditions_fn(spark, None, card.get("group_by_columns"), card.get("aggregate_conditions"), card.get("input_table"), card.get("output_table"))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_union':
       sparkUnion_fn(spark, card.get("output_table"), card.get("input_tables"))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_order_by':
      sparkOrderBy_fn(spark, card.get("input_table"), card.get("output_table") , card.get("order_by_conditions"))
    #elif

    elif card.get("card_type","").lower() == 'apply_difference':
      sparkDifference_fn(spark, card.get("output_table"), card.get("minuend"), card.get("subtrahend"))
    #elif

    elif card.get("card_type","").lower() == 'apply_standardization_rule':
      applyStandardizationRule_fn(spark, rulesenginePredefinedObj, card.get("input_table"), card.get("output_table"), card.get("standardization_rule"))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_window':
      sparkWindow_fn(spark, card.get("input_table"), card.get("partition_list"), card.get("expression"), card.get("new_column_name"), card.get("order_column"))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_intersect':
      sparkIntersect_fn(spark, card.get("output_table"), card.get("input_tables"))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_pivot':
      sparkPivot_fn(spark,card.get("output_table"), card.get("select_sql"), card.get("aggregate_in"), card.get("order_by"))
    #elif
    
    elif card.get("card_type","").lower() == 'apply_merge_count':
      return_value[card.get("label","")] = applySparkMergeCount_fn(spark, card.get("new_data_table"), card.get("dim_data_table"), card.get("merge_data_table"), card.get("primaryKeyColumn"), card.get("merge_type"), card.get("partition_new_data"), card.get("partition_dim_data"), card.get("active_column_name"), card.get("column_list_to_identify_changes"), card.get("start_date_column_name"), card.get("end_date_column_name"), card.get("active_column_true_value"), card.get("active_column_false_value"))
    #elif
    
    elif card.get("card_type","").lower() == 'ytd_running_sum_group':
      calculateYTDValue_fn(spark, card.get("input_table"), card.get("partition_column_list").split(","), card.get("order_by_column"), card.get("calculation_column_name"), card.get("ytd_column_name"), card.get("output_table"), card.get("order_by_column_format","yyyy-MM-dd"))
    #elif
    
    elif card.get("card_type","").lower() == 'impute_null_rows_group':
      imputeLastNotNullValueInGroup_fn(spark, card.get("input_table"), card.get("partition_column_list").split(","), card.get("order_by_column"), card.get("calculation_column_name"), card.get("ytd_column_name"), card.get("output_table"), card.get("order_by_column_format","yyyy-MM-dd"))
    #elif
    
    elif card.get("card_type","").lower() == 'in_out_running_count':
      runningEntryExitCount_fn(spark, card.get("input_table"), card.get("order_by_column"), card.get("value_column"), card.get("column_name_in_count"), card.get("column_name_out_count"),  card.get("output_table"), card.get("order_by_column_format","yyyy-MM-dd"))
    #elif
    
    elif card.get("card_type","").lower() == 'table_lookup':
      #To use this card, filter a dataframe into 2 column dataframe and use this to create a map for lookups. Also the commom_lookup_name is common column name between source and cache
      lookupJoinUsingUDF_fn(spark, card.get("source_table"), card.get("cache_table"), card.get("commom_lookup_name"), card.get("output_column_name"))
    #elif

#     #Function is updated with service principal instead of account key
#     elif card.get("card_type","").lower() == 'write_synapse':
#       dataframe = spark.sql(card.get("input_table"))
#       if dataframe == None or len(dataframe.take(1)) == 0:
#         RECORDER_SPARK_ETL.append ("IDF_Exception :  {exception} : {trace}".format (exception ="Dataframe Empty", trace = "Check Data Source. No records found."))
#         #raise Exception("Error Processing Config :: {config}".format(config=config_file_path))
#       #if
#       sf_jdbcConnectionString = "jdbc:sqlserver://{host}:1433;database={database};user={username};password={password};encrypt=false;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;".format(host = data_source_map.get('SYNAPSE').get("host"), database =  data_source_map.get('SYNAPSE').get("database_name"), username= dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_username')), password = dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_password')))
#       writeDataFrame2Synapse_fn(spark, dataframe, card.get("output_table"), sf_jdbcConnectionString, data_source_map.get('SYNAPSE').get("tmp_path"), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('storage-account-name')), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('storage-account-key')), card.get("repartition_columns"))
#       if is_last_card_fn(card.get("card_id",""),config.get("cards",[])):
#         return_value[card.get("label","")] = dataframe.count()
#     #elif
    
    elif card.get("card_type","").lower() == 'write_synapse':
      dataframe = spark.sql(card.get("input_table"))
      if dataframe == None or len(dataframe.take(1)) == 0:
        RECORDER_SPARK_ETL.append ("IDF_Exception :  {exception} : {trace}".format (exception ="Dataframe Empty", trace = "Check Data Source. No records found."))
        #raise Exception("Error Processing Config :: {config}".format(config=config_file_path))
      #if
      sf_jdbcConnectionString = "jdbc:sqlserver://{host}:1433;database={database};user={username};password={password};encrypt=false;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;".format(host = data_source_map.get('SYNAPSE').get("host"), database =  data_source_map.get('SYNAPSE').get("database_name"), username= dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_username')), password = dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_password')))
      writeDataFrame2Synapse_fn(spark, dataframe, card.get("output_table"), sf_jdbcConnectionString, data_source_map.get('SYNAPSE').get("tmp_path"), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('secret-key')), setup.get('tenant-id'), setup.get('client-id'), card.get("repartition_columns"))
      if is_last_card_fn(card.get("card_id",""),config.get("cards",[])):
        return_value[card.get("label","")] = dataframe.count()
    #elif
    
    elif card.get("card_type","").lower() == 'read_synapse':
      sf_jdbcConnectionString = "jdbc:sqlserver://{host}:1433;database={database}".format(host = data_source_map.get('SYNAPSE').get("host"), database =  data_source_map.get('SYNAPSE').get("database_name"))
      sf_connectionProperties = {"user" : dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_username')), "password" : dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_password')), "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
      readDataFrame4Synapse_fn(spark, card.get("output_table"), card.get("input_table"), sf_jdbcConnectionString, sf_connectionProperties)
    #elif
    
    
    elif card.get("card_type","").lower() == 'read_synapse':
      sf_jdbcConnectionString = "jdbc:sqlserver://{host}:1433;database={database}".format(host = data_source_map.get('SYNAPSE').get("host"), database =  data_source_map.get('SYNAPSE').get("database_name"))
      sf_connectionProperties = {"user" : dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_username')), "password" : dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_password')), "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
      readDataFrame4Synapse_fn(spark, card.get("output_table"), card.get("input_table"), sf_jdbcConnectionString, sf_connectionProperties)
    #elif
    
    
    elif card.get("card_type","").lower() == 'pushdown_synapse':
      sf_jdbcConnectionString = "jdbc:sqlserver://{host}:1433;database={database};encrypt=false;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;".format(host = data_source_map.get('SYNAPSE').get("host"), database =  data_source_map.get('SYNAPSE').get("database_name"))
      execStatement2Synapse_fn(spark, card.get("query"), sf_jdbcConnectionString,dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_username')), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('synapse_password')))
    #elif

    elif card.get("card_type","").lower() == 'pushdown_snowflake':
      _tmp_process_map[level2_dependency] = snowflakeETLQueryPushdown_fn(spark, data_source_map.get('SNOWFLAKE').get("url"), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_username')), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_password')), data_source_map.get('SNOWFLAKE').get("database_name"), data_source_map.get('SNOWFLAKE').get("schema_name"), data_source_map.get('SNOWFLAKE').get("warehouse_name"), card.get("query"))
    #if
    
    elif card.get("card_type","").lower() == 'read_snowflake':
      readFromSnowflakeUsingWithUsernamePassword_fn(spark, card.get('table_name'), card.get('query'), data_source_map.get('SNOWFLAKE').get("url"), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_username')), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_password')), data_source_map.get('SNOWFLAKE').get("database_name"), data_source_map.get('SNOWFLAKE').get("schema_name"), data_source_map.get('SNOWFLAKE').get("warehouse_name"))
    #if
    
    elif card.get("card_type","").lower() == 'write_snowflake':
      dataframe = spark.sql(card.get("query"))
      if dataframe == None or len(dataframe.take(1)) == 0:
        RECORDER_SPARK_ETL.append ("IDF_Exception :  {exception} : {trace}".format (exception ="Dataframe Empty", trace = "Check Data Source. No records found."))
        #raise Exception("Error Processing Config :: {config}".format(config=config_file_path))
      #if
      writeToSnowflakeUsingWithUsernamePassword_fn(spark, dataframe, data_source_map.get('SNOWFLAKE').get("tmp_path"), card.get('table_name'), data_source_map.get('SNOWFLAKE').get("url"), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_username')), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_password')), data_source_map.get('SNOWFLAKE').get("database_name"), data_source_map.get('SNOWFLAKE').get("schema_name"), data_source_map.get('SNOWFLAKE').get("warehouse_name"), how = "WRITE-FORMAT", mode=card.get("mode"))
      if is_last_card_fn(card.get("card_id",""),config.get("cards",[])):
        return_value[card.get("label","")] = dataframe.count()
    #if
    
    elif card.get("card_type","").lower() == 'upsert_snowflake':
      dataframe = spark.sql(card.get("query"))
      sfOptions = getSFOptions(data_source_map.get('SNOWFLAKE').get("url"), data_source_map.get('SNOWFLAKE').get("warehouse_name"), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_username')), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_password')), data_source_map.get('SNOWFLAKE').get("database_name"), data_source_map.get('SNOWFLAKE').get("schema_name"), truncate_table='ON', usestagingtable='OFF')
      dataframe = updateAndInsert_fn(spark, sfOptions, dataframe, card.get('input_table'), card.get('primary_keys'))
      dataframe.registerTempTable(card.get('output_table'))
      if is_last_card_fn(card.get("card_id",""),config.get("cards",[])):
        return_value[card.get("label","")] = dataframe.count()
    #if

  #for
  
  if len(_tmp_process_map) > 0:
    snowflake_filter_map =  dict(filter(lambda l: cards_map[l[0]]['card_type'].lower()=='pushdown_snowflake',_tmp_process_map.items()))
    if len(snowflake_filter_map) > 0:
      _process_map.update(waitForSnowflakeQueryListToExecute_fn(spark, data_source_map.get('SNOWFLAKE').get("url"), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_username')), dbutils.secrets.get(scope=setup.get('databricks-secrets-scope'), key=setup.get('snowflake_password')), data_source_map.get('SNOWFLAKE').get("database_name"), data_source_map.get('SNOWFLAKE').get("schema_name"), data_source_map.get('SNOWFLAKE').get("warehouse_name"), snowflake_filter_map))
    #if
  #if
  
#for

if len(_process_map) > 0:
  overall_status = checkIfAnySFQueryIsError_fn(_process_map)
  dir_path = f"{report_path}/etl-reports/{CURRENT_PARTITION_DATE}/{uuid.uuid4()}.json"
  dbutils.fs.put(dir_path, parseAndReturnJson_fn(_process_map))

  if overall_status != 0:
    raise ValueError("Error In Execution of Job ",_process_map)
  #if
#if


# COMMAND ----------

dir_path = f"{report_path}/job-audit/{CURRENT_PARTITION_DATE}/{uuid.uuid4()}.csv"
RECORDER_SPARK_ETL = RECORDER_SPARK_ETL + RECORDER_SPARK_COMMON + RECORDER_SPARK_BASE1 + RECORDER_SPARK_BASE3 
RECORDER_SPARK_ETL = "\n".join(RECORDER_SPARK_ETL)
dbutils.fs.put(dir_path, RECORDER_SPARK_ETL)
if 'IDF_Exception' in RECORDER_SPARK_ETL:

  return_value = dict([(k,0) for (k,v) in return_value.items()])
  return_value.update({"message" : "Error Processing Config :: {config}".format(config=config_file_path)})
  dbutils.notebook.exit(return_value)
  
  raise Exception("Error Processing Config :: {config}".format(config=config_file_path))
  #dbutils.notebook.exit("Error Processing Config :: {config}".format(config=config_file_path))
#if
dbutils.notebook.exit(return_value)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanup

# COMMAND ----------

if _connection!= None:
    _connection.commit()
    _connection.close()
#if

# COMMAND ----------

#spark.catalog.clearCache()

# COMMAND ----------

