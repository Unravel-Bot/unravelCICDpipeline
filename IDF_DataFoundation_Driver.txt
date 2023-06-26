# Databricks notebook source
# dbutils.library.install("dbfs:/FileStore/jars/b3c9de7a_98d4_4224_9c73_7c22ff12f412/sparkclean-0.1-py3-none-any.whl")
# dbutils.library.installPyPI("textdistance")
# dbutils.library.installPyPI("unidecode")
# dbutils.library.installPyPI("tqdm")
# dbutils.library.installPyPI("textblob")
# dbutils.library.restartPython()

# COMMAND ----------

#JPM#import sys
#JPM#import os
import json
#JPM#import re
#JPM#from datetime import datetime,timedelta
import uuid
#JPM#import shutil

#JPM#import pyspark
#JPM#from pyspark.sql import SparkSession, Row
#JPM#from pyspark.sql.types import *
#JPM#import pyspark.sql.functions as F

#JPM#import pandas as pd


# COMMAND ----------

# MAGIC %md
# MAGIC ## IDF SDK

# COMMAND ----------

# MAGIC %run ../commons/CommonBase

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_Common_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_BaseOneBinary_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_DQ_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_PySpark_BaseTwoBinary_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_Python_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_Regex_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_Classification_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_Extention_Module

# COMMAND ----------

# MAGIC %run ../commons/CommonBase

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Config File

# COMMAND ----------

dbutils.widgets.text("config_file_name", '', '')
config_file_path = dbutils.widgets.get('config_file_name') 

config = json.loads(open(f"/dbfs/mnt/idf-config/{config_file_path}",'r').read())
setup = json.loads(open(f"/dbfs/mnt/idf-config/setup-config/common-setup.json",'r').read())
#config = json.loads(open('/dbfs/mnt/wba-idf/configs/sample-orders-config.json','r').read())#
#sample-customers-config.json

dbutils.widgets.text("run_date", '', '')
run_date = dbutils.widgets.get('run_date')

dbutils.widgets.text("run_time", '', '')
run_time = dbutils.widgets.get('run_time')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common

# COMMAND ----------

CURRENT_PARTITION_DATE = generateCurrentDate_fn()  # else /

# COMMAND ----------

failed_enforce_test_flag = False
rule_condition_list,  complex_rule_condition_list, complex_rule_list, rule_result_fn, rule_list = [], [], [], [], []
isError_dict,  complex_isError_dict = {}, {}

total_count, bad_row_count, good_row_count = 0, 0, 0

return_value = {"message" : "Data Foundation", "total_count": total_count, "bad_row_count": bad_row_count, "good_row_count": good_row_count}
enforce_bad_row_count = False

# COMMAND ----------

schema = None if config.get('schema',[]) == [] else config.get('schema',None)
data_source = config.get("data_source")
data_destination = config.get("data_destination")
curated_path = list(filter(lambda l: l!='NA', map(lambda l: l.get('type_specific_details',{}).get('destination_path','NA'), data_destination)))
raw_path = config.get("raw_path")
quarantine_path = config.get("quarantine_path")
report_path = config.get("report_path")
partition_startegy = data_source.get("partition_startegy","YYYYMMDD000000")

rulesengineComplexObj = RulesEngineComplexExtClass()
rowbasedDQObject = RowBasedDataQualityExtClass()
rulesenginePredefinedObj  = RulesEnginePredefinedExtClass()
rulesengineExpressionObj = RulesEngineExpressionExtClass()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

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
# MAGIC ## Code

# COMMAND ----------

if partition_startegy == "YYYYMMDD000000" :
  partition_list = getListOfDateWisePartitionWith000000_fn(extraction_range = run_date,base_path = data_source.get('type_specific_details').get('source_path'), date_format="YYYYMMDD")
  #print(partition_list)
#if
elif partition_startegy == "YYYYMMDDHH0000" :
  partition_list = getListOfDateWisePartitionWithHH0000_fn(extraction_range = run_date,extraction_time = run_time, base_path = data_source.get('type_specific_details').get('source_path'), date_format="YYYYMMDD")
  #print(partition_list)
#if

if partition_list == []:
  #raise Exception("Empty Source Folders. No Data Found.")
  dir_path = f"{report_path}/job-audit/{CURRENT_PARTITION_DATE}/{uuid.uuid4()}.csv"
  dbutils.fs.put(dir_path, "No File Path Found in Source Location")
  dbutils.notebook.exit({"message" : "Empty Source Folders. No Data Found.","total_count": 0, "bad_row_count": 0, "good_row_count": 0})
#if

if data_source.get("data_type","CSV").upper() == 'CSV' :
  header = True if  data_source.get('type_specific_details').get('header',"true") in (True, "true", "True", "TRUE", "yes", "Yes", "YES") else False
  input_df = readCSVFilesWithFilterPartitionAndSchema_fn(spark, data_source.get('type_specific_details').get('source_path'), partition_list, schema, header, data_source.get('type_specific_details').get('delimiter'), data_source.get('type_specific_details').get('escape_character','NA'), config.get("schema_validation",False), ignoreTrailingWhiteSpace = 'true', ignoreLeadingWhiteSpace = 'true', mergeSchema = data_source.get('type_specific_details').get('mergeSchema','true'))
#if

elif data_source.get("data_type","CSV").upper() == 'PARQUET' :
  input_df = readParquetFilesFromFolder_fn(spark, data_source.get('type_specific_details').get('source_path'), partition_list, distinct_flag = False)
  input_df = normalizeDataframeColumns_fn(spark, input_df)
#if

elif data_source.get("data_type","CSV").upper() == 'PARQUET_WITH_DECIMAL' :
  input_df = readParquetWithDecimal4Databricks_fn(spark,  f"{data_source.get('type_specific_details').get('source_path')}", partition_list, distinct_flag = False)
#if

elif data_source.get("data_type","CSV").upper() == 'XML' :
  path_list = list(map(lambda l: f"{data_source.get('type_specific_details').get('source_path')}/{l}", partition_list))
  input_df = readXMLFilesFromFolder_fn(spark,  path_list,  data_source.get('type_specific_details').get("root_tag",'root'),  data_source.get('type_specific_details').get("selection_tag",'root'),  data_source.get('type_specific_details').get("sampling_ratio",1.0))
#if

#input_df = input_df.repartition(int(config.get("parallelism","10"))).cache()
 
total_count = 0 if input_df == None else input_df.count()
df_column_list = [] if input_df == None else input_df.columns

for item in config.get('rules_engine',{}).get('predefined_rules',[]):
  for k,v in item.items():
    input_df = rulesenginePredefinedObj.standardizeColumnsByPredefinedRule_fn(input_df, k, v.split(','), df_column_list)
  #for
#for

for item in config.get('rules_engine',{}).get('expression_rules',[]):
   input_df = rulesengineExpressionObj.standardizeColumnsByExpressionRule_fn(input_df, item.get('type'), item.get('column_names').split(','), item.get('expression'), item.get('replace_value'), df_column_list)
#for

if config.get("schema", None) != None:
  complex_rules  =  convertAliasColumnName2RulesEngine_fn(config.get("schema"), config.get("rules_engine",{}).get("complex_rules",[]))
#if
else:
  complex_rules = config.get("rules_engine",{}).get("complex_rules",[])
#else

for item in complex_rules:
  input_df = rulesengineComplexObj.standardizeColumnsByComplexRule_fn(spark, input_df, item, df_column_list)
  df_column_list = input_df.columns
#for

display(input_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DQ - Row Based

# COMMAND ----------

data_quality_section = config.get("data_quality",{}).get("row_based_rules",{})
if data_quality_section and input_df != None: 
  enforce = data_quality_section.get("enforce")
  
  # By pass return bad_row_count if no
  enforce_bad_row_count = True if enforce.lower() == 'true' or  enforce.lower() == 'yes'  or  enforce ==  True else False
  
  rule_mapping = []
  
  rules = convertAliasColumnName2DQRules(schema, data_quality_section.get("rules",[])) if schema != None else data_quality_section.get("rules",[])

  for rule_id, rule in zip(range(len(rules)), rules):
    if rule.get("rule_type") == "REGEX_LIBRARY":
      rule['column_values'] = replaceKeyWithRegex(rule.get('column_values').upper(), regexMap).pattern
    #if
    mapping, input_df = rowbasedDQObject.rowBasedRulesCheck_fn(spark, input_df, rule_id+1, rule, df_column_list, total_count)
    rule_mapping.extend(mapping)
  #for

  input_df = consolidateRowBasedDQConditionRows_fn(input_df)
  
  if enforce.lower() == 'true' or  enforce.lower() == 'yes'  or  enforce ==  True:
    bad_rows_df = input_df.filter(" __idf__dq_final == 0 ").drop("__idf__dq_final")
    input_df = input_df.filter(" __idf__dq_final == 1 ").drop("__idf__dq_final")

    bad_row_count = bad_rows_df.count()
    good_row_count = total_count - bad_row_count
    
    bad_rows_df = consolidateRowBasedDQErrorDescription_fn(bad_rows_df)
    writeToS3WithFilter_fn(spark, bad_rows_df, quarantine_path, CURRENT_PARTITION_DATE, None, "PARQUET", mode='append')
  #if
  
  else:
    bad_row_count = input_df.filter(" __idf__dq_final == 0 ").count()
    good_row_count = total_count

    input_df = input_df.drop("__idf__dq_final")
  #else
  
  input_df = removeExtraColumnsFromDFWithRegex_fn(input_df)
  
  row_based_checks = {}
  row_based_checks['enforce'] = enforce
  row_based_checks["total_rejected_rows"]= bad_row_count
  row_based_checks["total_validated_rows"]= good_row_count
  row_based_checks['rules'] = rule_mapping
  
  dq_dimensions = {}
  #dq_dimensions = {}

  dq_dimensions = dqDimensionAggregation(rule_mapping, dq_dimensions)
  dimensions_list = ['Conformity','Integrity','Consistency','Accuracy']
  for dimension in dimensions_list:
      if dimension in dq_dimensions:
          row_based_checks[dimension] = dq_dimensions[dimension]
      else:
          row_based_checks[dimension] = 0
  #for
  row_based_checks.update({'source_path':data_source.get('type_specific_details').get('source_path'),'quarantine_path': quarantine_path, 'run_date': CURRENT_PARTITION_DATE})
  rule_result_fn.append({"row_based_checks" :row_based_checks})
#if

data_quality_section = config.get("data_quality",{})

if data_quality_section: 
  dir_path = f"{report_path}/data-quality/{CURRENT_PARTITION_DATE}/{uuid.uuid4()}.json"
  dbutils.fs.put(dir_path, parseAndReturnJson_fn(rule_result_fn))
#if

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Data

# COMMAND ----------

if failed_enforce_test_flag:
  writeToS3WithFilter_fn(spark, input_df, quarantine_path, CURRENT_PARTITION_DATE, None, 'PARQUET', mode='append')
#if
elif input_df != None and len(input_df.take(1)) != 0:
  mappings = config.get('rules_engine',{})
  
  if "drop_empty_rows" in mappings and mappings.get("drop_empty_rows",{}).get("enable") == True:
    input_df = dropEmptyRows_fn(input_df)
  #if
  
  if "filter_mapping" in mappings:
    input_df = filterDataFrame_fn(input_df,mappings.get('filter_mapping'))
  #if
  
  if "generate_key" in mappings and mappings.get("generate_key",{}).get("enable") == True:
    input_df = generateUniqueIdPerRow_fn(input_df, mappings.get("generate_key",{}).get("column_name"))
  #if
  
  if "generate_constant_column" in mappings :
    input_df = generateNewColumnBasedOnConstants_fn(input_df,mappings.get("generate_constant_column",{}))
  #if
  
  if "generate_record_timestamp" in mappings and mappings.get("generate_record_timestamp",False) in (True, "true", "TRUE", "True"):
    input_df = input_df.withColumn("record_timestamp", F.lit(int(CURRENT_PARTITION_DATE)))
  #if
  
  for item in config.get('rules_engine',{}).get('derived_mapping',[]):
    input_df = generateNewColumnBasedOnCaseExpression_fn(input_df, item)
  #for

  if bool(config.get('rules_engine',{}).get('direct_mapping',{})):
    input_df = renameColumnName_fn(input_df, config.get('rules_engine',{}).get('direct_mapping',{}))
  #if
  
  if bool(config.get('rules_engine',{}).get('selected_columns',[])):
    input_df = selectColumnOrderInDF_fn(input_df, config.get('rules_engine',{}).get('selected_columns',[]))
  #if
  
  for item in config.get('rules_engine',{}).get('custom_ext_rules',[]):
    input_df = locals()[item.get("function_name","unknown_function")](spark, input_df, item.get("parameters",{}))
    display(input_df)
  #for
  
  input_df.createOrReplaceTempView("destination_table")
  for data_targets in data_destination:
    destination_details = data_targets.get('type_specific_details')
    query = destination_details.get('query',None)
    if query not in [None,"NA","","N/A"]:
      input_df = spark.sql(query)
    #if

    if data_targets.get('data_type','NA') ==  "DELTA" and destination_details.get("merge_type",'NA').upper() == 'SCD1':
      if destination_details.get("provider_name",'AZURE').upper() in  ["AWS","AZURE"]:
        table_exists = False
        try:
          dbutils.fs.ls(destination_details.get("destination_path"))
          table_exists = True
        #try
        except:
          table_exists = False
        #except
        writeDataWithDeltaSCD1Format_fn(spark, destination_details.get("table_name"), input_df, destination_details.get("destination_path"), destination_details.get("primary_key"), destination_details.get("partition_key_columns",None), table_exists)
        spark.sql(f"OPTIMIZE delta.`{destination_details.get('destination_path')}`")
      #if
    #if
    
    elif data_targets.get('data_type','NA') ==  "DELTA" and destination_details.get("merge_type",'NA').upper() == 'SCD2':
      if destination_details.get("provider_name",'AZURE').upper() in  ["AWS","AZURE"]:
        if len(dbutils.fs.ls(destination_details.get("destination_path")))==0:
          table_exists = False
        #if  
        else:
          table_exists = True
        #else
        writeDataWithDeltaDatabricksSCD2Format_fn(spark, destination_details.get("table_name"), input_df, destination_details.get("destination_path"), destination_details.get("primary_key"), destination_details.get("partition_key_columns"), table_exists,  destination_details.get("active_column_name"), destination_details.get("column_list_to_identify_scd2_merge"), destination_details.get("start_date_column_name"), destination_details.get("end_date_column_name"), destination_details.get("active_column_true_value"), destination_details.get("active_column_false_value"))
        spark.sql(f"OPTIMIZE delta.`{destination_details.get('destination_path')}`")
      #if
    #if
    
    else:
      #display(input_df)
      writeToS3WithFilter_fn(spark, input_df, destination_details.get('destination_path'),CURRENT_PARTITION_DATE, destination_details.get("partition_key_columns",None), data_targets.get('data_type','PARQUET'), mode='append')
    #else
  #for
  
#else

RECORDER_SPARK_COMMON = RECORDER_SPARK_COMMON + RECORDER_SPARK_DQ + RECORDER_SPARK_BASE1 + RECORDER_SPARK_BASE2
RECORDER_SPARK_COMMON = "\n".join(RECORDER_SPARK_COMMON)
dir_path = f"{report_path}/job-audit/{CURRENT_PARTITION_DATE}/{uuid.uuid4()}.csv"
dbutils.fs.put(dir_path, RECORDER_SPARK_COMMON)

if 'IDF_Exception' in RECORDER_SPARK_COMMON:
  dbutils.notebook.exit({"message" : "Error Processing Config :: {config}".format(config=config_file_path),"total_count": 0, "bad_row_count": 0, "good_row_count": 0})
  raise Exception("Error Processing Config :: {config} :: Error Log :: {log}".format(config=config_file_path, log = RECORDER_SPARK_COMMON))
  #dbutils.notebook.exit("Error Processing Config :: {config}".format(config=config_file_path))
#if

dbutils.notebook.exit({"message" : "Data Foundation OK", "total_count": total_count, "bad_row_count": bad_row_count if enforce_bad_row_count == True else 0, "good_row_count": good_row_count if enforce_bad_row_count == True else total_count})


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanup

# COMMAND ----------

#spark.catalog.clearCache()