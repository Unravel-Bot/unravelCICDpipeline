# Databricks notebook source
import pandas as pd
import os
import glob
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run "../Common/Upsert_to_Synapse"

# COMMAND ----------

WeeklySource='/mnt/self-serve/BUKMI/BUKMIEXCEL/BI2500_Exec_Scorecard/Default/BI2500_Exec_Scorecard*'
WeeklyDF=spark.read.format('csv').option('header',True).load(WeeklySource)


# COMMAND ----------

WeeklyDF=WeeklyDF.withColumn("Date",to_date(col("Date"),'d/M/yyyy'))
WeeklyDF.createOrReplaceTempView('Weekly_Exec')

# COMMAND ----------

outputDF=spark.sql("select Metric,Legend,Date as DATE ,CAST(Actual as DECIMAL(18,4)),CAST(ActualItems as DECIMAL(18,0)),CAST(`Actual�` as DECIMAL(18,2)) as `Actual£`,CAST(`Actual%` as DECIMAL(18,4)) ,CAST(`Target/LY` as DECIMAL(18,4)) ,CAST(TargetItems as DECIMAL(18,0)),CAST(`Target/LY�` as DECIMAL(18,2)) as `Target/LY£`,CAST(`Target/LY%` as DECIMAL(18,4)),CAST(`Target/LLY` as DECIMAL(18,4)) ,CAST(TargetItemsLLY as DECIMAL(18,0)),CAST(`Target/LLY�` as DECIMAL(18,2)) as `Target/LLY£`,CAST(`Target/LLY%` as DECIMAL(18,4)),CAST(Variance as DECIMAL(18,4)),CAST(VarianceLLY as DECIMAL(18,4)) ,CAST(CAST(KPI as INT) as STRING) as KPI,current_timestamp as RunDate  from Weekly_Exec")

# COMMAND ----------

dwhStagingTable =None
synapse_output_table = 'SS_BUKMI.Exec_Weekly_Data'
key_columns = None
deltaName =None
dwhStagingDistributionColumn =None

# COMMAND ----------

truncate_and_load_synapse (outputDF,synapse_output_table)