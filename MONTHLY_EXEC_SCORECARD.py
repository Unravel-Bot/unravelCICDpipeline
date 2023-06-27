# Databricks notebook source
import pandas as pd
import os
import glob

# COMMAND ----------

# MAGIC %run "../Common/Upsert_to_Synapse"

# COMMAND ----------

Source_Monthly='/mnt/self-serve/BUKMI/SAPBW/BootUK_Exec_Scorecard_Monthly_Actuals_SAPBW/Default/BI2500_Exec_Scorecard_Monthly*'
MonthlyDF=spark.read.format('csv').option('header',True).load(Source_Monthly)
MonthlyDF.createOrReplaceTempView('Monthly_Exec')

# COMMAND ----------

df2=spark.sql("select Metric,CAST(Legend as VARCHAR(100)),cast(to_timestamp(date ,'d/M/yyyy') as DATE) as `date` ,CAST(Actual as DECIMAL(18,4)), CAST(ActualItems as DECIMAL(18,0)) ,CAST(`Actual£` as DECIMAL(18,2)) ,CAST(`Actual%` as DECIMAL(18,4)) ,CAST(`Target/LY` as DECIMAL(18,4)), CAST(TargetItems as DECIMAL(18,0)), CAST(`Target/LY£` as DECIMAL(18,2)) ,CAST(`Target/LY%` as DECIMAL(18,4)) ,CAST(Variance as DECIMAL(18,4)) ,CAST(CAST(KPI as INT) as STRING) AS KPI ,current_timestamp as RunDate  from Monthly_Exec");


# COMMAND ----------

#dwhStagingTable =None
synapse_output_table = 'SS_BUKMI.Exec_Monthly_Data'
key_columns = None
deltaName =None
dwhStagingDistributionColumn =None

# COMMAND ----------

truncate_and_load_synapse (df2,synapse_output_table)