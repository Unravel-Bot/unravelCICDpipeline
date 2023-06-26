# Databricks notebook source
import pandas as pd
import os
import glob
import numpy as np

# COMMAND ----------

# MAGIC %run "../Common/Upsert_to_Synapse"

# COMMAND ----------

Omni_Channel_Source='/mnt/self-serve//BUKMI/SAPBW/BootsUK_Ominchannel_scorecard_Monthly_Actuals_SAPBW/Default/BI3005_OmniChannel_Scorecard*'
Omni_ChannelDF=spark.read.format('csv').option('header',True).load(Omni_Channel_Source)
Omni_ChannelDF.createOrReplaceTempView('Omni_Channel')


# COMMAND ----------

df2=spark.sql("SELECT to_timestamp(Date ,'d/M/yyyy') as `Date`,Category,Data,Timeframe,concat(trim(cast(coalesce(cast(`Value CY` as decimal(18, 4)),0)/1000000 as decimal(7,2))),'m') as ValueCY,concat(trim(cast(coalesce(try_cast(`Value LY` as decimal(18, 4)),0)/1000000 as decimal(7,2))),'m') as ValueLY,case when coalesce(cast(`YoY%` as decimal(18, 4)),0) >= 0 then concat(trim(cast(coalesce(cast(`YoY%` as decimal(18, 4)),0)*100 as decimal(7,1))),'%') else concat('(',trim(cast(abs(coalesce(cast(`YoY%` as decimal(18, 4)),0))*100 as decimal(7,1))),'%)') end as `YoY%`,current_timestamp as RunDate FROM Omni_Channel where Data in ('Digital Traffic','New Users','Transactions','Active Members','Active App Users', 'Email Contactability') union SELECT to_timestamp(Date ,'d/M/yyyy') as `Date`,Category,Data,Timeframe,concat('£',trim(cast(coalesce(cast(`Value CY` as decimal(18, 4)),0) as decimal(7,2)))) as ValueCY,concat('£',trim(cast(coalesce(cast(`Value LY` as decimal(18, 4)),0) as decimal(7,2)))) as ValueLY,case when coalesce(cast(`YoY%` as decimal(18, 4)),0) >= 0 then concat(trim(cast(coalesce(cast(`YoY%` as decimal(18,4)),0)*100 as decimal(7,1))),'%') else concat('(',trim(cast(abs(coalesce(cast(`YoY%` as decimal(18, 4)),0))*100 as decimal(7,1))),'%)') end as `YoY%`,current_timestamp as RunDate FROM Omni_Channel where Data in ('Avg. Revenue/Member') union SELECT to_timestamp(Date ,'d/M/yyyy') as `Date`,Category,Data,Timeframe,concat('£',trim(cast(coalesce(cast(`Value CY` as decimal(18, 4)),0) as decimal(7,2)))) as ValueCY,concat('£',trim(cast(coalesce(cast(`Value LY` as decimal(18, 4)),0) as decimal(7,2)))) as ValueLY,case when coalesce(cast(`YoY%` as decimal(18, 4)),0) >= 0 then concat('£',trim(cast(coalesce(cast(`YoY%` as decimal(18,4)),0)*100 as decimal(7,1)))) else concat('(£',trim(cast(abs(coalesce(cast(`YoY%` as decimal(18, 4)),0))*100 as decimal(7,2))),')') end as `YoY%`,current_timestamp as RunDate FROM Omni_Channel where Data in ('Digital ATV','Digital ATV £') union SELECT to_timestamp(Date ,'d/M/yyyy') as `Date`,Category,Data,Timeframe,concat(trim(cast(coalesce(cast(`Value CY` as decimal(18, 4)),0)*100 as decimal(7,0))),'%') as ValueCY,concat(trim(cast(coalesce(cast(`Value LY` as decimal(18, 4)),0)*100 as decimal(7,0))),'%') as ValueLY,case when coalesce(cast(`YoY%` as decimal(18, 4)),0) >= 0 then concat(trim(cast(coalesce(cast(`YoY%` as decimal(18, 4)),0)*100 as decimal(7,1))),'%p') else concat('(',trim(cast(abs(coalesce(cast(`YoY%` as decimal(18, 4)),0))*100 as decimal(7,1))),'%p)') end as `YoY%`,current_timestamp as RunDate FROM Omni_Channel where Data in ('Brand Affinity','In-store Satisfaction','NPS') union SELECT to_timestamp(Date ,'d/M/yyyy') as `Date`,Category,Data,Timeframe,trim(cast(coalesce(cast(`Value CY` as decimal(18, 4)),0) as decimal(7,1))) as ValueCY,trim(cast(coalesce(cast(`Value LY` as decimal(18, 4)),0) as decimal(7,1))) as ValueLY,case when coalesce(cast(`YoY%` as decimal(18, 4)),0) >= 0 then concat(trim(cast(coalesce(cast(`YoY%` as decimal(18, 4)),0)*100 as decimal(7,1))),'%') else concat('(',trim(cast(abs(coalesce(cast(`YoY%` as decimal(18, 4)),0))*100 as decimal(7,1))),'%)') end as `YoY%`,current_timestamp as RunDate FROM Omni_Channel where Data in ('Digital Satisfaction.com','Digital Satisfaction Mobile')")


# COMMAND ----------

dwhStagingTable =None
synapse_output_table = 'SS_BUKMI.Omni_Channel_Data'
key_columns = None
deltaName =None
dwhStagingDistributionColumn =None

# COMMAND ----------

truncate_and_load_synapse (df2,synapse_output_table)