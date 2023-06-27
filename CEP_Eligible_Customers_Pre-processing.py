# Databricks notebook source
# MAGIC %sql
# MAGIC --select * from curate_standard.BootsUK_EligibleCustomerOpportunities_Boots_CEP_Incr
# MAGIC --ALTER TABLE curate_standard.BootsUK_EligibleCustomerOpportunities_Boots_CEP_Incr ADD columns (Source_create_date string);
# MAGIC --describe curate_standard.BootsUK_EligibleCustomerOpportunities_Boots_CEP_Incr 

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text FileList default "";
# MAGIC create widget text SourcePath default "";
# MAGIC create widget text DestinationPath default "";
# MAGIC create widget text RunDate default "";
# MAGIC create widget text RunTime default "";

# COMMAND ----------

import os
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *


mountPoint='/mnt/idf-cleansed/'
tgtmountPoint='/mnt/idf-curatestage/'
first_row_is_header = "true"
delimiter = "|"
filelist=dbutils.widgets.get("FileList")
SourcePath=dbutils.widgets.get("SourcePath")
RunDate=dbutils.widgets.get("RunDate")
RunTime=dbutils.widgets.get("RunTime")
DestinationPath=dbutils.widgets.get("DestinationPath")
print(filelist)
SourceFileCount=len(filelist.split(","))
OutputTableList=[]
dfSchema = StructType([ StructField("contactId", StringType(), True),
StructField("campaignId", StringType(), True),
StructField("gameId", StringType(), True),
StructField("customerEligibilityId", StringType(), True),
StructField("obfuscatedCustomerId", StringType(), True),
StructField("countryCode", StringType(), True),
StructField("ciamsId", StringType(), True),
StructField("initialCepEligibility", StringType(), True),
StructField("channel", StringType(), True),
StructField("event_Version", StringType(), True),
StructField("event_ID", StringType(), True),
StructField("event_Name", StringType(), True),
StructField("producer", StringType(), True),
StructField("created_date", StringType(), True)])
dfSchema1 = StructType([ StructField("contactId", StringType(), True),
StructField("campaignId", StringType(), True),
StructField("gameId", StringType(), True),
StructField("customerEligibilityId", StringType(), True),
StructField("obfuscatedCustomerId", StringType(), True),
StructField("countryCode", StringType(), True),
StructField("ciamsId", StringType(), True),
StructField("initialCepEligibility", StringType(), True),
StructField("channel", StringType(), True),
StructField("event_Version", StringType(), True),
StructField("event_ID", StringType(), True),
StructField("event_Name", StringType(), True),
StructField("producer", StringType(), True),
StructField("created_date", StringType(), True),
StructField("Source_create_date", StringType(), True)])
filelist1=filelist[1:-1]
print(filelist1)
for file in filelist1.split(","):
    foldername=file[1:-5]
    print(foldername)
    dfo=foldername.split("_")
    dfo=dfo[2]
    print(dfo)
    df=spark.read.format('csv').option('delimiter',"|").schema(dfSchema).load(mountPoint+SourcePath+"/"+foldername+".csv")
    df1=df.toPandas()
    df1.insert(14,'Source_create_date',dfo,True)
    df1=df1[1:]
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    df2.write.mode('append').format("parquet").save(tgtmountPoint+DestinationPath+"/"+RunDate+RunTime+"/")
    

# COMMAND ----------

import os
from os import listdir
filelist1=filelist[1:-1]
for file1 in filelist1.split(","):
    foldername=file1[1:-5]
    OutputTableList.append(foldername)
    DestinationTableCount=len(OutputTableList)

# COMMAND ----------

dbutils.notebook.exit({"SourceCount":SourceFileCount,"DestinationCount":DestinationTableCount,"DestinationTableList":OutputTableList})