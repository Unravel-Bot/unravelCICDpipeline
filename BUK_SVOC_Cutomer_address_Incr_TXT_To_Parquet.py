# Databricks notebook source
# MAGIC %md
# MAGIC CONVERSION OF TEXT TO PARQUET

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALTER TABLE  curate_standard.BUKIT_BTCSVOC_BUK_SVOC_Customer_address ADD columns (Source_Sent_Date string, Source_Sent_Time string);
# MAGIC --describe curate_standard.BUKIT_BTCSVOC_BUK_SVOC_Customer_address
# MAGIC --ALTER TABLE delta.`/mnt/idf-curatestandard/quarantine/BUKIT/BTCSVOC/BUK_SVOC_Customer_address_Incr/Incremental` ADD columns (Source_Sent_Date string, Source_Sent_Time string);
# MAGIC --describe delta.`/mnt/idf-curatestandard/quarantine/BUKIT/BTCSVOC/BUK_SVOC_Customer_address_Incr/Incremental`

# COMMAND ----------

# MAGIC %sql

# MAGIC create widget text FileList default "";
# MAGIC create widget text SourcePath default "";
# MAGIC create widget text DestinationPath default "";
# MAGIC create widget text RunDate default "";
# MAGIC create widget text RunTime default "" ;

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
SourceFileCount=len(filelist.split(","))
OutputTableList=[]
dfSchema = StructType([ StructField("ENTITY_ID", StringType(), True), StructField(" PARTY_ID", StringType(), True), StructField(" ADDRESS_TYPE", StringType(), True), StructField(" HOUSE_NUMBER", StringType(), True), StructField(" HOUSE_UNIT", StringType(), True), StructField(" BUILDING_NAME", StringType(), True), StructField(" STREET", StringType(), True), StructField(" LOCALITY", StringType(), True), StructField(" TOWN", StringType(), True), StructField(" COUNTY", StringType(), True), StructField(" POST_CODE", StringType(), True), StructField(" COUNTRY_CODE", StringType(), True), StructField(" NO_FIXED_ABODE_FLG", StringType(), True), StructField(" ADDRESS_VALID_FLG", StringType(), True), StructField(" GONE_AWAY_FLG", StringType(), True), StructField(" ENTITY_CREATE_TIME", StringType(), True), StructField(" ENTITY_LAST_UPDATE_TIME", StringType(), True)])

dfSchema1 = StructType([StructField("ENTITY_ID", StringType(), True), StructField(" PARTY_ID", StringType(), True), StructField(" ADDRESS_TYPE", StringType(), True), StructField(" HOUSE_NUMBER", StringType(), True), StructField(" HOUSE_UNIT", StringType(), True), StructField(" BUILDING_NAME", StringType(), True), StructField(" STREET", StringType(), True), StructField(" LOCALITY", StringType(), True), StructField(" TOWN", StringType(), True), StructField(" COUNTY", StringType(), True), StructField(" POST_CODE", StringType(), True), StructField(" COUNTRY_CODE", StringType(), True), StructField(" NO_FIXED_ABODE_FLG", StringType(), True), StructField(" ADDRESS_VALID_FLG", StringType(), True), StructField(" GONE_AWAY_FLG", StringType(), True), StructField(" ENTITY_CREATE_TIME", StringType(), True), StructField(" ENTITY_LAST_UPDATE_TIME", StringType(), True),StructField("Source_Sent_Date", StringType(), True),StructField("Source_Sent_Time", StringType(), True)])

filelist1=filelist[1:-1]
print(filelist1)
for file in filelist1.split(","):
    foldername=file[1:-5]
    print(foldername)
    dfo=foldername.split("_")
    df=spark.read.format('csv').option('delimiter',"|").schema(dfSchema).load(mountPoint+SourcePath+"/"+foldername+".psv")
    #df.show()
    df1=df.toPandas()
    df1.insert(17,'Source_Sent_Date',dfo[3],True)
    df1.insert(18,'Source_Sent_Time',dfo[4],True)
    df1=df1[1:]
    #display(df1)
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    #display(df2)
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

# COMMAND ----------

# MAGIC %sql
# MAGIC describe delta.`/mnt/idf-curatestandard/BUKIT/BTCSVOC/BUK_SVOC_Customer_phone_Incr/Incremental`

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

