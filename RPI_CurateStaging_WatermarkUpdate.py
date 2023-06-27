# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md 
# MAGIC Get the parameters

# COMMAND ----------

dbutils.widgets.text("SourceName", "","")
SourceName = dbutils.widgets.get("SourceName")

dbutils.widgets.text("FeedName", "","")
FeedName = dbutils.widgets.get("FeedName")

dbutils.widgets.text("LastProcessedTime", "","")
LastProcessedTime = dbutils.widgets.get("LastProcessedTime")

dbutils.widgets.text("BatchID", "","")
BatchID = dbutils.widgets.get("BatchID")

# COMMAND ----------

# MAGIC %md 
# MAGIC Create database and table for tracking the latest processing time and batch id for each source

# COMMAND ----------

# MAGIC %sql
# MAGIC create Database if not exists RPI_Reference_DB;
# MAGIC
# MAGIC create table if not exists RPI_Reference_DB.CurateStage_WatermarkTable (SourceName string, FeedName string, BatchId string, LastProcessedTime timestamp) 
# MAGIC using delta 
# MAGIC partitioned by (SourceName,FeedName)
# MAGIC location '/mnt/idf-reports/RPI_Reference/CurateStage/WatermarkTable/';

# COMMAND ----------

# MAGIC %md
# MAGIC create temperory data frame with latest data. Values will be received from pipeline parameters.

# COMMAND ----------

updatedvalue=[(SourceName,FeedName,BatchID,LastProcessedTime)]
updatedDF=spark.createDataFrame(updatedvalue,schema=['SourceName','FeedName','BatchID','LastProcessedTime']).withColumn("LastProcessedTime",col("LastProcessedTime").cast('Timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC Save the data to delta lake table

# COMMAND ----------

updatedDF.write.format("delta").mode("append").partitionBy("SourceName","FeedName").saveAsTable("RPI_Reference_DB.CurateStage_WatermarkTable")