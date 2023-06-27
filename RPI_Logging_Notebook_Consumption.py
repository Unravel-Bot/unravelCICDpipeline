# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS RPI_LOGGING;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS RPI_LOGGING.CONSUMPTION_LOGGING (
# MAGIC     TargetTableName string COMMENT 'Target Table name for Consumption',
# MAGIC     BatchId string COMMENT 'Batch Id',
# MAGIC     Status string COMMENT 'Status of process',
# MAGIC     Error string COMMENT 'Error Details',
# MAGIC     RunTime string COMMENT 'Trigger Run Time',
# MAGIC     TriggerId string COMMENT 'Trigger Id'
# MAGIC     ) USING Delta
# MAGIC
# MAGIC LOCATION '/mnt/idf-reports/RPI_LOGGING/CONSUMPTION_LOGGING/'

# COMMAND ----------

dbutils.widgets.text("TargetTableName", "","")
TargetTableName = dbutils.widgets.get("TargetTableName")

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")

dbutils.widgets.text("Status", "","")
Status = dbutils.widgets.get("Status")

dbutils.widgets.text("Error", "","")
Error = dbutils.widgets.get("Error")

dbutils.widgets.text("RunTime", "","")
RunTime = dbutils.widgets.get("RunTime")

dbutils.widgets.text("TriggerId", "","")
TriggerId = dbutils.widgets.get("TriggerId")


# COMMAND ----------

updatedvalue=[(TargetTableName,BatchId ,Status,Error,RunTime,TriggerId)]
updatedDF=spark.createDataFrame(updatedvalue,schema=['TargetTableName','BatchId','Status','Error','RunTime','TriggerId'])
updatedDF.write.format("delta").mode("append").saveAsTable("RPI_LOGGING.CONSUMPTION_LOGGING")