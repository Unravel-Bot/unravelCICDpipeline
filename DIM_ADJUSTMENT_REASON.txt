# Databricks notebook source
import datetime


# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

max_load_date=last_loaded_date("con_columbus.DIM_ADJUSTMENT_REASON")
print(max_load_date)

# COMMAND ----------

OutputDF=spark.sql("SELECT  \
SAR.StockAdjustmentReasonSKID ADJUSTMENT_REASON_ID, \
SAR.Description DESCRIPTION, \
SAR.Sourcekey ADJUSTMENT_REASON_CODE, \
SAR.StockAdjustmentReasonType ADJUSTMENT_REASON_TYPE, \
SAR.UserName USER_NAME, \
SAR.CreationTime CREATION_TIME, \
SAR.LastUpdate LAST_UPDATE, \
SAR.ClosedTime CLOSED_TIME, \
RunDateTime as Run_date_time \
FROM \
columbus_curation.curateadls_stockAdjustmentReason SAR \
 where RunDateTime > CAST('"+str(max_load_date)+"'as timestamp) ")

# COMMAND ----------

upsertDWHSCD1(OutputDF,'CON_STG.DIM_ADJUSTMENT_REASON','con_columbus.DIM_ADJUSTMENT_REASON','ADJUSTMENT_REASON_ID',None,None)

# COMMAND ----------

