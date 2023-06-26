# Databricks notebook source
import datetime


# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

max_load_date=last_loaded_date("con_columbus.DIM_DISPENSING_PACK_SIZE")
print(max_load_date)

# COMMAND ----------

maxSKIDdf = get_max_SK('con_columbus.DIM_DISPENSING_PACK_SIZE','SK_DISPENSING_PACK_SIZE')
maxSKID = maxSKIDdf.select("SK_DISPENSING_PACK_SIZE").first()[0]
maxSKID = int(maxSKID or 0)
print(maxSKID)

# COMMAND ----------

OutputDF=spark.sql("SELECT \
cast('"+str(maxSKID)+"'+ row_number() over (order by Sourcekey ) as int) as SK_DISPENSING_PACK_SIZE, \
Sourcekey AS DISPENSING_PACK_SIZE_CODE, \
ProductskuStatus AS STATUS, \
ProductSKUName AS PRODUCT_SKU_NAME, \
PackQuantity  AS PACK_QUANTITY, \
IsCSSPLineIndicator AS CARE_SERVICES_SUPPORT_PHARMACY_LINE_IND, \
IsRetailTypeIndicator AS RETAIL_TYPE_IND, \
RunDateTime as RUN_DATE_TIME, \
current_date() as RECORD_START_DATE, \
cast('9999-12-31 11:59:59' as timestamp ) as RECORD_END_DATE, \
current_timestamp as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_pharmacyproductsku pps \
where RunDateTime > CAST('"+str(max_load_date)+"' as timestamp) ") 

# COMMAND ----------

Column="cast('"+str(maxSKID)+"' + row_number() over (order by DISPENSING_PACK_SIZE_CODE) as int),DISPENSING_PACK_SIZE_CODE,STATUS,PRODUCT_SKU_NAME,PACK_QUANTITY,RETAIL_TYPE_IND,CARE_SERVICES_SUPPORT_PHARMACY_LINE_IND,'Y' as SCD_ACTIVE_FLAG,RUN_DATE_TIME,RECORD_START_DATE,RECORD_END_DATE,CREATE_DATETIME,UPDATE_DATETIME"

# COMMAND ----------

upsertDWHscd2(OutputDF,'con_stg.DIM_DISPENSING_PACK_SIZE','con_columbus.DIM_DISPENSING_PACK_SIZE','DISPENSING_PACK_SIZE_CODE',None,None,Column)

# COMMAND ----------

