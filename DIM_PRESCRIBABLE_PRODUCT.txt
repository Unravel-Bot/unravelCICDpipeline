# Databricks notebook source
import  datetime


# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

maxSKIDdf = get_max_SK('con_columbus.DIM_PRESCRIBABLE_PRODUCT','SK_PRESCRIBABLE_PRODUCT')
maxSKID = maxSKIDdf.select("SK_PRESCRIBABLE_PRODUCT").first()[0]
maxSKID = int(maxSKID or 0)

# COMMAND ----------

max_load_date=last_loaded_date("con_columbus.DIM_PRESCRIBABLE_PRODUCT")
print(max_load_date)

# COMMAND ----------

OutputDF=spark.sql("SELECT  \
SourceSystemID as ID_PRODUCT, \
cast('"+str(maxSKID)+"' + row_number() over (order by sourcekey) as int) as SK_PRESCRIBABLE_PRODUCT, \
sourcekey AS PRESCRIBABLE_PRODUCT_CODE, \
ProductName AS PRODUCT_NAME, \
ProductClass AS PRODUCT_CLASS, \
ControlledDrugCode AS CONTROLLED_DRUG_CODE, \
ProductStatus AS STATUS, \
IsSugarFreeIndicator AS SUGAR_FREE_IND, \
RunDateTime as RUN_DATE_TIME, \
current_timestamp() as RECORD_START_DATE, \
cast('9999-12-31 11:59:59' as timestamp ) as RECORD_END_DATE, \
current_timestamp as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_pharmacyproduct \
where RunDateTime > CAST('"+str(max_load_date)+"'as timestamp)")

# COMMAND ----------

Column="ID_PRODUCT,cast('"+str(maxSKID)+"' + row_number() over (order by PRESCRIBABLE_PRODUCT_CODE) as int),PRESCRIBABLE_PRODUCT_CODE,PRODUCT_NAME,PRODUCT_CLASS,CONTROLLED_DRUG_CODE,STATUS,SUGAR_FREE_IND,'Y' as SCD_ACTIVE_FLAG,RUN_DATE_TIME,RECORD_START_DATE,RECORD_END_DATE,CREATE_DATETIME,UPDATE_DATETIME"

# COMMAND ----------

upsertDWHscd2(OutputDF,'con_stg.DIM_PRESCRIBABLE_PRODUCT','con_columbus.DIM_PRESCRIBABLE_PRODUCT','PRESCRIBABLE_PRODUCT_CODE',None,None,Column)


# COMMAND ----------

