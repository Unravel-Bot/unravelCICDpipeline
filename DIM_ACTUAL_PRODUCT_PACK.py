# Databricks notebook source
# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------


max_load_date=last_loaded_date("con_columbus.DIM_ACTUAL_PRODUCT_PACK")
print(max_load_date)

# COMMAND ----------

maxSKIDdf = get_max_SK('con_columbus.DIM_ACTUAL_PRODUCT_PACK','SK_ACTUAL_PRODUCT_PACK')
maxSKID = maxSKIDdf.select("SK_ACTUAL_PRODUCT_PACK").first()[0]
maxSKID = int(maxSKID or 0)


# COMMAND ----------

temp1_df  = spark.sql("select distinct b.PIPCode,b.PackName,b.Sourcekey,b.BuyingMultiple,b.IsBuyingMultipleOptimisationIndicator,b.IsBuyingMultipleEnforcedIndicator,b.ReceivingQuantity,b.IsSpecialItemIndicator,b.DmdSpecial,b.LEGALCATEGORY,b.RunDateTime,b.status,b.SUPPLIERCODE,b.IsTenderLineIndicator,b.PharmacyProductSKUID from (select * from columbus_curation.curateadls_actualproductpack where PharmacyProductSKUID is not null) b, (select SourceKey,max(RunDateTime) as RunDateTime  from columbus_curation.curateadls_actualproductpack where PharmacyProductSKUID is not null group by SourceKey ) a  where a.RunDateTime = b.RunDateTime and a.SourceKey = b.SourceKey and b.PharmacyProductSKUID is not null")
temp1_df.createOrReplaceTempView('temp_APP')

# COMMAND ----------

temp2_df  = spark.sql("select distinct b.* from (select * from columbus_curation.curateadls_PharmacyProductSKU) b, (select SourceKey,max(RunDateTime) as RunDateTime  from columbus_curation.curateadls_PharmacyProductSKU group by SourceKey ) a  where a.RunDateTime = b.RunDateTime and a.SourceKey = b.SourceKey")
temp2_df.createOrReplaceTempView('temp_PPS')

# COMMAND ----------

OutputDF=spark.sql("select distinct \
cast('"+str(maxSKID)+"'+ row_number() over (order by a.Sourcekey ) as int) as SK_ACTUAL_PRODUCT_PACK, \
A.PIPCode as PIP_CODE,\
a.PackName as PACK_NAME,\
a.Sourcekey as ACTUAL_PRODUCT_PACK_CODE,\
ps.SourceKey as DISPENSING_PACK_SIZE_CODE,\
cast(A.BuyingMultiple as int) as BUYING_MULTIPLE,\
cast(A.IsBuyingMultipleOptimisationIndicator as int) as BUYING_MULTIPLE_OPTIMISATION_IND, \
cast(A.IsBuyingMultipleEnforcedIndicator as int) as BUYING_MULTIPLE_ENFORCED_IND,\
cast(A.ReceivingQuantity as decimal(15,5)) as RECEIVING_QUANTITY,\
cast(A.IsSpecialItemIndicator as int) as SPECIAL_ITEM_IND,\
A.STATUS as ACTUAL_PRODUCT_PACK_STATUS,\
A.SUPPLIERCODE as SUPPLIER_CODE,\
cast(A.IsTenderLineIndicator as int) as TENDER_LINE_IND, \
A.DmdSpecial as DMD_SPECIAL,\
A.LEGALCATEGORY as LEGAL_CATEGORY, \
cast('9999-12-31 11:59:59' as timestamp ) as RECORD_END_DATE, \
A.RunDateTime as RUN_DATE_TIME, \
current_timestamp as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
from temp_APP a, temp_PPS ps \
where A.PharmacyProductSKUID = PS.PharmacyProductSKUID  \
and a.RunDateTime > CAST('"+str(max_load_date)+"' as timestamp) ") 
OutputDF.createOrReplaceTempView("Output")

# COMMAND ----------

APP1 = synapse_sql("select ACTUAL_PRODUCT_PACK_CODE from con_columbus.DIM_ACTUAL_PRODUCT_PACK where scd_active_flag='Y'")
APP1.createOrReplaceTempView('APP1')


# COMMAND ----------

DIM_ACTUAL_PRODUCT_PACK = synapse_sql("select * from con_columbus.DIM_ACTUAL_PRODUCT_PACK where scd_active_flag='Y'")
DIM_ACTUAL_PRODUCT_PACK.createOrReplaceTempView('DIM_ACTUAL_PRODUCT_PACK')

# COMMAND ----------

app_df=spark.sql("select a.*,case when b.ACTUAL_PRODUCT_PACK_CODE is not null then current_timestamp else cast('1900-01-01 00:00:00' as timestamp) end RECORD_START_DATE from output a left join APP1 b on a.ACTUAL_PRODUCT_PACK_CODE=b.ACTUAL_PRODUCT_PACK_CODE")
app_df.createOrReplaceTempView('app_df')

# COMMAND ----------

Final=spark.sql("select ot.* from app_df ot left join DIM_ACTUAL_PRODUCT_PACK DP on nvl(OT.PIP_CODE,0)=nvl(DP.PIP_CODE,0) and ot.PACK_NAME=dp.PACK_NAME and ot.ACTUAL_PRODUCT_PACK_CODE=dp.ACTUAL_PRODUCT_PACK_CODE and ot.DISPENSING_PACK_SIZE_CODE=dp.DISPENSING_PACK_SIZE_CODE and nvl(ot.BUYING_MULTIPLE,0)=nvl(dp.BUYING_MULTIPLE,0) and ot.BUYING_MULTIPLE_OPTIMISATION_IND=dp.BUYING_MULTIPLE_OPTIMISATION_IND and ot.BUYING_MULTIPLE_ENFORCED_IND=dp.BUYING_MULTIPLE_ENFORCED_IND and ot.ACTUAL_PRODUCT_PACK_STATUS=dp.ACTUAL_PRODUCT_PACK_STATUS and ot.TENDER_LINE_IND=dp.TENDER_LINE_IND and ot.DMD_SPECIAL=dp.DMD_SPECIAL and ot.LEGAL_CATEGORY=dp.LEGAL_CATEGORY where DP.ACTUAL_PRODUCT_PACK_CODE is null")

# COMMAND ----------

Column="cast('"+str(maxSKID)+"'+ row_number() over (order by ACTUAL_PRODUCT_PACK_CODE ) as int) as SK_ACTUAL_PRODUCT_PACK,PIP_CODE,PACK_NAME,ACTUAL_PRODUCT_PACK_CODE,DISPENSING_PACK_SIZE_CODE,BUYING_MULTIPLE,BUYING_MULTIPLE_OPTIMISATION_IND,BUYING_MULTIPLE_ENFORCED_IND,RECEIVING_QUANTITY,SPECIAL_ITEM_IND,ACTUAL_PRODUCT_PACK_STATUS,SUPPLIER_CODE,TENDER_LINE_IND,DMD_SPECIAL,LEGAL_CATEGORY,RECORD_START_DATE,RECORD_END_DATE,'Y' as SCD_ACTIVE_FLAG,RUN_DATE_TIME,CREATE_DATETIME,UPDATE_DATETIME"

# COMMAND ----------

upsertDWHscd2(Final,'con_stg.DIM_ACTUAL_PRODUCT_PACK','con_columbus.DIM_ACTUAL_PRODUCT_PACK','ACTUAL_PRODUCT_PACK_CODE',None,None,Column)
