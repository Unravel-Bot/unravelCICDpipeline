# Databricks notebook source
# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

DIM_DISPENSING_PACK_SIZEDF = synapse_sql("select SK_DISPENSING_PACK_SIZE,DISPENSING_PACK_SIZE_CODE,RECORD_START_DATE,RECORD_END_DATE from con_columbus.DIM_DISPENSING_PACK_SIZE ")
DIM_DISPENSING_PACK_SIZEDF.createOrReplaceTempView('DIM_DISPENSING_PACK_SIZE')

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,PRICING_REGION_number,scd_active_flag,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

Fact_Overstock_PSKUDF=synapse_sql("select DAY_IN_OVERSTOCK,SK_STORE,PRODUCT_SKU_CODE,CREATION_TIME from con_columbus.FACT_OVERSTOCK_PRODUCT_STOCK_KEEPING_UNITS")
Fact_Overstock_PSKUDF.createOrReplaceTempView('Fact_Overstock_PSKU')

# COMMAND ----------

OutputDF=spark.sql("SELECT \
cast(ds.SK_STORE as int) as SK_STORE,\
cast(d.date_key as int) as SK_DATE, \
cast(SK_DISPENSING_PACK_SIZE as int) as SK_DISPENSING_PACK_SIZE, \
os.StoreCode as STORE_CODE, \
i.productskucode PRODUCT_SKU_CODE, \
i.type OVERSTOCK_TYPE, \
case when DAY_IN_OVERSTOCK is null then nvl(os.UpdateTime,os.CREATIONTIME) else os.CREATIONTIME end as CREATION_TIME,\
cast(i.quantity as decimal(15,5)) as OVERSTOCK_QUANTITY, \
case when DAY_IN_OVERSTOCK is null then 1 else DAY_IN_OVERSTOCK+1 end as DAY_IN_OVERSTOCK \
FROM Columbus_Curation.curateadls_OverStock os INNER JOIN Columbus_Curation.curateadls_OverStockItem I ON I.OverStockID = os.OverStockSKID \
inner join dimPharmacySite ds on \
ds.SITE_CODE = os.StoreCode \
and current_date()-1 >= cast(ds.RECORD_START_DATE as date) AND current_date()-1 < cast(ds.RECORD_END_DATE as date)\
inner join DIM_DISPENSING_PACK_SIZE dps on \
dps.DISPENSING_PACK_SIZE_CODE = I.ProductSKUCode and \
current_date()-1 >= cast(dps.RECORD_START_DATE as date) and \
current_date()-1 < cast(dps.RECORD_END_DATE as date) \
inner JOIN dimCalendar D on cast(os.UpdateTime as date) = D.date_time_start \
LEFT JOIN (SELECT DAY_IN_OVERSTOCK,SK_STORE,PRODUCT_SKU_CODE FROM FACT_OVERSTOCK_PSKU WHERE to_date(CREATION_TIME) = current_date()-2) ld \
on ld.Sk_STORE = ds.SK_STORE \
and ld.PRODUCT_SKU_CODE = I.ProductSKUCode \
WHERE I.Quantity <> 0 \
")

# COMMAND ----------

upsertDWHSCD1(OutputDF,'CON_STG.FACT_OVERSTOCK_PRODUCT_STOCK_KEEPING_UNITS','con_columbus.FACT_OVERSTOCK_PRODUCT_STOCK_KEEPING_UNITS','STORE_CODE,PRODUCT_SKU_CODE,OVERSTOCK_TYPE',None,None)