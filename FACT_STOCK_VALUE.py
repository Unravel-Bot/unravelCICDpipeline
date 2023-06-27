# Databricks notebook source
# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------



DIM_DISPENSING_PACK_SIZEDF = synapse_sql("select DISPENSING_PACK_SIZE_CODE,SCD_ACTIVE_FLAG,SK_DISPENSING_PACK_SIZE from con_columbus.DIM_DISPENSING_PACK_SIZE ")
DIM_DISPENSING_PACK_SIZEDF.createOrReplaceTempView('DIM_DISPENSING_PACK_SIZE')

dim_pharmacy_siteDF = synapse_sql("select SITE_CODE,SCD_ACTIVE_FLAG,SK_STORE,PRICING_REGION_NUMBER from  con_columbus.dim_pharmacy_site")
dim_pharmacy_siteDF.createOrReplaceTempView('dim_pharmacy_site')
DimDateDF = synapse_sql("select DATE_KEY as dateint,month_value as calendarmonth,calendar_week as calendarweek,year_value as calendaryear,DATE_TIME_START from con_columbus.dim_calendar")
DimDateDF.createOrReplaceTempView('DimCalendar')

RefLOVDF = synapse_sql("select LOVId,LOVName,LOVKey  from [SER_PHARMACEUTICALS].[RefLOV]")
RefLOVDF.createOrReplaceTempView('RefLOV')
DIM_DISPENSING_PACK_PRICEDF = synapse_sql("select Product_SKU_Code,PP_UNIT_PRICE,scd_active_FLAG,PRICING_REGION  from con_columbus.DIM_DISPENSING_PACK_PRICE")
DIM_DISPENSING_PACK_PRICEDF.createOrReplaceTempView('DIM_DISPENSING_PACK_PRICE')

# COMMAND ----------

OutputDF=spark.sql("SELECT distinct cast(DS.SK_STORE as int) as SK_STORE, \
PRODUCTSKUCODE PRODUCT_SKU_CODE, \
cast(dateint  as int) as DATE_KEY, \
current_Date()-1 STOCK_VALUE_DATE, \
cast(SK_DISPENSING_PACK_SIZE as int) as SK_DISPENSING_PACK_SIZE, \
cast(st.OnShelfQuantity as decimal(15,5)) as STOCK_ON_HAND, \
cast(pp.PP_UNIT_PRICE  as decimal(24,4)) asPRESCRIBABLE_PRODUCT_UNIT_PRICE, \
current_timestamp() as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
from columbus_curation.curateadls_stock st \
INNER JOIN DIM_DISPENSING_PACK_SIZE PS ON ST.ProductSKUCode=PS.DISPENSING_PACK_SIZE_CODE \
INNER JOIN DIM_DISPENSING_PACK_PRICE PP ON PP.Product_SKU_Code=ST.ProductSKUCode \
inner join dim_pharmacy_site DS ON DS.SITE_CODE=ST.STORECODE \
INNER JOIN DimCalendar CAL ON CAL.DATE_TIME_START=current_date()-1 \
WHERE  pS.scd_active_FLAG='Y' AND  pp.scd_active_FLAG='Y' AND DS.scd_active_flag='Y' and ds.PRICING_REGION_NUMBER=pp.PRICING_REGION ")

# COMMAND ----------

append_to_synapse(OutputDF,'con_columbus.FACT_STOCK_VALUE')