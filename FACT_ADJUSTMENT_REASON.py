# Databricks notebook source
import datetime
dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01","")
dbutils.widgets.text("Dateto","2022-06-30","")

# COMMAND ----------

LoadType = dbutils.widgets.get("Loadtype")

DateFrom = dbutils.widgets.get("Datefrom")

DateTo = dbutils.widgets.get("Dateto")
print(LoadType)
print(DateFrom)
print(DateTo)

# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------


DIM_ADJUSTMENT_REASON_DF = synapse_sql("select ADJUSTMENT_REASON_ID from con_columbus.DIM_ADJUSTMENT_REASON")
DIM_ADJUSTMENT_REASON_DF.createOrReplaceTempView('DIM_ADJUSTMENT_REASON')

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,scd_active_flag from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

# COMMAND ----------

if(LoadType=='I'):
    WEEK_TO_PROCESS_DF=spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=Current_date()")
    WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")
    display(WEEK_TO_PROCESS_DF)

# COMMAND ----------

if(LoadType=='I'):
    DATE_RANGE_OF_WEEK_DF=spark.sql("select \
min(cast(DATE_TIME_START as date)+1) as v_start,\
max(cast(DATE_TIME_START as date)+1) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
    DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")
    display(DATE_RANGE_OF_WEEK_DF)


# COMMAND ----------

if(LoadType=="I"):
  DatetimeFilter="and cast(AD.CREATIONTIME as date) between v_start and v_end"
else:
 DatetimeFilter="and AD.CREATIONTIME between cast('"+str(DateFrom)+"' as timestamp) and cast('"+str(DateTo)+"' as timestamp)"
    
print(DatetimeFilter)

# COMMAND ----------

OutputDF=spark.sql("SELECT  \
ds.sk_store SK_STORE \
,AR.ADJUSTMENT_REASON_ID REASON_ID \
,PRODUCTSKUCODE PRODUCT_SKU_CODE \
,DATE_KEY  SK_ADJUSTED_DATE \
,ai.OnShelfUnitQuantity ON_SHELF_UNIT_QUANTITY \
,ai.ADJUSTEdPACKQUANTITY ADJUSTED_PACK_QUANTITY \
,ai.ADJUSTEDUNITQUANTITY ADJUSTED_UNIT_QUANTITY \
,current_timestamp() as CREATE_DATETIME \
,cast(Null as timestamp) as UPDATE_DATETIME \
from columbus_curation.curateadls_STOCKADJUSTMENT ad \
inner join columbus_curation.curateadls_STOCKADJUSTMENTITEM ai on ai.StockAdjustmentID=ad.StockAdjustmentSKID \
inner join DIM_ADJUSTMENT_REASON ar on ar.ADJUSTMENT_REASON_ID= ai.StockAdjustmentID \
INNER JOIN dimPharmacySite DS ON DS.SITE_CODE=AD.STORECODE \
INNER JOIN dimCalendar CAL ON cast(CAL.DATE_TIME_START as Date)=cast(ad.CreationTime as Date) \
inner join DATE_RANGE_OF_WEEK \
WHERE DS.scd_active_flag='Y' \
"+str(DatetimeFilter)+" ")

# COMMAND ----------

append_to_synapse (OutputDF,'con_columbus.FACT_ADJUSTMENT_REASON')