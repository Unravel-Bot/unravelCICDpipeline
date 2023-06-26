# Databricks notebook source
dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01 00:00:00","")
dbutils.widgets.text("Dateto","2022-06-30 23:59:59","")

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

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,PRICING_REGION_number,PRICING_REGION_name,scd_active_flag,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

DIM_PRESCRIBABLE_PRODUCT_DF = synapse_sql("select PRESCRIBABLE_PRODUCT_CODE,RECORD_START_DATE,RECORD_END_DATE,SK_PRESCRIBABLE_PRODUCT from con_columbus.DIM_PRESCRIBABLE_PRODUCT")
DIM_PRESCRIBABLE_PRODUCT_DF.createOrReplaceTempView('DIM_PRESCRIBABLE_PRODUCT')

DIM_DISPENSING_PACK_SIZE_DF = synapse_sql("select SK_DISPENSING_PACK_SIZE,RECORD_START_DATE,RECORD_END_DATE,DISPENSING_PACK_SIZE_code from con_columbus.DIM_DISPENSING_PACK_SIZE")
DIM_DISPENSING_PACK_SIZE_DF.createOrReplaceTempView('DIM_DISPENSING_PACK_SIZE')

DIM_ACTUAL_PRODUCT_PACK_DF = synapse_sql("select SK_ACTUAL_PRODUCT_PACK,ACTUAL_PRODUCT_PACK_CODE,RECORD_START_DATE,RECORD_END_DATE from con_columbus.DIM_ACTUAL_PRODUCT_PACK")
DIM_ACTUAL_PRODUCT_PACK_DF.createOrReplaceTempView('DIM_ACTUAL_PRODUCT_PACK')



# COMMAND ----------

WEEK_TO_PROCESS_DF=spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=Current_date()")
WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")

# COMMAND ----------

DATE_RANGE_OF_WEEK_DF=spark.sql("select \
min(cast(DATE_TIME_START as date)) as v_start,\
max(cast(DATE_TIME_START as date)) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")

# COMMAND ----------

if(LoadType=="I"):
    DatetimeFilter1="cast(di.DISPENSEDDATE as date)>= (select V_START from DATE_RANGE_OF_WEEK)  AND cast(di.DISPENSEDDATE as date)<=(select V_END from DATE_RANGE_OF_WEEK)"
else:
    DatetimeFilter1="cast(di.DISPENSEDDATE as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)" 

print(DatetimeFilter1)


# COMMAND ----------

#changed the join for INNER JOIN DIM_DISPENSING_PACK_SIZE dps on dps.DISPENSING_PACK_SIZE_code=Dp.ProductSKUCode(sourcekey to productskucode)
TMP_FCT_PREFERENCE_OVERRIDE=spark.sql(" SELECT \
ds.SK_STORE, \
ds.PRICING_REGION_NUMBER REGION, \
dpp.SK_PRESCRIBABLE_PRODUCT, \
dps.SK_DISPENSING_PACK_SIZE, \
di.REASONFORSUPPLY , \
date_format(di.DISPENSEDDATE,'yyyyMMdd') SK_DISPENSED_DATE \
,dap.SK_ACTUAL_PRODUCT_PACK \
,current_timestamp as CREATE_DATETIME \
,cast(Null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_PRESCRIPTIONgROUP PG  \
INNER JOIN columbus_curation.curateadls_PRESCRIPTIONFORM PF ON PF.PrescriptionGroupID = PG.PrescriptionGroupSKID \
INNER JOIN columbus_curation.curateadls_PRESCRIBEDITEM PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN columbus_curation.curateadls_DISPENSEDITEM DI ON DI.PRESCRIBEDITEMID = PI.PRESCRIBEDITEMSKID  \
INNER JOIN columbus_curation.curateadls_PRESCRIBEDPRODUCT PP ON PP.PrescribedItemID = PI.PrescribedItemSKID \
INNER JOIN columbus_curation.curateadls_DISPENSEDPRODUCT DP ON DP.DISPENSEDITEMid=DI.DispensedItemSKID \
INNER JOIN dimPharmacySite DS ON DS.SITE_CODE=PG.STORECODE \
INNER JOIN DIM_PRESCRIBABLE_PRODUCT dpp on  dpp.PRESCRIBABLE_PRODUCT_CODE=pp.productcode \
INNER JOIN DIM_DISPENSING_PACK_SIZE dps on dps.DISPENSING_PACK_SIZE_code=Dp.ProductSKUCode  \
INNER JOIN DIM_ACTUAL_PRODUCT_PACK dap on dap.ACTUAL_PRODUCT_PACK_CODE = DP.ActualProductPackCode \
WHERE  "+str(DatetimeFilter1)+" \
AND pf.PRESCRIPTIONFORMSTATUS<>'Deleted' \
AND pg.PRESCRIPTIONgROUPstatus not in ('Drafted','Deleted') \
AND di.REASONFORSUPPLY is not null \
AND ( Cast(di.DISPENSEDDATE as date)>= cast(DS.RECORD_START_DaTe as date) AND cast(di.DISPENSEDDATE as date) < cast(DS.RECORD_END_DATE as date))  \
AND (  cast(di.DISPENSEDDATE as date)>= cast(DPP.RECORD_START_DaTe as date) AND cast(di.DISPENSEDDATE as date) < cast(DPP.RECORD_END_DATE as date)) \
AND (  cast(di.DISPENSEDDATE as date)>= cast(DPS.RECORD_START_DaTe as date) AND cast(di.DISPENSEDDATE as date) < cast(DPS.RECORD_END_DATE as date)) \
AND  ( cast(di.DISPENSEDDATE as date)>= cast(DAP.RECORD_START_DaTe as date) AND cast(di.DISPENSEDDATE as date) < cast(DAP.RECORD_END_DATE as date)) \
 ")
TMP_FCT_PREFERENCE_OVERRIDE.createOrReplaceTempView("TMP_FCT_PREFERENCE_OVERRIDE")

# COMMAND ----------

OutputDF=spark.sql(" SELECT  \
cast(SK_STORE as int) as SK_STORE,REGION \
,cast(SK_PRESCRIBABLE_PRODUCT as int) as SK_PRESCRIBABLE_PRODUCT ,\
cast(SK_DISPENSING_PACK_SIZE as int) as SK_DISPENSING_PACK_SIZE,\
REASONFORSUPPLY as REASON,\
cast(SK_DISPENSED_DATE as int) as SK_DISPENSED_DATE,\
cast(SK_ACTUAL_PRODUCT_PACK  as int) as SK_ACTUAL_PRODUCT_PACK,\
current_timestamp as CREATE_DATETIME,\
cast(Null as timestamp) as UPDATE_DATETIME \
from TMP_FCT_PREFERENCE_OVERRIDE \
group by SK_STORE, \
REGION, \
SK_PRESCRIBABLE_PRODUCT, \
SK_DISPENSING_PACK_SIZE, \
REASONFORSUPPLY, \
cast(SK_DISPENSED_DATE as int), \
SK_ACTUAL_PRODUCT_PACK ")                                     

# COMMAND ----------

append_to_synapse(OutputDF,'con_columbus.FACT_PREFERENCE_OVERRIDE')