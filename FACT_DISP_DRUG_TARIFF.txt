# Databricks notebook source
dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01","")
dbutils.widgets.text("Dateto","2022-06-30","")

LoadType = dbutils.widgets.get("Loadtype")
DateFrom = dbutils.widgets.get("Datefrom")
DateTo = dbutils.widgets.get("Dateto")

# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------


dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,DAY_OF_WEEK_SDESC from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,RECORD_START_DATE,RECORD_END_DATE,pricing_region_number from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

RefLOVDF = synapse_sql("select LOVId,LOVName,LOVKey  from [SER_PHARMACEUTICALS].[RefLOV]")
RefLOVDF.createOrReplaceTempView('RefLOV')

DIM_DISPENSING_PACK_PRICEDF = synapse_sql("select PRODUCT_SKU_CODE,Record_Start_Date,Record_End_Date,PRICING_REGION,PP_UNIT_PRICE from con_columbus.DIM_DISPENSING_PACK_PRICE")
DIM_DISPENSING_PACK_PRICEDF.createOrReplaceTempView('DIM_DISPENSING_PACK_PRICE')


# COMMAND ----------

if(LoadType=='I'):
    WEEK_TO_PROCESS_DF=spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=Current_date()")
    WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")

# COMMAND ----------

if(LoadType=='I'):
    DATE_RANGE_OF_WEEK_DF=spark.sql("select \
cast(min(DATE_TIME_START + interval '1' day) as date) as v_start,\
cast(max(DATE_TIME_END + interval '1' day) as date) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
    DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")


# COMMAND ----------

if(LoadType=="I"):
  DatetimeFilter="cast(di.DISPENSEDDATE as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
else:
 DatetimeFilter="cast(di.DISPENSEDDATE as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)"
    
print(DatetimeFilter)

# COMMAND ----------

FCT_DRUG_TARIFF_df=spark.sql("select cast(ds.SK_STORE as int) as SK_STORE, \
cast(DI.DISPENSEDDATE as Date) as DISPENSED_DATE, \
cast(dc.DATE_KEY AS INT) as SK_DAILY_DATE, \
dp.DISPENSEDPRODUCTNAME as DISP_PRODUCT_NAME,\
cast(SUM(DI.QUANTITY) as int) as DISP_QUANTITY,\
cast(NVL(DDPP.PP_UNIT_PRICE,0) as decimal(24,4)) as DSKU_UNIT_COST,\
cast(SUM(DI.QUANTITY*NVL(DDPP.PP_UNIT_PRICE,0)) as decimal(24,4)) as DISP_ITEM_VALUE \
,current_timestamp() as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_PRESCRIPTIONGROUP PG \
INNER JOIN columbus_curation.curateadls_PRESCRIPTIONFORM PF ON PF.PRESCRIPTIONGROUPID = PG.prescriptiongroupSKID \
INNER JOIN columbus_curation.curateadls_PRESCRIBEDITEM PI ON PF.PRESCRIPTIONFORMSKID=PI.PRESCRIPTIONFORMID \
inner join columbus_curation.curateadls_PRESCRIBEDPRODUCT pp on pp.PRESCRIBEDITEMID = pi.PRESCRIBEDITEMSKID \
INNER JOIN columbus_curation.curateadls_DISPENSEDITEM DI ON PI.PRESCRIBEDITEMSKID=DI.PRESCRIBEDITEMID \
inner join columbus_curation.curateadls_DISPENSEDPRODUCT dp on dp.DISPENSEDITEMID = di.DISPENSEDITEMSKID \
inner join dimPharmacySite ds on ds.SITE_CODE = PG.STORECODE \
inner join dimCalendar dc on  dc.DATE_TIME_START =cast(di.dispenseddate as date)\
left JOIN DIM_DISPENSING_PACK_PRICE DDPP ON DDPP.PRODUCT_SKU_CODE = dp.PRODUCTSKUCODE and cast(di.DispensedDate as date) between cast(DDPP.Record_Start_Date as date) and cast(DDPP.Record_End_Date as date) \
and DS.pricing_region_number = DDPP.PRICING_REGION \
where \
"+str(DatetimeFilter)+" \
and DI.QUANTITY > 0 \
and PG.PrescriptionSourceType is not null \
and PG.SERVICETYPE is not null  \
and (PG.PrescriptionSourceType,PG.SERVICETYPE) not in (('PMS','CHS')) \
and pg.PRESCRIPTIONGROUPstatus <> 'Drafted' \
and upper(pg.PRESCRIPTIONGROUPstatus) <> upper('deleted') \
and upper(pf.PRESCRIPTIONformstatus) <> upper('deleted') \
and pi.status <> 'Deleted' \
and pi.IsNotDispensedIndicator <> 1 \
and pp.PrescribedProductType <> 'UNPRD' \
group by \
dc.DATE_KEY, \
ds.SK_STORE\
,dp.DISPENSEDPRODUCTNAME\
,NVL(DDPP.PP_UNIT_PRICE,0)\
,cast(DI.DISPENSEDDATE as date)")
            

# COMMAND ----------

append_to_synapse (FCT_DRUG_TARIFF_df,'con_columbus.FACT_DISP_DRUG_TARIFF')