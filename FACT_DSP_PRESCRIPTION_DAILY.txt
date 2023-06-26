# Databricks notebook source
##### COBA-118328
##### USAIN 
##### SPRINT 3

# COMMAND ----------

dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01 00:00:00","")
dbutils.widgets.text("Dateto","2022-06-30 23:59:59","")

# COMMAND ----------

LoadType = dbutils.widgets.get("Loadtype")

DateFrom = dbutils.widgets.get("Datefrom")

DateTo = dbutils.widgets.get("Dateto")


# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

# DBTITLE 1,COBA-118328 Add region number on dim_pharmacy select

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,pricing_region_number,scd_active_flag from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

DIM_ACTUAL_PRODUCT_PACKDF=synapse_sql("select ACTUAL_PRODUCT_PACK_CODE,SCD_ACTIVE_FLAG,SK_ACTUAL_PRODUCT_PACK from con_columbus.DIM_ACTUAL_PRODUCT_PACK")
DIM_ACTUAL_PRODUCT_PACKDF.createOrReplaceTempView('DIM_ACTUAL_PRODUCT_PACK')

# COMMAND ----------

WEEK_TO_PROCESS_DF=spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=Current_date()")
WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")

# COMMAND ----------

DATE_RANGE_OF_WEEK_DF=spark.sql("select \
cast(min(DATE_TIME_START ) as date) as v_start,\
cast(max(DATE_TIME_END) as date) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")

# COMMAND ----------

if(LoadType=="I"):
    DatetimeFilter1="and cast(TR.CREATIONTIME as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
else:
    DatetimeFilter1="and cast(TR.CREATIONTIME as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)" 


# COMMAND ----------

TMP_DSP_SCRIPTSDF=spark.sql("SELECT \
TR.PrescriptionGroupCode,NVL(IsPartialIndicator,0) as IsPartialIndicator,MAX(TR.CreationTime) DSP_PROCESS_DATE \
FROM columbus_curation.curateadls_DispensingSupportPharmacyTracking TR \
LEFT JOIN columbus_curation.curateadls_DispensingSupportPharmacyPrescription DS ON DS.SourceKey=TR.PrescriptionGroupCode \
WHERE TR.Result not in ('Triage Not Passed','Triage Passed - Sent To DAC Queue','Resubmissions - Sent To Due Date Queue','Triage Passed - Not Sent To DSP') \
and TR.process IN ('Capped Processing','Triage') \
"+str(DatetimeFilter1)+" AND NVL(DS.IsExcludedIndicator,0)=0 \
GROUP BY TR.PrescriptionGroupCode ,NVL(IsPartialIndicator,0) ")
TMP_DSP_SCRIPTSDF.createOrReplaceTempView('TMP_DSP_SCRIPTS')

# COMMAND ----------

OutputDF=spark.sql("SELECT \
cast(SK_STORE as int) as SK_STORE, \
cast(CAL.DATE_KEY AS int) as SK_CREATION_DSP_DATE, \
cast(FINANCIAL_WEEK_NUMBER as int) as FINANCIAL_WEEK_NUMBER, \
FINANCIAL_YEAR as FINANCIAL_YEAR, \
PG.StoreCode, \
CASE WHEN S.pricing_region_number=1 AND (PA.SOURCEKEY IS NULL OR pa.Optedinindicator='F') THEN 'NA' else \
CASE WHEN pf.IsElectronicIndicator=1 then UniquePrescriptionNumber else PF.SourceKey end end PRESCRIPTION_IDENTIFIER, \
cast(DSP.IsPartialIndicator as int) as PG_PART_FILL, \
cast(AP.SK_ACTUAL_PRODUCT_PACK as int) as SK_ACTUAL_PRODUCT_PACK, \
PI.Fulfilled as DISPENSED_MODE, \
cast(DI.Quantity as decimal(15,5)) AS DISPENSED_QUANTITY, \
MAX(cast(DI.DispensedDate as date)) AS Dispensed_Date \
,current_timestamp as CREATE_DATETIME \
,cast(null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_PrescriptionGroup PG \
INNER JOIN columbus_curation.curateadls_PrescriptionForm PF ON PF.PrescriptionGroupID=PG.PrescriptionGroupSKID \
INNER JOIN columbus_curation.curateadls_PrescribedItem PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN TMP_DSP_SCRIPTS DSP ON DSP.PrescriptionGroupCode=PG.SourceKey \
INNER JOIN columbus_curation.curateadls_DispensedItem DI ON DI.PrescribedItemID=PI.PrescribedItemSKID \
INNER JOIN columbus_curation.curateadls_DispensedProduct DP ON DP.DispensedItemID=DI.DispensedItemSKID \
INNER JOIN dimPharmacySite S ON S.SITE_CODE=PG.StoreCode AND S.SCD_ACTIVE_FLAG='Y' \
INNER JOIN DIMCALENDAR CAL ON cast(CAL.DATE_TIME_START as date) = cast(DSP.DSP_PROCESS_DATE as date) \
INNER JOIN DIM_ACTUAL_PRODUCT_PACK AP ON AP.ACTUAL_PRODUCT_PACK_CODE=DP.ActualProductPackCode AND AP.SCD_ACTIVE_FLAG='Y' \
left join columbus_curation.curateadls_PatientOptedInOut pa on pa.sourcekey=PG.PATIENTCODE AND pa.SCDactiveflag='Y' \
where PG.PrescriptionGroupStatus='Completed' \
AND UPPER(PF.PrescriptionFormStatus)<>'DELETED' \
AND DI.Quantity>0 \
AND PI.IsNotDispensedIndicator=0 \
GROUP BY SK_STORE, \
CAL.DATE_KEY , \
FINANCIAL_WEEK_NUMBER , \
FINANCIAL_YEAR , \
PG.StoreCode, \
CASE WHEN S.pricing_region_number=1 AND (PA.SOURCEKEY IS NULL OR pa.Optedinindicator='F') THEN 'NA' else \
CASE WHEN pf.IsElectronicIndicator=1 then UniquePrescriptionNumber else PF.SourceKey end end, \
DSP.IsPartialIndicator, \
AP.SK_ACTUAL_PRODUCT_PACK, \
PI.Fulfilled , \
DI.Quantity")

# COMMAND ----------

append_to_synapse (OutputDF,'con_columbus.FACT_DSP_PRESCRIPTION_DAILY')