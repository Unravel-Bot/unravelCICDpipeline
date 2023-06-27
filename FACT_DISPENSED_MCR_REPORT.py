# Databricks notebook source
dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01 00:00:00","")
dbutils.widgets.text("Dateto","2022-06-30 00:00:00","")

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

dimPharmacySiteDF = synapse_sql("select SITE_CODE,SK_STORE,RECORD_START_DATE,RECORD_END_DATE,SCD_ACTIVE_FLAG from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,calendar_week,week_commencing_date from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

# COMMAND ----------

if(LoadType=="I"):
    DatetimeFilter1="cast(DI.DispensedDate as date) >= (select v_start from ProcessDate) and cast(DI.DispensedDate as date) <= (Select v_end from ProcessDate) "
   
else:
    DatetimeFilter1="cast(DI.DispensedDate as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)" 

print(DatetimeFilter1)

# COMMAND ----------

WeekToProcessDf=spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 \
THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar \
WHERE DATE_TIME_START=current_date-7) \
ELSE FINANCIAL_WEEK_NUMBER-1 \
END V_FIN_WEEK \
,CASE WHEN FINANCIAL_WEEK_NUMBER=1 \
THEN (SELECT FINANCIAL_YEAR FROM dimCalendar \
WHERE DATE_TIME_START=current_date-7) \
ELSE FINANCIAL_YEAR \
END V_FIN_YEAR FROM dimCalendar \
WHERE DATE_TIME_START=current_date")
WeekToProcessDf.createOrReplaceTempView('WeekToProcess') 

# COMMAND ----------

ProcessDateDf=spark.sql("select min(DATE_TIME_START) v_start \
,max(DATE_TIME_END) v_end \
FROM DimCalendar \
where FINANCIAL_YEAR =(select V_FIN_YEAR  FROM WeekToProcess) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WeekToProcess)")
ProcessDateDf.createOrReplaceTempView('ProcessDate')

# COMMAND ----------

TMP_DISP_MCRDF=spark.sql("SELECT PG.StoreCode \
,week_commencing_date \
,COUNT(*) DISP_MCR \
FROM columbus_curation.curateadls_prescriptiongroup PG \
INNER JOIN columbus_curation.curateadls_prescriptionform PF ON PF.PrescriptionGroupID=PG.PrescriptionGroupSKID \
INNER JOIN columbus_curation.curateadls_prescribeditem PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN columbus_curation.curateadls_dispenseditem DI ON DI.PrescribedItemID=PI.PrescribedItemSKID \
inner join dimcalendar dc on dc.DATE_TIME_START=cast(DI.DispensedDate as date) \
WHERE PF.PrescriptionFormTypeID in (SELECT PrescriptionFormTypeSKID FROM columbus_curation.curateadls_PrescriptionFormType WHERE SourceSystemID =12438 ) \
AND DI.Quantity>0 \
AND "+str(DatetimeFilter1)+" \
and UPPER(PF.PrescriptionFormStatus) <>'DELETED' \
and UPPER(PG.PrescriptionGroupStatus) <>'DELETED' \
GROUP BY PG.StoreCode,week_commencing_date ")
TMP_DISP_MCRDF.createOrReplaceTempView('TMP_DISP_MCR')

# COMMAND ----------

TMP_DISP_TOTDF=spark.sql("SELECT PG.StoreCode \
,dc.week_commencing_date \
,COUNT(*) DISP_TOT \
FROM columbus_curation.curateadls_prescriptiongroup PG \
INNER JOIN columbus_curation.curateadls_prescriptionform PF ON PF.PrescriptionGroupID=PG.PrescriptionGroupSKID \
INNER JOIN columbus_curation.curateadls_prescribeditem PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN columbus_curation.curateadls_dispenseditem DI ON DI.PrescribedItemID=PI.PrescribedItemSKID \
inner join dimcalendar dc on dc.DATE_TIME_START=cast(DI.DispensedDate as date) \
INNER JOIN TMP_DISP_MCR TM ON TM.StoreCode=PG.StoreCode and TM.week_commencing_date = dc.week_commencing_date \
WHERE \
DI.Quantity>0 \
AND "+str(DatetimeFilter1)+" \
and UPPER(PF.PrescriptionFormStatus) <>'DELETED' \
and UPPER(PG.PrescriptionGroupStatus) <>'DELETED' \
GROUP BY PG.StoreCode,dc.week_commencing_date")
TMP_DISP_TOTDF.createOrReplaceTempView('TMP_DISP_TOT')

# COMMAND ----------

FCT_DISPENSED_MCR_REPORTDF=spark.sql("SELECT \
cast(date_format(max(CAL.week_commencing_date),'yyyMMdd') as int) AS SK_SUN_DATE, \
cast(DS.SK_STORE as int) as SK_STORE, \
cast(DISP_TOT as int) DISPENSED_TOTAL, \
cast(NVL(DISP_MCR,0) as int) DISPENSED_MCR, \
cast(ROUND((NVL(DISP_MCR,0)*100)/DISP_TOT,2) as decimal(5,2)) DISPENSED_PERCENTAGE \
,current_timestamp as CREATE_DATETIME, \
cast(Null as timestamp) as UPDATE_DATETIME \
FROM TMP_DISP_TOT TOT \
INNER JOIN TMP_DISP_MCR MCR ON MCR.StoreCode=TOT.StoreCode \
INNER JOIN dimPharmacySite DS ON DS.Site_Code=TOT.StoreCode AND DS.SCD_ACTIVE_FLAG='Y' \
INNER JOIN dimCalendar CAL ON CAL.week_commencing_date=TOT.week_commencing_date and CAL.week_commencing_date=MCR.week_commencing_date \
group by DS.SK_STORE,cast(DISP_TOT as int),cast(NVL(DISP_MCR,0) as int),cast(ROUND((NVL(DISP_MCR,0)*100)/DISP_TOT,2) as decimal(5,2))")

# COMMAND ----------

append_to_synapse (FCT_DISPENSED_MCR_REPORTDF,'con_columbus.FACT_DISPENSED_MCR_REPORT')