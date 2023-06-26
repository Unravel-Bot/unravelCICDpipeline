# Databricks notebook source
##### COBA-118330
##### USAIN
##### SPRINT 2

# COMMAND ----------

dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01","")
dbutils.widgets.text("Dateto","2022-06-30","")

LoadType = dbutils.widgets.get("Loadtype")
DateFrom = dbutils.widgets.get("Datefrom")
DateTo = dbutils.widgets.get("Dateto")

# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------


dimCalendarDF = synapse_sql("select DATE_KEY,DATE_TIME_START,DAY_OF_WEEK_SDESC,FINANCIAL_WEEK_NUMBER,Financial_year from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

# COMMAND ----------

if(LoadType=="I"):
    WEEK_TO_PROCESS_DF=spark.sql("SELECT \
    CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
    CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
    FROM dimCalendar \
    WHERE cast(DATE_TIME_START as date)=Current_date()")
    WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")

# COMMAND ----------

if(LoadType=="I"):
    DATE_RANGE_OF_WEEK_DF=spark.sql("select \
    min(cast(DATE_TIME_START as date)+1) as v_start,\
    max(cast(DATE_TIME_START as date)+1) as v_end \
    FROM dimCalendar \
    where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
    and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
    DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")
    

# COMMAND ----------

if(LoadType=="I"):
      DatetimeFilter1="and cast(dsp.CreationTime as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
else:
     DatetimeFilter1="and cast(dsp.CreationTime as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)"
#print(DatetimeFilter1)

# COMMAND ----------

FCT_NWOS_PRESCRIBNGDF=spark.sql("select cast(ds.sk_store as int) as SK_STORE, \
cast(dc.DATE_KEY as int) as SK_DAILY_DATE, \
CASE WHEN pf.ISELECTRONICINDICATOR = 1 then UNIQUEPRESCRIPTIONNUMBER else PF.SourceKey end as PRESCRIPTION_IDENTIFIER, \
CASE WHEN PA.SOURCEKEY IS NULL OR pa.Optedinindicator='F' THEN 'NA' else pg.patientcode end as PATIENT_CODE, \
max(dispenseddate) as DISPENSED_DATE, \
'Y' as  ELEGIBLE_FOR_NWOS_FLAG, \
CASE WHEN upper(PF.FULFILLED) = 'NWOS' THEN 'Y' ELSE 'N' END  AS SENT_TO_NWOS_FLAG, \
current_timestamp as CREATE_DATETIME, \
CAST(NULL AS timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_PRESCRIPTIONGROUP PG \
INNER JOIN columbus_curation.curateadls_PRESCRIPTIONFORM PF ON PF.PRESCRIPTIONGROUPID = PG.prescriptiongroupSKID \
INNER JOIN columbus_curation.curateadls_prescribeditem PI ON PF.prescriptionformSKID = PI.PRESCRIPTIONFORMID \
INNER JOIN columbus_curation.curateadls_DISPENSEDITEM DI ON PI.prescribeditemSKID = DI.prescribeditemid \
INNER join dimPharmacySite ds on ds.SITE_CODE = pg.storecode and cast(di.dispenseddate as date) >= cast(ds.RECORD_START_DATE as date) and  cast(di.dispenseddate as date) <= cast(ds.RECORD_END_DATE as date) \
inner join columbus_curation.curateadls_DispensingSupportPharmacyTracking dsp on pf.SourceKey=dsp.prescriptionformcode \
INNER join dimCalendar dc on cast(dc.DATE_TIME_START as date) = cast(dsp.CreationTime as date) \
left join columbus_curation.curateadls_PatientOptedInOut pa on pa.sourcekey=PG.PATIENTCODE AND pa.SCDactiveflag='Y' \
where dsp.process = 'NWOS Triage' and dsp.prescriptionformcode is not null \
"+str(DatetimeFilter1)+" \
and upper(pg.prescriptiongroupstatus) <> upper('deleted') \
and upper(pf.prescriptionformstatus) <> upper('deleted') \
and DI.QUANTITY > 0 and pi.isnotdispensedIndicator <> 1 \
group by \
ds.sk_store, \
dc.DATE_KEY, \
CASE WHEN PA.SOURCEKEY IS NULL OR pa.Optedinindicator='F' THEN 'NA' else pg.patientcode end, \
CASE WHEN pf.ISELECTRONICINDICATOR = 1 then UNIQUEPRESCRIPTIONNUMBER else PF.SourceKey end,\
CASE WHEN upper(PF.FULFILLED) = 'NWOS' THEN 'Y' ELSE 'N' END\
")

# COMMAND ----------

append_to_synapse (FCT_NWOS_PRESCRIBNGDF,'con_columbus.FACT_NORTH_WEST_OSTOMY_SUPPLY_PRESCRIBING')