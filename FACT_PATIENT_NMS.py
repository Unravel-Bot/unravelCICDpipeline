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

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,scd_active_flag,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_KEY,DATE_TIME_START,MONTH_START_DATE,MONTH_END_DATE,MONTH_VALUE,YEAR_VALUE from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

# COMMAND ----------

if(LoadType=="I"):
    MONTH_TO_PROCESS_DF=spark.sql("SELECT \
MONTH_START_DATE as V_START, \
MONTH_END_DATE as V_END, \
MONTH_VALUE as V_MONTH_VALUE,\
YEAR_VALUE as V_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=add_months(current_Date(),-1)")
    MONTH_TO_PROCESS_DF.createOrReplaceTempView("MONTH_TO_PROCESS")


# COMMAND ----------

if(LoadType=="I"):
    DatetimeFilter1="cast(DI.DispensedDate as date) between (select v_start from MONTH_TO_PROCESS)  and (select v_end from MONTH_TO_PROCESS)"
else:
    DatetimeFilter1="cast(DI.DispensedDate as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)" 

print(DatetimeFilter1)


# COMMAND ----------

EPIF_DF=spark.sql("SELECT PrescriptionGroupCode,StoreCode as STORE_CODE,CreationTime FROM columbus_curation.curateadls_electronicpharmacistsinformationformlabel WHERE IsNewMedicineServiceVerifiedIndicator=1")
EPIF_DF.createOrReplaceTempView("EPIF")

# COMMAND ----------

OutputDF=spark.sql("select cast(SK_MONTH_DATE as int) SK_MONTH_DATE, \
cast(SK_STORE as int) as SK_STORE, \
PATIENT_CODE, \
DISPENSED_DATE, \
current_timestamp() as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME from (SELECT \
date_format(DC.MONTH_START_DATE,'yyyyMMdd') as SK_MONTH_DATE, \
DC.MONTH_VALUE, \
DC.YEAR_VALUE, \
SK_STORE, \
PG.PatientCode AS PATIENT_CODE, \
MIN(cast(DI.DispensedDate as date)) as DISPENSED_DATE \
FROM columbus_curation.curateadls_PrescriptionGroup PG INNER JOIN columbus_curation.curateadls_PrescriptionForm PF ON PF.PrescriptionGroupID=PG.PrescriptionGroupSKID INNER JOIN columbus_curation.curateadls_PrescribedItem PI \
ON PI.PrescriptionFormID=PF.PrescriptionFormSKID INNER JOIN columbus_curation.curateadls_DispensedItem DI \
ON DI.PrescribedItemID=PI.PrescribedItemSKID INNER JOIN EPIF EL ON EL.STORE_CODE=PG.StoreCode \
AND EL.PrescriptionGroupCode=PG.SourceKey INNER JOIN columbus_curation.curateadls_Patient PT ON PT.SourceKey =PG.PatientCode \
INNER JOIN DIMPHARMACYSITE DS ON DS.SITE_CODE=PG.StoreCode \
INNER JOIN dimCalendar DC ON DC.DATE_TIME_START = cast(DI.DispensedDate AS DATE) \
WHERE PT.PatientStatus='Active' \
AND UPPER(pg.PrescriptionGroupStatus) NOT IN ('DELETED','DRAFTED') \
AND UPPER(pf.PrescriptionFormStatus) NOT IN ('DELETED','DRAFTED') \
AND UPPER(pi.Status) NOT IN ('DELETED','DRAFTED') \
AND (current_date()>= cast(DS.RECORD_START_DATE as date) \
and current_date()< cast(DS.RECORD_END_DATE as date))  \
AND "+str(DatetimeFilter1)+" GROUP BY date_format(DC.MONTH_START_DATE,'yyyyMMdd'),DC.MONTH_VALUE,DC.YEAR_VALUE,SK_STORE,PATIENT_CODE) a")

# COMMAND ----------

delete_and_append(OutputDF,"con_columbus.FACT_PATIENT_NMS","DELETE FROM con_columbus.FACT_PATIENT_NMS WHERE datepart(Month,(convert(varchar(10),SK_MONTH_DATE,120)))=month(dateadd(month,-1,getdate())) AND datepart(year,(convert(varchar(10),SK_MONTH_DATE,120)))=year(getdate()-1)")