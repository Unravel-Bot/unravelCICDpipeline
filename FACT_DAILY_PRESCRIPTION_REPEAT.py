# Databricks notebook source
import datetime
import datetime
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
min(date_Add(DATE_TIME_START,1)) as v_start,\
max(date_Add(DATE_TIME_START,1)) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
    DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")
    display(DATE_RANGE_OF_WEEK_DF)


# COMMAND ----------

if(LoadType=="I"):
  DatetimeFilter1="and EP.ReceivedTime>=v_start and EP.ReceivedTime<=v_end"
  DatetimeFilter2="and pf.PrescriptionDate>v_start and pf.PrescriptionDate<=v_end"
else:
 DatetimeFilter1="and EP.ReceivedTime between cast(\'"+str(DateFrom)+"\' as timestamp) and cast(\'"+str(DateTo)+"\' as timestamp)"
 DatetimeFilter2="and pf.PrescriptionDate between cast(\'"+str(DateFrom)+"\' as timestamp) and cast(\'"+str(DateTo)+"\' as timestamp)"
print(DatetimeFilter1)
print(DatetimeFilter2)

# COMMAND ----------

#display(spark.sql("select DATE_TIME_START from dimCalendar where "+str(DatetimeFilter1)+" union all select current_date() as DATE_TIME_START from dimCalendar where "+str(DatetimeFilter2)+"  "))

# COMMAND ----------

OutputDF=spark.sql("SELECT  \
s.SK_STORE\
,cal.DATE_KEY SK_PRESCRIPTION_DATE \
,EP.RECEIVEDTIME PRESCRIPTION_DATE\
,ep.REPEATISSUENUMBER REPEAT_INDICATOR \
,ep.sourceElectronicPrescriptionID PRESCRIPTION_IDENTIFIER \
,'YES' as ELECTRONIC_PRESCRIPTION_SERVICE_FLAG \
,current_timestamp() as CREATE_DATETIME \
,cast(Null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_ElectronicPrescription EP \
INNER JOIN dimPharmacySite S ON S.site_code=EP.STORECODE AND s.scd_active_flag='Y' \
INNER JOIN dimCalendar CAL ON cast(CAL.DATE_TIME_START as date)=cast(EP.RECEIVEDTIME as date) \
inner join DATE_RANGE_OF_WEEK \
WHERE \
ep.REPEATISSUENUMBER is not null \
AND EP.ElectronicPrescriptionSTATUS<>'Deleted' \
"+str(DatetimeFilter1)+" \
union all \
SELECT \
s.SK_STORE \
,cal.DATE_KEY \
,PF.PRESCRIPTIONDATE  \
,'NULL' as REPEATISSUENUMBER \
,'NO' as EPS \
,PF.SourceKey \
,current_timestamp() as CREATE_DATETIME \
,cast(Null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_PrescriptionForm pf \
INNER JOIN columbus_curation.curateadls_prescriptiongroup pg on PG.PRESCRIPTIONGROUPskID=PF.PRESCRIPTIONGROUPid \
INNER JOIN columbus_curation.curateadls_PRESCRIPTIONFORMTYPE PT ON PT.PrescriptionFormTypeSKID=PF.PRESCRIPTIONFORMTYPEID and PT.DESCRIPTION='FP10RD' \
INNER JOIN dimPharmacySite S ON S.SITE_CODE=PG.STORECODE AND s.scd_active_flag='Y' \
INNER JOIN dimCalendar CAL ON cast(CAL.DATE_TIME_START as date)=cast(PF.PRESCRIPTIONDATE as date) \
inner join DATE_RANGE_OF_WEEK \
WHERE \
 PF.PrescriptionFormSTATUS <>'Deleted' \
"+str(DatetimeFilter2)+"")

# COMMAND ----------

append_to_synapse (OutputDF,'con_columbus.FACT_DAILY_PRESCRIPTION_REPEAT')