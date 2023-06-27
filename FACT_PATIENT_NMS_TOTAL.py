# Databricks notebook source
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

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select MONTH_START_DATE,MONTH_END_DATE,DATE_TIME_START,DATE_KEY,MONTH_VALUE,YEAR_VALUE,FINANCIAL_YEAR,FINANCIAL_WEEK_NUMBER from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

FACT_PATIENT_NMS_DF=synapse_sql("select PATIENT_CODE,SK_STORE,SK_MONTH_DATE from con_columbus.FACT_PATIENT_NMS")
FACT_PATIENT_NMS_DF.createOrReplaceTempView('FACT_PATIENT_NMS')

# COMMAND ----------

MONTH_TO_PROCESS_DF=spark.sql("SELECT \
MONTH_START_DATE as V_START, \
MONTH_END_DATE as V_END, \
MONTH_VALUE as V_MONTH_VALUE,\
YEAR_VALUE as V_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=add_months(current_Date(),-1) ") 
MONTH_TO_PROCESS_DF.createOrReplaceTempView("MONTH_TO_PROCESS")

# COMMAND ----------

if(LoadType=="I"):
    DatetimeFilter1="cast(DATE_TIME_START as date) between (select v_start from MONTH_TO_PROCESS)  and (select v_end from MONTH_TO_PROCESS)"
else:
    DatetimeFilter1="cast(DATE_TIME_START as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)" 

print(DatetimeFilter1)


# COMMAND ----------

OutputDF=spark.sql("SELECT cast(F.SK_STORE as int) as SK_STORE, \
DC.MONTH_VALUE, \
DC.YEAR_VALUE, \
DC.FINANCIAL_YEAR, \
cast(DC.FINANCIAL_WEEK_NUMBER as int) as FINANCIAL_WEEK_NUMBER, \
cast(COUNT(DISTINCT(PATIENT_CODE)) as int) TOTAL_NUMBER_PATIENTS_NMS \
,current_timestamp as CREATE_DATETIME \
,cast(Null as timestamp) as UPDATE_DATETIME \
FROM FACT_PATIENT_NMS F INNER JOIN \
DIMPHARMACYSITE DS ON F.SK_STORE=DS.SK_STORE \
inner join DimCalendar DC ON F.SK_MONTH_DATE=DC.DATE_KEY \
WHERE "+str(DatetimeFilter1)+" \
GROUP BY F.SK_STORE, \
DC.MONTH_VALUE, \
DC.YEAR_VALUE, \
DC.FINANCIAL_YEAR, \
DC.FINANCIAL_WEEK_NUMBER ")

# COMMAND ----------

delete_and_append(OutputDF,"con_columbus.FACT_PATIENT_NMS_TOTAL","DELETE FROM con_columbus.FACT_PATIENT_NMS_TOTAL WHERE Month_value=format(dateadd(month,-1,getdate()),'MM') AND YEAR_VALUE=year(getdate()-1)")