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

FCT_ODS_OWINGSDF = synapse_sql("select SK_STORE,SK_PRESCRIBABLE_PRODUCT,OWING_QUANTITY,DATE_KEY from con_columbus.FACT_ODS_OWINGS")
FCT_ODS_OWINGSDF.createOrReplaceTempView('FCT_ODS_OWINGS')

dimCalendarDF = synapse_sql("select DATE_KEY,FINANCIAL_WEEK_NUMBER,MONTH_VALUE,YEAR_VALUE,DATE_TIME_START,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')


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
    weekly_calc_df = spark.sql("SELECT distinct FINANCIAL_WEEK_NUMBER as V_W,MONTH_VALUE as V_M,YEAR_VALUE as V_Y,FINANCIAL_YEAR as V_FY \
FROM dimCalendar join WEEK_TO_PROCESS \
WHERE FINANCIAL_WEEK_NUMBER=V_FIN_WEEK and FINANCIAL_YEAR=V_FIN_YEAR")
    weekly_calc_df.createOrReplaceTempView('weekly_calc')

# COMMAND ----------

if(LoadType=='H'):
    weekly_hist = spark.sql("SELECT distinct FINANCIAL_YEAR as V_FY \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)")
    weekly_hist.createOrReplaceTempView('weekly_hist')

# COMMAND ----------

############## history to be run for entire year ########################
if(LoadType=="I"):
      DateFilter="DD.FINANCIAL_WEEK_NUMBER =(select distinct V_W from weekly_calc) and DD.FINANCIAL_YEAR = (select distinct V_FY from weekly_calc)"
else:
      DateFilter="DD.FINANCIAL_YEAR in (select V_FY from weekly_hist)"
    
print(DateFilter)

# COMMAND ----------

OutputDF=spark.sql("SELECT \
cast(SK_STORE as int) as SK_STORE, \
cast(SK_PRESCRIBABLE_PRODUCT as int) as SK_PRESCRIBABLE_PRODUCT, \
cast(DD.FINANCIAL_WEEK_NUMBER as int) as FINANCIAL_WEEK_NUMBER, \
DD.MONTH_VALUE,DD.YEAR_VALUE, \
cast(COUNT(*) as int) AS TOTAL_OWINGS, \
CAST(SUM(OWING_QUANTITY) AS DECIMAL(15,5)) as TOTAL_OWED_QUANTITY, \
current_timestamp as CREATE_DATETIME, \
cast(Null as timestamp) as UPDATE_DATETIME \
FROM FCT_ODS_OWINGS OO \
INNER JOIN dimCalendar DD ON \
DD.DATE_KEY = OO.DATE_KEY \
where "+str(DateFilter)+" \
GROUP BY SK_STORE, \
SK_PRESCRIBABLE_PRODUCT, \
DD.FINANCIAL_WEEK_NUMBER, \
DD.MONTH_VALUE, \
DD.YEAR_VALUE ")

# COMMAND ----------

append_to_synapse (OutputDF,'CON_columbus.FACT_ODS_OWINGS_WEEKLY')