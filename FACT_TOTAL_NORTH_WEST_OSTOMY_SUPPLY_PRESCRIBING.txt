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

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,Calendar_week, Month_value, year_value,day_of_week_number from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dim_calendar')
FCT_NWOS_PRESCRIBNGDF = synapse_sql("select SK_STORE,DISPENSED_DATE,SENT_TO_NWOS_FLAG,ELIGIBLE_FOR_NWOS_FLAG,SK_DAILY_DATE from con_columbus.FACT_NORTH_WEST_OSTOMY_SUPPLY_PRESCRIBING")
FCT_NWOS_PRESCRIBNGDF.createOrReplaceTempView('FACT_NORTH_WEST_OSTOMY_SUPPLY_PRESCRIBING')


# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f
calandar1=spark.sql("select DATE_KEY,case when day_of_week_number=2 then DATE_TIME_START end as min_date,DATE_TIME_START,Calendar_week,FINANCIAL_WEEK_NUMBER,Financial_year from dim_calendar order by date_time_start asc")
df1=calandar1.withColumn('min_date',f.last('min_date',True).over(Window.orderBy('DATE_TIME_START')))
df1.createOrReplaceTempView('dim_calendar1')
dim_calendar2 = spark.sql("select DATE_KEY, month(min_date) as month_value,year(min_date) as year_value, weekofyear(cast(min_date as date))  as Calendar_week,cast(DATE_TIME_START as date),FINANCIAL_WEEK_NUMBER,Financial_year  from dim_calendar1 ")
dim_calendar2.createOrReplaceTempView('dimCalendar2')
dim_calendar3 = spark.sql("select DATE_KEY,month_value,year_value,case when Calendar_week=1 and month_value=12 then 53 else calendar_week end as calendar_week,DATE_TIME_START,FINANCIAL_WEEK_NUMBER,Financial_year from dimcalendar2 ")
dim_calendar3.createOrReplaceTempView('dimCalendar3')
dim_calendar4 = spark.sql("select DATE_KEY,month_value,year_value,case when year_value in (2020,2026,2032,2037) then calendar_week-1 else calendar_week end as calendar_week,DATE_TIME_START,FINANCIAL_WEEK_NUMBER,Financial_year from dimcalendar3 ")
dim_calendar4.createOrReplaceTempView('dimCalendar')

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
min(date_Add(DATE_TIME_START,1)) as v_start,\
max(date_Add(DATE_TIME_START,1)) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
    DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")


# COMMAND ----------

if(LoadType=="I"):
  DatetimeFilter="cast(DISPENSED_DATE as date) between (select v_start from DATE_RANGE_OF_WEEK)  and (select v_end from DATE_RANGE_OF_WEEK)"
else:
 DatetimeFilter="cast(DISPENSED_DATE as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)"
    
print(DatetimeFilter)

# COMMAND ----------

TotalCount=spark.sql("select CALENDAR_WEEK  \
, MONTH_VALUE \
,YEAR_VALUE \
,count(ELIGIBLE_FOR_NWOS_FLAG) as TOT_ELEGIBLE_NWOS \
,count(CASE WHEN SENT_TO_NWOS_FLAG = 'Y' then SENT_TO_NWOS_FLAG end) as TOT_SENT_TO_NWOS \
from FACT_NORTH_WEST_OSTOMY_SUPPLY_PRESCRIBING \
inner join dimCalendar dc ON dc.DATE_KEY=SK_DAILY_DATE \
where "+str(DatetimeFilter)+" group by CALENDAR_WEEK, \
MONTH_VALUE, \
YEAR_VALUE ")
TotalCount.createOrReplaceTempView('TotalCount')

# COMMAND ----------

PerStoreCount=spark.sql("select fn.sk_store as SK_STORE, \
count(fn.ELIGIBLE_FOR_NWOS_FLAG) as TOT_ELEGIBLE_NWOS_PER_STORE, \
SUM(CASE WHEN FN.SENT_TO_NWOS_FLAG = 'Y' THEN 1 ELSE 0 END) as TOT_SENT_TO_NWOS_PER_STORE, \
CALENDAR_WEEK  \
, MONTH_VALUE  \
,YEAR_VALUE  \
from FACT_NORTH_WEST_OSTOMY_SUPPLY_PRESCRIBING fn \
inner join dimCalendar dc ON dc.DATE_KEY=SK_DAILY_DATE \
where "+str(DatetimeFilter)+" group by fn.sk_store, \
CALENDAR_WEEK, \
MONTH_VALUE, \
YEAR_VALUE  ")
PerStoreCount.createOrReplaceTempView('PerStoreCount')

# COMMAND ----------

TOTAL_NWOS_PRESCRIBNGDF=spark.sql("select cast(t.SK_STORE as int) as SK_STORE, \
cast(t.TOT_ELEGIBLE_NWOS_PER_STORE as int) as TOT_ELEGIBLE_NWOS_PER_STORE, \
cast(t.TOT_SENT_TO_NWOS_PER_STORE as int) as TOT_SENT_TO_NWOS_PER_STORE, \
cast(j.TOT_ELEGIBLE_NWOS as int) as TOT_ELEGIBLE_NWOS, \
cast(j.TOT_SENT_TO_NWOS as int) as TOT_SENT_TO_NWOS, \
cast(t.CALENDAR_WEEK as int) as CALENDAR_WEEK, \
cast(t.MONTH_VALUE as string) as MONTH_VALUE, \
cast(t.YEAR_VALUE as string) as YEAR_VALUE, \
Current_timestamp as CREATE_DATETIME, \
cast(Null as timestamp) as UPDATE_DATETIME \
from PerStoreCount t join TotalCount j  \
on concat(t.YEAR_VALUE,t.CALENDAR_WEEK)=concat(j.YEAR_VALUE,j.CALENDAR_WEEK) ")

# COMMAND ----------

append_to_synapse (TOTAL_NWOS_PRESCRIBNGDF,'con_columbus.FACT_TOTAL_NORTH_WEST_OSTOMY_SUPPLY_PRESCRIBING')