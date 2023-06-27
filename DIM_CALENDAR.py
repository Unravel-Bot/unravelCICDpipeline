# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------


Source='/dbfs/mnt/idf-curate/R4_Dim_calendar/DIM_CALENDAR.xlsx'
sourceDF=pd.read_excel(Source)
finalDF=spark.createDataFrame(sourceDF)
finalDF.createOrReplaceTempView('dim_calendar')



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_calendar

# COMMAND ----------

OutputDF=spark.sql("SELECT   \
cast(date_key  as int) \
,cast(JULIAN_DAY  as int) \
,DATE_TIME_START \
,DATE_TIME_END \
,DATE_VALUE \
,cast(DAY_OF_WEEK_NUMBER  as int)\
,DAY_OF_WEEK_DESC \
,DAY_OF_WEEK_SDESC \
,cast(WEEKEND_FLAG  as int) \
,cast(DAY_OF_MONTH_NUMBER  as int)\
,cast(MONTH_VALUE as varchar(2)) \
,MONTH_DESC \
,MONTH_SDESC \
,cast(MONTH_START_DATE as date)\
,cast(MONTH_END_DATE  as date)\
,cast(DAYS_IN_MONTH   as int) \
,cast(LAST_DAY_OF_MONTH_FLAG as int) \
,cast(DAY_OF_YEAR_NUMBER  as int) \
,cast(YEAR_VALUE  as varchar(4)) \
,YEAR_DESC \
,YEAR_SDESC \
,cast(YEAR_START_DATE  as date) \
,cast(YEAR_END_DATE  as date)\
,cast(LAST_DAY_OF_YEAR_FLAG as int) \
,cast(DAYS_IN_YEAR  as int) \
,cast(START_FIN_YEAR  as date) \
,cast(END_FIN_YEAR  as date)\
,cast(WEEKS_IN_FIN_YEAR  as int)\
,cast(FINANCIAL_WEEK_NUMBER as int)  \
,cast(DAYS_IN_FIN_YEAR as int)\
,FINANCIAL_YEAR \
,cast(FINANCIAL_MONTH_NUMBER  as int)  \
,cast(FINANCIAL_QUARTER_NUMBER  as int)  \
,cast(WEEK_COMMENCING_DATE as Date) \
,weekofyear(WEEK_COMMENCING_DATE)  as CALENDAR_WEEK \
,current_timestamp as CREATE_DATETIME \
,cast(null as timestamp) as UPDATE_DATETIME \
from dim_calendar ")

# COMMAND ----------

truncate_and_load_synapse(OutputDF,'con_columbus.DIM_CALENDAR')