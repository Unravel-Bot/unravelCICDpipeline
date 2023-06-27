# Databricks notebook source
import pandas as pd
import os
import glob
from pyspark.sql.functions import explode
from datetime import date, timedelta, datetime
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import max,lit, col
from delta.tables import *

# COMMAND ----------

# MAGIC %run "../../Common/Upsert_to_Synapse"

# COMMAND ----------

# MAGIC %run "../../Common/read_from_synapse"

# COMMAND ----------

currentyearmonth=spark.sql("select concat(Year,month) as currentyearmonth from (select cast(year(current_date()) as string) as Year, case when length(month(current_date()))='1' then Concat(0,month(current_date())) else month(current_date()) end as month)l ").first()["currentyearmonth"]
print(currentyearmonth)

# COMMAND ----------

d = datetime.datetime.strptime(currentyearmonth, '%Y%m').date()
LatestTwoMonth =(d - relativedelta(months=2)).strftime('%Y%m')
print(LatestTwoMonth)


# COMMAND ----------

FactTransactionLineDF = synapse_sql("select  ProductSKey,OrganisationSKeyRetailer,SiteSKey,TransactionDate,TransactionTime,BasketSKey,TISP,Units,TransactionDatetime,YearMonth from [con_dwh].[FactTransactionLine]  where YearMonth >'{0}'".format(LatestTwoMonth))
FactTransactionLineDF.createOrReplaceTempView('FactTransactionLine')
DimOrganisationDF = synapse_sql("select distinct  SCDEndDate,SCDStartDate,OrganisationSKey,OrganisationName from [con_dwh].[DimOrganisation] where OrganisationName ='Boots UK Ltd'")
DimOrganisationDF.createOrReplaceTempView('DimOrganisation')
DimSiteDF = synapse_sql("select distinct SCDEndDate,SCDStartDate,SiteName,SiteSKey,StoreFormat,SitesourceKey from [con_dwh].[DimSite]")
DimSiteDF.createOrReplaceTempView('DimSite')
DimProductDF = synapse_sql("select distinct ProductSKey,ProductHierarchy3Name,SCDEndDate,SCDStartDate from [con_dwh].[DimProduct]")
DimProductDF.createOrReplaceTempView('DimProduct')
DimDateDF = synapse_sql("select distinct CalendarWeek,CalendarYear,DayName,SalesPlanPeriodA,SalesPlanPeriodB,Date from [con_dwh].[DimDate]")
DimDateDF.createOrReplaceTempView('DimDate')

# COMMAND ----------

sourceDF = spark.read.option("multiline", "true").json('/mnt/self-serve/DSE/DSEEXCEL/BootsUK_UK_Footfall data_SPRINGBOARD_MSEXCEL/Historical/Footfall*')
sourceDF.createOrReplaceTempView('uk_footfalldata')


# COMMAND ----------

outputDF=spark.sql("SELECT date(processdate) AS TransactionDate,hour(processdate) AS TransactionHour,substring(sitename,9,4) as Sitenumber, SUM(acceptedin) AS Footfall,cast(trim (LEADING '0' FROM substring(sitename,9,4)) as int)  as StoreNumber FROM uk_footfalldata GROUP BY date(processdate),hour(processdate),substring(sitename,9,4),cast(trim (LEADING '0' FROM substring(sitename,9,4)) as int)")
outputDF.createOrReplaceTempView("footfalldata")

# COMMAND ----------

outputDF1=spark.sql("SELECT  StoreNumber,Sitenumber,TransactionHour, TransactionDate FROM footfalldata")
outputDF1.createOrReplaceTempView("footfall_live_stores")

# COMMAND ----------

OutputDF3=spark.sql("SELECT trim(FTL.TransactionDate) AS TRANSACTION_DATE,trim(hour(FTL.TransactionTime)) AS TRANSACTION_HOUR,trim(DD.CalendarWeek) AS CALENDAR_WEEK,trim(CONCAT(DD.CalendarYear,case when length(DD.Calendarweek)=1 then concat('0',cast(DD.Calendarweek as string)) else cast(DD.Calendarweek as string) end)) AS YEAR_WEEK,trim(DD.DayName) AS DAY_NAME,trim(CONCAT('p',DD.SalesPlanPeriodA,'a')) AS SALES_PLAN_PERIOD_A,trim(CONCAT('p',DD.SalesPlanPeriodB,'b')) AS SALES_PLAN_PERIOD_B,trim(FLS.Storenumber)AS SITE_SOURCE_KEY,trim(DS.SiteName)AS SITE_NAME,DS.StoreFormat AS STORE_FORMAT,trim(CONCAT(fls.Sitenumber,' - ',DS.SiteName) )AS STORE_FILTER,trim(DP.ProductHierarchy3Name) AS ITEM_HIERARCHY_3_DESCRIPTION,trim(CASE WHEN DO.OrganisationName = 'Boots UK Ltd'THEN 'UK'END) AS COUNTRY_CODE,COUNT(distinct FTL.BasketSKey) AS TRANSACTION_COUNT,SUM(FTL.TISP) AS TRANSACTION_SALES_SUM,SUM(FTL.Units) AS UNITS from FactTransactionLine FTL LEFT JOIN DimSite DS ON FTL.SiteSKey = DS.SiteSKey INNER JOIN footfall_live_stores FLS ON DS.SiteSourceKey = FLS.StoreNumber LEFT JOIN DimProduct DP ON FTL.ProductSKey = DP.ProductSKey  LEFT JOIN DimOrganisation DO ON FTL.OrganisationSKeyRetailer = DO.OrganisationSKey LEFT JOIN DimDate DD ON FTL.TransactionDate = DD.Date  WHERE  DO.OrganisationName ='Boots UK Ltd' AND FTL.TransactionDate  BETWEEN DS.SCDStartDate AND DS.SCDEndDate AND FTL.TransactionDate  BETWEEN DO.SCDStartDate AND DO.SCDEndDate AND FTL.TransactionDate  BETWEEN DP.SCDStartDate AND DP.SCDEndDate and Hour(FTL.TransactionDatetime)=FLS.TransactionHour  and FTL.TransactionDate=FLS.Transactiondate  GROUP BY CONCAT(fls.Sitenumber,' - ',DS.SiteName),FTL.TransactionDate,HOUR(FTL.TransactionTime),DD.CalendarWeek,CONCAT(DD.CalendarYear,case when length(DD.Calendarweek)=1 then concat('0',cast(DD.Calendarweek as string)) else cast(DD.Calendarweek as string) end),DD.DayName,CONCAT('p',DD.SalesPlanPeriodA,'a'),CONCAT('p',DD.SalesPlanPeriodB,'b'),FLS.StoreNumber, DS.SiteName, DS.StoreFormat,DP.ProductHierarchy3Name,CASE WHEN DO.OrganisationName = 'Boots UK Ltd' THEN 'UK' END")
OutputDF3.createOrReplaceTempView("GDH_footfall_transactions")

# COMMAND ----------

outputDF4=spark.sql("SELECT cast(NVL(GDH_footfall_transactions.Transaction_Date,'9999-12-31') as Date) AS TRANSACTION_DATE_P_KEY,cast(NVL(GDH_footfall_transactions.Transaction_Hour,0) as int) AS TRANSACTION_HOUR_P_KEY,cast(NVL(GDH_footfall_transactions.CALENDAR_WEEK,0) as Int) AS CALENDAR_WEEK,cast(NVL(GDH_footfall_transactions.YEAR_WEEK,0) as int) AS YEAR_WEEK,NVL(substring(GDH_footfall_transactions.DAY_NAME,0,3),'UNKNOWN') AS DAY_NAME,NVL(GDH_footfall_transactions.SALES_PLAN_PERIOD_A,'UNKNOWN') AS SALES_PLAN_A_PERIOD,NVL(GDH_footfall_transactions.SALES_PLAN_PERIOD_B,'UNKNOWN') AS SALES_PLAN_B_PERIOD,NVL(cast(GDH_footfall_transactions.SITE_SOURCE_KEY as String),'UNKNOWN') AS SITE_SOURCE_KEY_P_KEY,NVL(GDH_footfall_transactions.SITE_NAME,'UNKNOWN') AS SITE_NAME, NVL(GDH_footfall_transactions.STORE_FORMAT,'UNKNOWN') AS STORE_FORMAT,NVL(GDH_footfall_transactions.STORE_FILTER,'UNKNOWN') AS  STORE_FILTER,NVL(GDH_footfall_transactions.ITEM_HIERARCHY_3_DESCRIPTION,'UNKNOWN') AS ITEM_HIERARCHY_3_DESCRIPTION,NVL(GDH_footfall_transactions.COUNTRY_CODE,'-1') AS REPORTING_COUNTRY_CODE_P_KEY,cast(NVL(footfalldata.Footfall,0) as int) AS FOOTFALL,cast(NVL(GDH_footfall_transactions.TRANSACTION_COUNT,0) as int) AS TRANSACTION_COUNT,cast(NVL(GDH_footfall_transactions.TRANSACTION_SALES_SUM,0.0000) as decimal(30,4)) AS TRANSACTION_SALES_SUM,cast(NVL(GDH_footfall_transactions.UNITS,0.0000) as decimal(30,4)) AS UNITS,current_timestamp() as CREATE_DATETIME,cast(null as timestamp) as UPDATE_DATETIME  FROM footfalldata  inner join  GDH_footfall_transactions  ON  GDH_footfall_transactions.SITE_SOURCE_KEY=footfalldata.StoreNumber and GDH_footfall_transactions.Transaction_Date=footfalldata.transactiondate and GDH_footfall_transactions.Transaction_Hour=footfalldata.TransactionHour")
# outputDF4.createOrReplaceTempView("final")

# COMMAND ----------

footfall_last2month = date.today().replace(day=1) -relativedelta(months=1)
last2Year = date.today() - relativedelta(years=2)
print(footfall_last2month)
print(last2Year)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, '/mnt/self-serve/Temp_Tables/FootFall/')
deltaTable.delete(col('TRANSACTION_DATE_P_KEY') >= footfall_last2month)
outputDF4.write.option('header','true').partitionBy('YEAR_WEEK').mode('append').save('/mnt/self-serve/Temp_Tables/FootFall/')

# COMMAND ----------

latest2yeardata = spark.read.option('header', True).load('/mnt/self-serve/Temp_Tables/FootFall/').where(col('TRANSACTION_DATE_P_KEY') >= last2Year)

# COMMAND ----------

truncate_and_load_synapse(latest2yeardata,'con_dwh.RPT_FOOTFALL')