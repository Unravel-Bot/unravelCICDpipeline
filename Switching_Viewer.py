# Databricks notebook source
import pandas as pd
import os
import glob
import datetime

# COMMAND ----------

# MAGIC %run "../../Common/read_from_synapse"

# COMMAND ----------

# MAGIC %run "../../Common/Upsert_to_Synapse"

# COMMAND ----------

ProductDF = synapse_sql("select * from con_dwh.DimProduct where SCDActiveFlag =1")
ProductDF.createOrReplaceTempView('DimProduct')
DateDF = synapse_sql("select * from con_dwh.DimDate")
DateDF.createOrReplaceTempView('DimDate')



# COMMAND ----------

def getlatestfile(path):
    LMT=datetime.datetime(1970, 1, 1, 0, 0, 0,0)
    for file_item in os.listdir(path):
        file_path = os.path.join(path, file_item)
        ti_m = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if ti_m>LMT:
            LMT=ti_m
            LastModifiedFile=file_path[5:]
    return(LastModifiedFile)

#CPI_new=getlatestfile('/dbfs/mnt/self-serve/DSE/HADOOP/DSE_Legacy_CPI_Snapshot_Hadoop/Default/')
#print(CPI_new)

# COMMAND ----------

def getlast2files(path):
    LMT1=datetime.datetime(1970, 1, 1, 0, 0, 0,0)
    LMT2=datetime.datetime(1970, 1, 1, 0, 0, 0,0)
    LastModifiedFile1=""
    LastModifiedFile2=""
    filelist=os.listdir(path)
    for file_item in filelist:
        file_path = os.path.join(path, file_item)
        ti_m = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if ti_m>LMT2 and ti_m>LMT1:
            LMT2=LMT1
            LastModifiedFile2=LastModifiedFile1
            LMT1=ti_m
            LastModifiedFile1=file_path[5:]

        elif ti_m>LMT2:
            LMT2=ti_m
            LastModifiedFile2=file_path[5:]
    finallist=[]
    finallist.append(LastModifiedFile1)
    finallist.append(LastModifiedFile2)

    finallist[:] = [x for x in finallist if x]

    return(finallist)

# COMMAND ----------

CPI=getlast2files('/dbfs/mnt/self-serve/DSE/HADOOP/DSE_Legacy_CPI_Snapshot_Hadoop/Default/')
coeffs=getlast2files('/dbfs/mnt/self-serve/DSE/HADOOP/BootsUK_Snapshot_Loyalty_Brandcoeffs_Hadoop/Default/')
Source1=getlast2files('/dbfs/mnt/self-serve/DSE/HADOOP/BootsUk_Snapshot_Loyalty_Matrix_Hadoop/Default/')

print(CPI)
print(coeffs)
print(Source1)

# COMMAND ----------

#changes
#CPI='/mnt/self-serve/DSE/HADOOP/DSE_Legacy_CPI_Snapshot_Hadoop/Default/fact_CPI_sample*'
CPIDF=spark.read.format('csv').option('header',True).option("encoding","windows-1252").option('delimiter',',').load(CPI)
CPIDF.createOrReplaceTempView('uk_snapshot_cpi')

#coeffs='/mnt/self-serve/DSE/HADOOP/BootsUK_Snapshot_Loyalty_Brandcoeffs_Hadoop/Default/uk_snapshot_loyalty_brandcoeffs1/part*'
coeffsDF=spark.read.format('csv').option('header',True).option("encoding","windows-1252").option('delimiter',',').load(coeffs)
coeffsDF.createOrReplaceTempView('uk_snapshot_loyalty_brandcoeffs')


#Source1='/mnt/self-serve/DSE/HADOOP/BootsUk_Snapshot_Loyalty_Matrix_Hadoop/Default/uk_snapshot_loyalty_matrix'
SourceDF1=spark.read.format('csv').option('header',True).option("encoding","windows-1252").option('delimiter',',').load(Source1)
SourceDF1.createOrReplaceTempView('uk_snapshot_loyalty_matrix')

# COMMAND ----------

TempItems=spark.sql("Select distinct ProductSourceKey,RetailStatusCode from DimProduct where SCDActiveFlag=1 and RetailStatusCode in ('Active','Pending discontinuation')")
TempItems.createOrReplaceTempView('TempItems')

# COMMAND ----------

RunDates=spark.sql("select distinct end_date as PROJECTDATETIME,execution_code,Country from (select substring(execution_code,4,6) as calendar_week_year,max(execution_code) as execution_code,country from uk_snapshot_cpi where country in ('UK','IE') group by substring(execution_code,4,6),country) a inner join (select WeekStartDate as start_date,WeekEndDate as end_date, CalendarYear, CalendarWeek from DimDate) b on a.calendar_week_year = Concat(b.CalendarYear,b.CalendarWeek)")
RunDates.createOrReplaceTempView('RunDates')

# COMMAND ----------

BrandViewerDF=spark.sql("SELECT nvl(to_date(SWITCH_DATE,'d/M/yyyy'),to_date('31/12/9999','d/M/yyyy'))as SWITCH_DATE_P_KEY,SWITCHINGGROUPKEY as SWITCHING_GROUP_KEY,cast(case when COUNTRY is null then '-1' else COUNTRY end  as string) as REPORTING_COUNTRY_CODE_P_KEY,NVL(BRANDNAME1,'UNKNOWN') as BRAND_NAME1_P_KEY, nvl(BRANDNAME2,'UNKNOWN') as BRAND_NAME2_P_KEY,cast(nvl(CANN_PERCENT,'0.000') as decimal(30,3)) as CANN_PERCENT, cast(nvl(RBP,'0.000') as decimal(30,3)) as RBP,cast(nvl((max(RBP) over (partition by BRANDNAME1,SWITCH_DATE,SWITCHINGGROUP)),'0.000') as decimal(30,3)) as PROPORTION_OF_RBP, cast(nvl(PRODUCTCOUNT,'0') as int) as PRODUCT_COUNT,SWITCHINGGROUP,country FROM (SELECT u1.switching_group AS SWITCHINGGROUP, count(distinct u1.clientproductid) AS PRODUCTCOUNT, CONCAT(u1.switching_group,u1.country) as SWITCHINGGROUPKEY, u1.country AS COUNTRY, u2.brandname1 AS BRANDNAME1, u2.brandname2 AS BRANDNAME2,u2.cannpercent AS CANN_PERCENT, u6.PROJECTDATETIME AS SWITCH_DATE,(case when (u2.brandname1)=(u2.brandname2) then u2.cannpercent else 0 end )as RBP FROM uk_snapshot_cpi u1 inner join uk_snapshot_loyalty_brandcoeffs u2 on u1.switching_group =u2.switching_group inner JOIN TempItems u4 on u1.clientproductid= u4.ProductSourceKey inner join RunDates u6 on u1.execution_code = u6.execution_code and u2.execution_code = u6.execution_code and u1.country = u6.country WHERE (u2.switching_group = u1.switching_group and u2.brandname1 = u1.brand) and u1.country ='UK' and u2.execution_code in (select max(execution_code) from uk_snapshot_loyalty_matrix where country ='UK' and execution_code not in ('UK_201925_Z','UK 201927 (2016-07-03 to 2019-07-06)MEDIUM','UK 201927 (2016-07-03 to 2019-07-06)HIGH','UK 201927 (2016-07-03 to 2019-07-06)LOW')) and u1.aggregation = 'Switching Group' GROUP BY u1.switching_group,u1.country,u2.brandname1, u2.brandname2, u2.execution_code, u2.cannpercent,u6.PROJECTDATETIME) union SELECT nvl(to_date(SWITCH_DATE,'d/M/yyyy'),to_date('31/12/9999','d/M/yyyy')) as SWITCH_DATE_P_KEY,SWITCHINGGROUPKEY SWITCHING_GROUP_KEY, cast(case when COUNTRY is null then '-1' else COUNTRY end  as string) as REPORTING_COUNTRY_CODE_P_KEY,nvl(BRANDNAME1,'UNKNOWN') as BRAND_NAME1_P_KEY,nvl(BRANDNAME2,'UNKNOWN') as BRAND_NAME2_P_KEY,cast(nvl(CANN_PERCENT,'0.000') as decimal(30,3)) as CANN_PERCENT, cast(nvl(RBP,'0.000') as decimal(30,3)) as RBP,cast(nvl((max(RBP) over (partition by BRANDNAME1,SWITCH_DATE,SWITCHINGGROUP)),'0.000') as decimal(30,3)) as PROPORTION_OF_RBP, cast(nvl(PRODUCTCOUNT,'0') as int) as PRODUCT_COUNT, SWITCHINGGROUP,country FROM (SELECT u1.switching_group AS SWITCHINGGROUP, count(distinct u1.clientproductid) AS PRODUCTCOUNT, CONCAT(u1.switching_group,u1.country) as SWITCHINGGROUPKEY, u1.country AS COUNTRY, u2.brandname1 AS BRANDNAME1, u2.brandname2 AS BRANDNAME2,u2.cannpercent AS CANN_PERCENT,u6.PROJECTDATETIME AS SWITCH_DATE,case when (u2.brandname1)=(u2.brandname2) then u2.cannpercent else 0 end as RBP FROM uk_snapshot_cpi u1 inner join uk_snapshot_loyalty_brandcoeffs u2 on u1.switching_group= u2.switching_group inner JOIN TempItems u4 on u1.clientproductid = u4.ProductSourceKey inner join RunDates u6 on u1.execution_code = u6.execution_code and u2.execution_code = u6.execution_code and u1.country = u6.country WHERE (u2.switching_group = u1.switching_group and u2.brandname1 = u1.brand) and u1.country ='IE' and u2.execution_code in (select max(execution_code) from uk_snapshot_loyalty_brandcoeffs where country ='IE') and u1.aggregation = 'Switching Group' GROUP BY u1.switching_group,u1.country, u2.brandname1, u2.brandname2 ,u2.execution_code,u2.cannpercent, u6.PROJECTDATETIME)")
BrandViewerDF.createOrReplaceTempView('BrandViewer')


# COMMAND ----------

ProductViewerDF=spark.sql("SELECT distinct nvl(to_date(SWITCH_DATE,'d/M/yyyy'),to_date('31/12/9999','d/M/yyyy')) as SWITCH_DATE_P_KEY,SWITCHINGGROUPKEY as SWITCHING_GROUP_KEY,cast(case when COUNTRY is null then '-1' else COUNTRY end  as string) as REPORTING_COUNTRY_CODE_P_KEY,nvl(PRODUCT1,'UNKNOWN') as PRODUCT1_SOURCE_P_KEY, NVL(PRODUCT2,'UNKNOWN') as PRODUCT2_SOURCE_P_KEY, cast(nvl(CANN_PERCENT, '0.000') as decimal(30,3)) AS CANN_PERCENT,cast(NVL(RPP,'0.000') as decimal(30,3)) as RPP,cast(nvl((max(RPP) over (partition by PRODUCT1,SWITCH_DATE)),'0.000') as decimal(30,3)) as PROPORTION_OF_RPP, current_timestamp as CREATE_DATETIME, current_timestamp as UPDATE_DATETIME,SWITCHINGGROUP,country  FROM (SELECT u1.switching_group as SWITCHINGGROUP, CONCAT(u1.switching_group,u1.country) as SWITCHINGGROUPKEY, u6.PROJECTDATETIME AS SWITCH_DATE,u1.clientproductid1 AS PRODUCT1,u1.clientproductid2 AS PRODUCT2,u1.country AS COUNTRY,u1.cannpercent AS CANN_PERCENT,(case when (u1.CLIENTPRODUCTID1)=(u1.CLIENTPRODUCTID2)then u1.cannpercent else 0 end )as RPP FROM uk_snapshot_loyalty_matrix u1 inner JOIN TempItems u4 on u1.clientproductid1 = u4.ProductSourceKey inner join RunDates u6 on u1.execution_code = u6.execution_code and u1.country = u6.country WHERE u1.execution_code in (select max(execution_code) from uk_snapshot_loyalty_matrix where country ='UK' and execution_code not in ('UK_201925_Z','UK 201927 (2016-07-03 to 2019-07-06)MEDIUM','UK 201927 (2016-07-03 to 2019-07-06)HIGH','UK 201927 (2016-07-03 to 2019-07-06)LOW')) AND u1.cannpercent >= 0.05 and u1.country IN ('UK')) UNION SELECT distinct nvl(to_date(SWITCH_DATE,'d/M/yyyy'), to_date('31/12/9999','d/M/yyyy')) as SWITCH_DATE_P_KEY,SWITCHINGGROUPKEY as SWITCHING_GROUP_KEY,cast(case when COUNTRY is null then '-1' else COUNTRY end  as string) as REPORTING_COUNTRY_CODE_P_KEY,nvl(PRODUCT1,'UNKNOWN') as PRODUCT1_SOURCE_P_KEY, NVL(PRODUCT2,'UNKNOWN') as PRODUCT2_SOURCE_P_KEY,cast(nvl(CANN_PERCENT, '0.00') as decimal(30,3)) AS CANN_PERCENT,cast(nvl(RPP,'0.000') as decimal(30,3)) as RPP, cast(nvl((max(RPP) over (partition by PRODUCT1,SWITCH_DATE)),'0.000') as decimal(30,3)) as PROPORTION_OF_RPP,current_timestamp as CREATE_DATETIME,current_timestamp as UPDATE_DATETIME,SWITCHINGGROUP,country FROM (SELECT u1.switching_group as SWITCHINGGROUP, CONCAT(u1.switching_group,u1.country) as SWITCHINGGROUPKEY, u6.PROJECTDATETIME AS SWITCH_DATE, u1.clientproductid1 AS PRODUCT1, u1.clientproductid2 AS PRODUCT2,u1.country AS COUNTRY, u1.cannpercent AS CANN_PERCENT,(case when (u1.CLIENTPRODUCTID1)=(u1.CLIENTPRODUCTID2) then u1.cannpercent else 0 end)as RPP FROM uk_snapshot_loyalty_matrix u1 inner JOIN TempItems u4 on u1.clientproductid1 = u4.ProductSourceKey inner join RunDates u6 on u1.execution_code = u6.execution_code and u1.country = u6.country WHERE u1.execution_code in (select max(execution_code) from uk_snapshot_loyalty_matrix where country ='IE') AND u1.cannpercent >= 0.05 and u1.country IN ('IE'))")
ProductViewerDF.createOrReplaceTempView('ProductViewer')


# COMMAND ----------

SwitchingCategoryDF1=spark.sql("select distinct nvl(SWITCHING_GROUP_KEY,'UNKNOWN') as SWITCHING_GROUP_P_KEY, nvl(a.SWITCHING_GROUP,'UNKNOWN') as SWITCHING_GROUP,NVL(c1.CATEGORY,'UNKNOWN') as CATEGORY,NVL(a.COUNTRY,'-1') as COUNTRY_CODE,current_timestamp as CREATE_DATETIME, cast(null as timestamp) as UPDATE_DATETIME from ((select distinct SWITCHING_GROUP_KEY,COUNTRY,SWITCHINGGROUP as SWITCHING_GROUP from BrandViewer b1 where COUNTRY in ('IE','UK') union select distinct SWITCHING_GROUP_KEY,COUNTRY,SWITCHINGGROUP as SWITCHING_GROUP from ProductViewer where COUNTRY in ('IE','UK')) as a left join uk_snapshot_loyalty_matrix c1 on a.SWITCHING_GROUP=c1.switching_group)")
SwitchingCategoryDF1.createOrReplaceTempView('SwitchingCategory1')


# COMMAND ----------

RPTSwitchingCategoryDF=spark.sql("select distinct cast(DENSE_RANK() over (order by SWITCHING_GROUP_P_KEY,COUNTRY_CODE) as int) as SWITCHING_GROUP_KEY_S_KEY,SWITCHING_GROUP_P_KEY,SWITCHING_GROUP,CATEGORY, COUNTRY_CODE as REPORTING_COUNTRY_CODE_P_KEY,CREATE_DATETIME, UPDATE_DATETIME from SwitchingCategory1")
RPTSwitchingCategoryDF.createOrReplaceTempView('RPTSwitchingCategory')

# COMMAND ----------

RPT_SWITCHING_BRAND_VIEWER=spark.sql("select SWITCH_DATE_P_KEY,nvl(b.SWITCHING_GROUP_KEY_S_KEY,0) as SWITCHING_GROUP_KEY_S_KEY,REPORTING_COUNTRY_CODE_P_KEY,BRAND_NAME1_P_KEY,BRAND_NAME2_P_KEY, CANN_PERCENT,RBP,PROPORTION_OF_RBP,PRODUCT_COUNT,current_timestamp as CREATE_DATETIME, cast(null as timestamp) as UPDATE_DATETIME from BrandViewer a left outer join (select distinct SWITCHING_GROUP_KEY_S_KEY,SWITCHING_GROUP_P_KEY from RPTSwitchingCategory ) b on a.SWITCHING_GROUP_KEY=b.SWITCHING_GROUP_P_KEY")

# COMMAND ----------

RPT_SWITCHING_PRODUCT_VIEWER=spark.sql("select SWITCH_DATE_P_KEY,nvl(b.SWITCHING_GROUP_KEY_S_KEY,0) as SWITCHING_GROUP_KEY_S_KEY,REPORTING_COUNTRY_CODE_P_KEY,PRODUCT1_SOURCE_P_KEY,PRODUCT2_SOURCE_P_KEY, CANN_PERCENT,RPP,PROPORTION_OF_RPP,current_timestamp as CREATE_DATETIME, cast(null as timestamp) as UPDATE_DATETIME from ProductViewer a left outer join  (select distinct SWITCHING_GROUP_KEY_S_KEY,SWITCHING_GROUP_P_KEY from RPTSwitchingCategory ) b on a.SWITCHING_GROUP_KEY=b.SWITCHING_GROUP_P_KEY")

# COMMAND ----------

truncate_and_load_synapse(RPTSwitchingCategoryDF,'con_dwh.SS_DIM_RPT_SWITCHING_CATEGORY')

# COMMAND ----------

truncate_and_load_synapse(RPT_SWITCHING_BRAND_VIEWER,'con_dwh.RPT_SWITCHING_BRAND_VIEWER')

# COMMAND ----------

truncate_and_load_synapse(RPT_SWITCHING_PRODUCT_VIEWER,'con_dwh.RPT_SWITCHING_PRODUCT_VIEWER')