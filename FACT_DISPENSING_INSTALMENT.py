# Databricks notebook source
import datetime
from pyspark.sql.functions import explode,split,col,date_format
from pyspark.sql.types import DateType
dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01","")
dbutils.widgets.text("Dateto","2022-06-30","")

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

# max_load_date=last_loaded_date("con_columbus.FACT_DISPENSING_INSTALMENT")
# print(max_load_date)

# COMMAND ----------

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')
dimPharmacySiteDF = synapse_sql("select site_code,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

# COMMAND ----------

WEEK_TO_PROCESS_DF=spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=Current_date()")
WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")


# COMMAND ----------

DATE_RANGE_OF_WEEK_DF=spark.sql("select \
min(cast(DATE_TIME_START as date)) as v_start,\
max(cast(DATE_TIME_START as date)) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")
display(DATE_RANGE_OF_WEEK_DF)

# COMMAND ----------

# --and  DS.STOCK_VALID_FROM_DT IS NOT NULL \ column not present in DS
TMP_INSTALMENT_DF = spark.sql("SELECT PI.StoreCode,DispensedDate,DI.PrescribedItemID,NVL(II.IsCollectedIndicator,0) COLLECTED \
from  dimPharmacySite DS \
INNER JOIN columbus_curation.curateadls_PrescribedItem PI on DS.SITE_CODE=PI.StoreCode \
inner JOIN columbus_curation.curateadls_PrescriptionForm pf ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN columbus_curation.curateadls_DispensedItem DI ON DI.PrescribedItemID=PI.PrescribedItemSKID \
INNER JOIN columbus_curation.curateadls_InstalmentItem II ON II.DispensedItemCode=DI.SourceKey \
WHERE (DispensedDate)>=(select v_start from DATE_RANGE_OF_WEEK) \
AND DispensedDate <=(select v_end from DATE_RANGE_OF_WEEK) \
AND II.InstalmentStatus='Dispensed' \
AND UPPER(PF.PrescriptionFormStatus) <> 'DELETED' \
AND DI.Quantity>0 \
AND DS.RECORD_START_DATE<=DispensedDate \
AND DS.RECORD_END_DATE>DispensedDate \
and PI.IsNotDispensedIndicator <>1 \
and PF.PrescriptionFormTypeID in (19,36,56,64,82)")

# COMMAND ----------

################check with Daffrin on ID column not brough in TMP_INStallment
TMP_PAT_INST_DF =spark.sql("SELECT STORE_CODE,DispensedDate,COUNT(DISTINCT PatientCode) PATIENT_INSTALMENT , \
count(distinct(DI.PrescribedItemID)) ITEM_INSTALMENT \
FROM ( \
SELECT PG.PatientCode,DispensedDate DispensedDate,PG.StoreCode,DI.PrescribedItemID \
FROM columbus_curation.curateadls_PrescriptionGroup PG \
INNER JOIN columbus_curation.curateadls_PrescriptionForm PF ON PF.PrescriptionGroupID=PG.PrescriptionGroupSKID \
INNER JOIN (SELECT ID FROM columbus_curation.curateadls_PrescriptionFormType \
WHERE DESCRIPTION IN ('FP10MDA', 'WP10MDA', 'HBP(A)', 'SP1', 'SP2')) FT ON FT.PrescriptionFormTypeSKID=PF.PrescriptionFormTypeID \
INNER JOIN columbus_curation.curateadls_PrescribedItem PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN columbus_curation.curateadls_DispensedItem DI ON DI.PrescribedItemID=PI.PrescribedItemSKID \
INNER JOIN columbus_curation.curateadls_InstalmentItem II ON II.DispensedItemCode=DI.SourceKey \
where (DispensedDate)>=(select v_start from DATE_RANGE_OF_WEEK) \
AND DispensedDate <=(select v_end from DATE_RANGE_OF_WEEK) \
AND DI.Quantity>0 \
AND II.InstalmentStatus='Dispensed' \
and PI.IsNotDispensedIndicator <>1 \
AND UPPER(PG.PrescriptionGroupStatus) NOT IN ('DRAFTED','DELETED') \
AND UPPER(PF.PrescriptionFormStatus) <> 'DELETED' \
AND II.Ordering=1) \
GROUP BY STORE_CODE,DispensedDate")
TMP_PAT_INST_DF.createOrReplaceTempView('TMP_PAT_INST')

# COMMAND ----------

TMP_NOT_INST_DF = spark.sql("SELECT STORE_CODE, DispensedDate DispensedDate,COUNT(DISTINCT PatientCode) PATIENT_NOT_INSTALMENT,COUNT(distinct (DI.PrescribedItemID)) ITEM_NOT_INSTALMENT \
FROM ( \
SELECT PG.PatientCode,PG.StoreCode,DI.PrescribedItemID,DI.DispensedDate \
FROM PrescriptionGroup PG \
INNER JOIN PrescriptionForm PF ON PF.PrescriptionGroupID=PG.PrescriptionGroupSKID \
INNER JOIN (SELECT ID FROM PrescriptionFormType \
WHERE DESCRIPTION NOT IN ( 'FP10MDA', 'WP10MDA', 'HBP(A)', 'SP1', 'SP2')) FT ON FT.PrescriptionFormTypeSKID=PF.PrescriptionFormTypeID \
INNER JOIN PrescribedItem PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN DispensedItem DI ON DI.PrescribedItemID=PI.PrescribedItemSKID \
WHERE (DispensedDate)>=(select v_start from DATE_RANGE_OF_WEEK) \
AND DispensedDate <=(select v_end from DATE_RANGE_OF_WEEK) \
and (PG.ServiceType,PG.PrescriptionSourceType) NOT IN (('CHS','PMS')) \
AND DI.Quantity>0 \
and PI.IsNotDispensedIndicator <>1 \
AND UPPER(PF.PrescriptionFormStatus) <> 'DELETED' \
AND UPPER(PG.PrescriptionGroupStatus) NOT IN ('DRAFTED','DELETED')) \
GROUP BY STORE_CODE,(DispensedDate)")

# COMMAND ----------

NOT_COLL_DF = spark.sql("SELECT STORE_CODE,DispensedDate,COUNT(DISTINCT ID) ITEM_INSTALMENT_NOT_COLLECTED \
FROM TMP_INSTALMENT \
WHERE COLLECTED <>1 \
GROUP BY STORE_CODE,DispensedDate")
NOT_COLL_DF.createOrReplaceTempView('NOT_COLL')

# COMMAND ----------

COLL_DF = spark.sql("SELECT STORE_CODE,DispensedDate,COUNT(DISTINCT ID) ITEM_INSTALMENT_COLLECTED \
FROM TMP_INSTALMENT \
WHERE COLLECTED =1 \ 
GROUP BY STORE_CODE,DispensedDate")
COLL_DF.createOrReplaceTempView('COLL')

# COMMAND ----------

OutputDF = spark.sql("SELECT \
ISNULL(DS.SK_STORE,-1) \
,ISNULL(DM.DATE_KEY,-1) \
,nvl(PATIENT_INSTALMENT,0) PATIENT_INSTALMENT \
,nvl(PATIENT_NOT_INSTALMENT,0) PATIENT_NOT_INSTALMENT \
,nvl(ITEM_INSTALMENT,0) ITEM_INSTALMENT \
,nvl(ITEM_NOT_INSTALMENT,0) ITEM_NOT_INSTALMENT \
,nvl(ITEM_INSTALMENT_COLLECTED,0) ITEM_INSTALMENT_COLLECTED \
,nvl(ITEM_INSTALMENT_NOT_COLLECTED,0) ITEM_INSTALMENT_NOT_COLLECTED \
FROM dimPharmacySite DS \
INNER JOIN DIM_CALENDAR DM ON DM.DATE_TIME_START>=(select v_start from DATE_RANGE_OF_WEEK) \
AND DM.DATE_TIME_START <=(select v_end from DATE_RANGE_OF_WEEK) \
LEFT JOIN TMP_NOT_INST NI ON NI.STORE_CODE=DS.SITE_CODE and DM.DATE_TIME_START=ni.DispensedDate \
LEFT JOIN COLL CO  ON DS.SITE_CODE=CO.STORE_CODE and DM.DATE_TIME_START=co.DispensedDate \
LEFT JOIN NOT_COLL NC ON DS.SITE_CODE=NC.STORE_CODE and DM.DATE_TIME_START=nc.DispensedDate \
LEFT JOIN TMP_PAT_INST PI ON PI.StoreCode=DS.SITE_CODE and DM.DATE_TIME_START=pi.DispensedDate \
WHERE ---DS.STOCK_VALID_FROM_DT IS NOT NULL column not present
AND DS.RECORD_START_DT<=current_date() \
AND DS.RECORD_END_DT>current_date()")