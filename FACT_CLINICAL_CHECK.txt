# Databricks notebook source
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

dimCalendarDF = synapse_sql("select FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,DATE_KEY,DATE_TIME_START,DAY_OF_WEEK_SDESC from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

DIM_PHARMACY_SITEDF = synapse_sql("select SITE_CODE,SCD_ACTIVE_FLAG,SK_STORE from con_columbus.DIM_PHARMACY_SITE")
DIM_PHARMACY_SITEDF.createOrReplaceTempView('DIM_PHARMACY_SITE')

# COMMAND ----------

if(LoadType=="I"):
    WEEK_TO_PROCESS_DF = spark.sql("SELECT \
    CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
    CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
    FROM dimCalendar \
    WHERE cast(DATE_TIME_START as date)=Current_date()")
    WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")
    display(WEEK_TO_PROCESS_DF)

# COMMAND ----------

if(LoadType=="I"):
    DATE_RANGE_OF_WEEK_DF=spark.sql("select \
    min(cast(DATE_TIME_START as date)+1) as v_start,\
    max(cast(DATE_TIME_START as date)+1) as v_end \
    FROM dimCalendar \
    where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
    and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
    DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")
    display(DATE_RANGE_OF_WEEK_DF)

# COMMAND ----------

if(LoadType=="I"):
    DatetimeFilter1="cast(CC.CreationTime as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
    DatetimeFilter2="cast(UpdateTime as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
    DatetimeFilter3 = "DATE_TIME_START"
else:
    DatetimeFilter1="cast(CC.CreationTime as date) between cast('"+str(DateFrom)+"' as date) and cast('"+str(DateTo)+"' as date)"
    DatetimeFilter2="cast(UpdateTime as date) between cast('"+str(DateFrom)+"' as date) and cast('"+str(DateTo)+"' as date)"
    DatetimeFilter3 = "'"+str(DateFrom)+"'"
    
print(DatetimeFilter1)
print(DatetimeFilter2)

# COMMAND ----------

st_DF = spark.sql("SELECT distinct PS.SourceKey \
FROM columbus_curation.curateadls_ClinicalCheck CC \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId \
where "+str(DatetimeFilter1)+" \
union \
SELECT distinct PS.SourceKey \
FROM columbus_curation.curateadls_ClinicalCheckDuration CCD \
inner join columbus_curation.curateadls_ClinicalCheck CC on CCD.ClinicalCheckId = CC.ClinicalCheckSKID \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId \
WHERE DeactivationReason='Manual User Deactivation' \
AND "+str(DatetimeFilter2)+"  \
GROUP BY PS.SourceKey \
")
st_DF.createOrReplaceTempView('st')



# COMMAND ----------

if(LoadType=="I"):
    cal_df = spark.sql("select DATE_TIME_START from dimCalendar \
  WHERE FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS) \
  AND FINANCIAL_YEAR=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
  group by DATE_TIME_START")
    cal_df.createOrReplaceTempView('cal')

# COMMAND ----------

if(LoadType=="I"):
    TMP_MATRIX_df = spark.sql("select SourceKey as STORE_CODE , DATE_TIME_START as CREATION_DATE from st,cal ")
    TMP_MATRIX_df.createOrReplaceTempView('TMP_MATRIX')
else:
    TMP_MATRIX_df = spark.sql("select SourceKey as STORE_CODE,DATE_TIME_START as CREATION_DATE  from st,dimcalendar where DATE_TIME_START between cast('"+str(DateFrom)+"' as date) and cast('"+str(DateTo)+"' as date )")
    TMP_MATRIX_df.createOrReplaceTempView('TMP_MATRIX')

# COMMAND ----------

CC_AUT_DF = spark.sql("SELECT  PS.SourceKey,cast(cc.CreationTime as date) as CREATION_DATE ,COUNT(DISTINCT PG.SourceKey) as TOT_AUT \
FROM columbus_curation.curateadls_ClinicalCheck CC \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId \
inner join columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=CC.PrescriptionGroupId \
where "+str(DatetimeFilter1)+"  \
AND cc.CheckMode='Out Dispensing' \
AND cc.ClinicalCheckStatus ='Automatically Approved' \
GROUP BY PS.SourceKey,cast(cc.CreationTime as date)")
CC_AUT_DF.createOrReplaceTempView('CC_AUT')


# COMMAND ----------

CC_NO_END_DF = spark.sql("SELECT PS.SourceKey,cast(cc.CreationTime as date) as CREATION_DATE ,COUNT(DISTINCT PG.SourceKey) as TOT_NO_END \
FROM columbus_curation.curateadls_ClinicalCheck CC \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId \
inner join columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=CC.PrescriptionGroupId \
WHERE "+str(DatetimeFilter1)+" \
AND cc.CheckMode='Out Dispensing' \
AND cast(CC.CheckDate as date) > cast(cc.CreationTime as date) \
GROUP BY PS.SourceKey,cast(cc.CreationTime as date)")
CC_NO_END_DF.createOrReplaceTempView('CC_NO_END')


# COMMAND ----------

CC_REMOVED_DF = spark.sql("SELECT PS.SourceKey,cast(cc.CreationTime as date) as CREATION_DATE ,COUNT(DISTINCT PG.SourceKey) as TOT_REMOVED \
FROM columbus_curation.curateadls_ClinicalCheck CC \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId \
inner join columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=CC.PrescriptionGroupId \
WHERE "+str(DatetimeFilter1)+" \
AND cc.CheckMode='Out Dispensing' \
AND cc.ClinicalCheckStatus ='Deleted' \
AND cc.UserName IS NOT NULL \
GROUP BY PS.SourceKey,cast(cc.CreationTime as date)")
CC_REMOVED_DF.createOrReplaceTempView('CC_REMOVED')


# COMMAND ----------

CC_DISP_DF = spark.sql("SELECT PS.SourceKey,cast(cc.CreationTime as date) as CREATION_DATE ,COUNT(DISTINCT PG.SourceKey) as TOT_DISP \
FROM columbus_curation.curateadls_ClinicalCheck CC \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId \
inner join columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=CC.PrescriptionGroupId \
WHERE "+str(DatetimeFilter1)+" \
AND cc.CheckMode='Out Dispensing' \
AND cc.ClinicalCheckStatus ='Deleted' \
AND cc.UserName IS NULL \
GROUP BY PS.SourceKey,cast(cc.CreationTime as date)")
CC_DISP_DF.createOrReplaceTempView('CC_DISP')


# COMMAND ----------

CC_N_DAY_DF = spark.sql("SELECT PS.SourceKey,cast(cc.CreationTime as date) CREATION_DATE ,COUNT(DISTINCT PG.SourceKey) TOT_N_DAY \
FROM columbus_curation.curateadls_ClinicalCheck CC \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId \
inner join columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=CC.PrescriptionGroupId \
inner join columbus_curation.curateadls_ClinicalCheckDuration CCD on CCD.ClinicalCheckId = CC.ClinicalCheckSKID \
WHERE "+str(DatetimeFilter1)+" \
AND cc.CheckMode='Out Dispensing' \
AND cc.ClinicalCheckStatus ='Approved' \
GROUP BY PS.SourceKey,cast(cc.CreationTime as date)")
CC_N_DAY_DF.createOrReplaceTempView('CC_N_DAY') 


# COMMAND ----------

CC_APPROVED_DF = spark.sql("SELECT PS.SourceKey,cast(cc.CreationTime as date) as CREATION_DATE ,COUNT(DISTINCT PG.SourceKey) as TOT_APPROVED \
FROM columbus_curation.curateadls_ClinicalCheck CC \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId \
inner join columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=CC.PrescriptionGroupId \
WHERE "+str(DatetimeFilter1)+" \
AND cc.CheckMode='Out Dispensing' \
AND cc.ClinicalCheckStatus ='Approved' \
GROUP BY PS.SourceKey,cast(cc.CreationTime as date)")
CC_APPROVED_DF.createOrReplaceTempView('CC_APPROVED') 


# COMMAND ----------

CC_PMR_DF = spark.sql("SELECT PS.SourceKey,cast(CCD.UpdateTime as date) as UPDATE_DATE,COUNT(*) TOT_PMR \
FROM columbus_curation.curateadls_ClinicalCheckDuration CCD \
inner join columbus_curation.curateadls_ClinicalCheck CC on CCD.ClinicalCheckId = CC.ClinicalCheckSKID \
inner join columbus_curation.curateadls_PharmacyStore PS on CC.PharmacyStoreSiteRoleId = PS.PharmacyStoreSiteRoleId  \
WHERE DeactivationReason='Manual User Deactivation' \
AND "+str(DatetimeFilter2)+" \
GROUP BY PS.SourceKey,cast(CCD.UpdateTime as date)")
                     
CC_PMR_DF.createOrReplaceTempView('CC_PMR') 


# COMMAND ----------

OutputDF = spark.sql("SELECT cast(SK_STORE as int) as SK_STORE \
,cast(CAL.DATE_KEY as int) as SK_CREATION_DATE, \
cast((NVL(TOT_REMOVED,0)+NVL(TOT_DISP,0)+NVL(TOT_N_DAY,0)+(NVL(TOT_APPROVED,0)-NVL(TOT_N_DAY,0))+NVL(TOT_AUT,0)) as int) \
TOTAL_ELIGIBLE , \
cast((NVL(TOT_REMOVED,0)+NVL(TOT_DISP,0)+NVL(TOT_N_DAY,0)+(NVL(TOT_APPROVED,0)-NVL(TOT_N_DAY,0))) as int) \
TOTAL_ADD_QUEUE , \
cast(NVL(TOT_AUT,0) as int )  TOTAL_AUT \
,cast(NVL(TOT_NO_END,0) as int ) TOTAL_NO_END \
,cast(NVL(TOT_REMOVED,0) as int ) TOTAL_REMOVED \
,cast(NVL(TOT_DISP,0)  as int) TOTAL_DISP \
,cast(NVL(TOT_N_DAY,0) as int) TOTAL_N_DAY_APPROVAL \
,cast(NVL(TOT_APPROVED,0) - NVL(TOT_N_DAY,0)  as int )  intTOTAL_SINGLE_APPROVAL \
,cast(NVL(TOT_PMR,0) as int) TOTAL_PMR \
,current_timestamp() as CREATE_DATETIME \
,cast(null as timestamp) as UPDATE_DATETIME \
FROM TMP_MATRIX TM INNER JOIN \
DIM_PHARMACY_SITE DS ON DS.SITE_CODE=TM.STORE_CODE \
AND DS.SCD_ACTIVE_FLAG='Y' \
INNER JOIN dimCalendar CAL on TM.CREATION_DATE = CAL.DATE_TIME_START \
LEFT JOIN CC_AUT ON CC_AUT.SourceKey=DS.SITE_CODE AND TM.CREATION_DATE=CC_AUT.CREATION_DATE \
LEFT JOIN CC_NO_END ON CC_NO_END.SourceKey=DS.SITE_CODE AND TM.CREATION_DATE=CC_NO_END.CREATION_DATE \
LEFT JOIN CC_REMOVED ON CC_REMOVED.SourceKey=DS.SITE_CODE AND TM.CREATION_DATE=CC_REMOVED.CREATION_DATE \
LEFT JOIN CC_DISP ON CC_DISP.SourceKey=DS.SITE_CODE AND TM.CREATION_DATE=CC_DISP.CREATION_DATE \
LEFT JOIN CC_N_DAY ON CC_N_DAY.SourceKey=DS.SITE_CODE AND TM.CREATION_DATE=CC_N_DAY.CREATION_DATE \
LEFT JOIN CC_APPROVED ON CC_APPROVED.SourceKey=DS.SITE_CODE AND TM.CREATION_DATE=CC_APPROVED.CREATION_DATE \
LEFT JOIN CC_PMR ON CC_PMR.SourceKey=DS.SITE_CODE AND TM.CREATION_DATE=CC_PMR.UPDATE_DATE where (NVL(TOT_REMOVED,0)+NVL(TOT_DISP,0)+NVL(TOT_N_DAY,0)+(NVL(TOT_APPROVED,0)-NVL(TOT_N_DAY,0))+NVL(TOT_AUT,0)>0 or NVL(TOT_PMR,0)>0) ")

# COMMAND ----------

append_to_synapse(OutputDF,'con_columbus.FACT_CLINICAL_CHECK')