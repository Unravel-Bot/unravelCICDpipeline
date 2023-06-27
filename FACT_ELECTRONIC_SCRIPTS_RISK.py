# Databricks notebook source
dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01","")
dbutils.widgets.text("Dateto","2022-06-30","")

LoadType = dbutils.widgets.get("Loadtype")
DateFrom = dbutils.widgets.get("Datefrom")
DateTo = dbutils.widgets.get("Dateto")

# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

dimCalendarDF = synapse_sql("select FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,DATE_KEY,DATE_TIME_START,DAY_OF_WEEK_SDESC from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

DIM_PHARMACY_SITE_DF = synapse_sql("select site_code,SCD_ACTIVE_FLAG,PRICING_REGION_NUMBER,SK_STORE from con_columbus.DIM_PHARMACY_SITE")
DIM_PHARMACY_SITE_DF.createOrReplaceTempView('DIM_PHARMACY_SITE')

# COMMAND ----------

if(LoadType=="I"):
  WEEK_TO_PROCESS_DF = spark.sql("SELECT \
  CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
  CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
  FROM dimCalendar \
  WHERE cast(DATE_TIME_START as date)=Current_date()")
  WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")

# COMMAND ----------

if(LoadType=="I"):
  DATE_RANGE_OF_WEEK_DF=spark.sql("select \
  min(DATE_TIME_START ) as v_start,\
  max(DATE_TIME_START ) as v_end \
  FROM dimCalendar \
  where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
  and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
  DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")

# COMMAND ----------

if(LoadType=="I"):
    DatetimeFilter1="cast(EPS.ReceivedTime as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
    DatetimeFilter2="cast(EPS.UpdateTime as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
    DatetimeFilter3 = "cast(EPS.PRODUCEDDATE as date) between (current_date()-300) and (select v_end from DATE_RANGE_OF_WEEK)"
    DatetimeFilter4 = "cast(EPS.CLAIMSENDDATETIME as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
    
print(DatetimeFilter1)
print(DatetimeFilter2)
print(DatetimeFilter3)
print(DatetimeFilter4)

# COMMAND ----------

TMP_ELECTRONIC_SCRIPTS_DF = spark.sql("select \
STORECODE \
,EPS.SourceKey \
,eps.ELectronicPrescriptionStatus \
,eps.ClaimStatus \
,epi.NHSSTATUS \
from columbus_curation.curateadls_ElectronicPrescription eps \
inner join columbus_curation.curateadls_ELECTRONICPRESCRIBEDITEM Epi on epi.ELECTRONICPRESCRIPTIONID= EPS.ElectronicPrescriptionSKID \
where \
"+str(DatetimeFilter1)+" \
and eps.TYPE='EPS' \
and length(EPS.SourceElectronicPrescriptionID)=20")

TMP_ELECTRONIC_SCRIPTS_DF.createOrReplaceTempView('TMP_ELECTRONIC_SCRIPTS')

# COMMAND ----------

TMP_ELECTRONIC_SCRIPTS_EX_DF = spark.sql("SELECT \
STORECODE, \
EPS.SourceKey, \
CASE WHEN NVL(pp.CONTROLLEDDRUGCODE,'0') IN ('1','2','11','0') \
THEN \
CASE WHEN NONCDEXPIRYDATE IS NULL \
THEN CASE WHEN EPS.PRODUCEDDATE IS NULL THEN cast(EPS.RECEIVEDTIME as date) +180 ELSE cast(EPS.PRODUCEDDATE as date)+180 END \
ELSE NONCDEXPIRYDATE +1 END \
ELSE \
CASE WHEN CDEXPIRYDATE IS NULL \
THEN CASE WHEN EPS.PRODUCEDDATE IS NULL THEN cast(EPS.RECEIVEDTIME as date) +28 ELSE cast(EPS.PRODUCEDDATE as date)+28 END \
ELSE CDEXPIRYDATE+1 END END EXPIRY_DATE, \
CASE WHEN NVL(pp.CONTROLLEDDRUGCODE,'0') IN ('1','2','11','0') \
THEN 0 ELSE 1 END CD_DRUG \
FROM columbus_curation.curateadls_ElectronicPrescription eps \
inner join columbus_curation.curateadls_ELECTRONICPRESCRIBEDITEM epi on epi.electronicprescriptionid = eps.ElectronicPrescriptionSKID \
inner join columbus_curation.curateadls_Pharmacyproduct pp on pp.CommonDrugServiceCode = epi.CommonDrugServiceCode and pp.PRODUCTCLASS<>'Sub Generic' \
where \
pp.ProductSTATUS='Valid' \
and eps.TYPE='EPS' \
and "+str(DatetimeFilter3)+" \
and length(EPS.SourceElectronicPrescriptionID)=20 \
and eps.ElectronicPrescriptionStatus in ('FD','PD') \
and (eps.CLAIMSTATUS IN ('To Be Claimed','To be Claimed') \
OR (eps.claimstatus in  ('Claim Error','RMX','Withdraw Error','DN Error') and BUSINESSSTATUS in ( 'CN - In Exception','DN- In Exception','Unsuccessful','DN - Timed out','CN- Timed Out')) ) ")

TMP_ELECTRONIC_SCRIPTS_EX_DF.createOrReplaceTempView('TMP_ELECTRONIC_SCRIPTS_EX')

# COMMAND ----------

TMP_ELECTRONIC_SCRIPTS_NO_CLAIM_DF = spark.sql("SELECT \
STORECODE \
,EPS.SourceKey \
,EPS.ElectronicPrescriptionSKID \
,CASE WHEN NVL(pp.CONTROLLEDDRUGCODE,'0') IN ('1','2','11','0') \
THEN \
CASE WHEN NONCDEXPIRYDATE IS NULL \
THEN CASE WHEN EPS.PRODUCEDDATE IS NULL THEN cast(EPS.RECEIVEDTIME as date) +180 ELSE cast(EPS.PRODUCEDDATE as date)+180 END \
ELSE NONCDEXPIRYDATE +1 END \
ELSE \
CASE WHEN CDEXPIRYDATE IS NULL \
THEN CASE WHEN EPS.PRODUCEDDATE IS NULL THEN cast(EPS.RECEIVEDTIME as date) +28 ELSE cast(EPS.PRODUCEDDATE as date)+28 END \
ELSE CDEXPIRYDATE+1 END END EXPIRY_DATE, \
CASE WHEN NVL(pp.CONTROLLEDDRUGCODE,'0') IN ('1','2','11','0') \
THEN 0 ELSE 1 END CD_DRUG \
FROM columbus_curation.curateadls_electronicprescription eps \
inner join columbus_curation.curateadls_ELECTRONICPRESCRIBEDITEM epi on epi.ElectronicPrescriptionID = eps.ElectronicPrescriptionSKID \
inner join columbus_curation.curateadls_PharmacyProduct pp on pp.CommonDrugServiceCode = epi.CommonDrugServiceCode and pp.PRODUCTCLASS<>'Sub Generic' \
where \
pp.ProductStatus='Valid' \
and eps.TYPE='EPS' \
and "+str(DatetimeFilter3)+" \
and length(EPS.SourceElectronicPrescriptionID)=20 \
and eps.ElectronicPrescriptionStatus not in ('DEL','PR','EXP','PRA','IS','TBD') \
and EPS.ClaimStatus <>'Claimed'")
TMP_ELECTRONIC_SCRIPTS_NO_CLAIM_DF.createOrReplaceTempView('TMP_ELECTRONIC_SCRIPTS_NO_CLAIM')

# COMMAND ----------

TMP_ELECTRONIC_SCRIPTS_CLAIMED_DF = spark.sql("select \
STORECODE \
,EPS.SourceKey \
,EPI.NHSSTATUS \
from columbus_curation.curateadls_ElectronicPrescription eps \
inner join columbus_curation.curateadls_ElectronicPrescribedItem Epi on epi.ElectronicPrescriptionID= EPS.ElectronicPrescriptionSKID \
where \
"+str(DatetimeFilter4)+" \
and eps.TYPE='EPS' \
and eps.ElectronicPrescriptionStatus not in ('DEL','PR','EXP','PRA','IS') \
and length(EPS.SourceElectronicPrescriptionID)=20 \
and eps.claimstatus='Claimed'")
TMP_ELECTRONIC_SCRIPTS_CLAIMED_DF.createOrReplaceTempView('TMP_ELECTRONIC_SCRIPTS_CLAIMED')

# COMMAND ----------

TMP_DISP_DF = spark.sql("SELECT \
STORECODE,EPS.SourceKey,epi.NHSSTATUS \
from columbus_curation.curateadls_ElectronicPrescription EPS \
inner join columbus_curation.curateadls_ElectronicPrescribedItem Epi on epi.ElectronicPrescriptionID= EPS.ElectronicPrescriptionSKID \
where \
"+str(DatetimeFilter2)+" \
and eps.TYPE='EPS' \
and length(EPS.SourceElectronicPrescriptionID)=20 \
and eps.ElectronicPrescriptionStatus in ('FD','PD') \
and eps.RECEIVEDTIME is not null \
and NVL(epi.IsNotDispensableIndicator,0)<>1 \
and (eps.CLAIMSTATUS IN ('To Be Claimed','To be Claimed','DN Approved','Claimed','DN Sent','Claim Sent','Withdraw Sent') \
OR \
(eps.claimstatus in  ('Claim Error','RMX','Withdraw Error', 'DN Error') \
and BUSINESSSTATUS in ( 'CN - In Exception','DN- In Exception','Unsuccessful','DN - Timed out','CN- Timed Out')))")
TMP_DISP_DF.createOrReplaceTempView('TMP_DISP')

# COMMAND ----------

TMP_HND_NO_CLAIM_DF = spark.sql("SELECT BEF.* \
FROM columbus_curation.curateadls_PrescriptionForm PF \
INNER JOIN TMP_ELECTRONIC_SCRIPTS_NO_CLAIM BEF ON BEF.ElectronicPrescriptionSKID=PF.ElectronicPrescriptionID \
INNER JOIN columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=PF.PrescriptionGroupID \
INNER JOIN columbus_curation.curateadls_BAG BA ON BA.PrescriptionGroupCode=PG.SourceKey \
WHERE BA.BagSTATUS='HANDOUT'")
TMP_HND_NO_CLAIM_DF.createOrReplaceTempView('TMP_HND_NO_CLAIM')

# COMMAND ----------

TMP_HND_SCRIPTS_EX_DF = spark.sql("SELECT EX.StoreCode, EX.SourceKey, EX.EXPIRY_DATE \
FROM columbus_curation.curateadls_PrescriptionForm PF \
INNER JOIN TMP_ELECTRONIC_SCRIPTS_EX EX ON EX.SourceKey=PF.ElectronicPrescriptionID \
INNER JOIN columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=PF.PrescriptionGroupID \
INNER JOIN columbus_curation.curateadls_Bag BA ON BA.PrescriptionGroupCode=PG.SourceKey \
WHERE BA.BagStatus='HANDOUT'")
TMP_HND_SCRIPTS_EX_DF.createOrReplaceTempView('TMP_HND_SCRIPTS_EX')

# COMMAND ----------

DWN_DF = spark.sql("SELECT StoreCode,COUNT(*) as SCRIPTS \
FROM ( SELECT StoreCode,SourceKey FROM TMP_ELECTRONIC_SCRIPTS WHERE ElectronicPrescriptionStatus IN ('LCK','TBD') \
GROUP BY StoreCode,SourceKey) \
GROUP BY StoreCode ")
DWN_DF.createOrReplaceTempView('DWN')

# COMMAND ----------

DWN_I_DF = spark.sql("SELECT StoreCode,sum(item) as ITEMS \
FROM ( SELECT StoreCode,SourceKey,COUNT(*) ITEM FROM TMP_ELECTRONIC_SCRIPTS WHERE ElectronicPrescriptionStatus IN ('LCK','TBD') AND NHSStatus='PEN' \
GROUP BY StoreCode,SourceKey ) \
GROUP BY StoreCode")
DWN_I_DF.createOrReplaceTempView('DWN_I')

# COMMAND ----------

RS_DF = spark.sql("SELECT StoreCode,COUNT(*) as RS_SCRIPTS \
FROM ( \
SELECT \
StoreCode,EPS.ElectronicPrescriptionStatus EPS_STATUS \
from columbus_curation.curateadls_ElectronicPrescription eps \
where \
EPS.RepeatIssueNumber is null \
and EPS.ReceivedTime IS NOT NULL \
and length(EPS.SourceElectronicPrescriptionID)=20 \
and EPS.ElectronicPrescriptionStatus ='RS') \
GROUP BY StoreCode")
RS_DF.createOrReplaceTempView('RS')

# COMMAND ----------

DISP_DF = spark.sql("SELECT StoreCode,COUNT(*) DISP_SCRIPTS \
FROM ( \
select StoreCode,SourceKey \
FROM TMP_DISP \
GROUP BY StoreCode,SourceKey) \
GROUP BY StoreCode")
DISP_DF.createOrReplaceTempView('DISP')

# COMMAND ----------

DISP_I_DF = spark.sql("SELECT StoreCode,SUM(ITEMS) DISP_ITEMS \
FROM ( \
select StoreCode,SourceKey,COUNT(*) as ITEMS \
FROM TMP_DISP \
WHERE NHSSTATUS IN ('POW','DIS','FOW') \
GROUP BY StoreCode,SourceKey) \
GROUP BY StoreCode")
DISP_I_DF.createOrReplaceTempView('DISP_I')

# COMMAND ----------

CLAIM_DF = spark.sql("SELECT StoreCode,COUNT(*) CLAIM_SUCCESS_SCRIPTS \
FROM ( \
select StoreCode,SourceKey \
FROM TMP_ELECTRONIC_SCRIPTS_CLAIMED \
GROUP BY StoreCode,SourceKey) \
GROUP BY StoreCode")
CLAIM_DF.createOrReplaceTempView('CLAIM')

# COMMAND ----------

CLAIM_1_DF = spark.sql("SELECT StoreCode,SUM(ITEMS) CLAIM_SUCCESS_ITEMS \
FROM ( \
select StoreCode,SourceKey,COUNT(*) ITEMS \
FROM TMP_ELECTRONIC_SCRIPTS_CLAIMED \
WHERE NHSSTATUS='DIS' \
GROUP BY StoreCode,SourceKey) \
GROUP BY StoreCode ")
CLAIM_1_DF.createOrReplaceTempView('CLAIM_I')

# COMMAND ----------

TMP_ELECTRONIC_SCRIPTS_TOT_DF =spark.sql("SELECT \
st.SITE_CODE, \
ST.SK_STORE, \
NVL(D.SCRIPTS,0) as DOWNLOADED_SCRIPTS, \
NVL(DI.ITEMS,0) as DOWNLOADED_ITEMS, \
nvl(DISP_SCRIPTS,0) as DISPENSED_SCRIPTS, \
nvl(DISP_ITEMS,0) as DISPENSED_ITEMS, \
nvl(RS_SCRIPTS,0) as RETURNED_TO_SURGERY, \
nvl(CLAIM_SUCCESS_SCRIPTS,0) as CLAIM_SUCCESS_SCRIPTS, \
nvl(CLAIM_SUCCESS_ITEMS,0) as CLAIM_SUCCESS_ITEMS \
FROM \
DIM_PHARMACY_SITE st \
LEFT join dwn d on st.site_code=d.StoreCode \
left join DWN_I di on st.site_code=di.StoreCode \
left join RS R on st.site_code=R.StoreCode \
left join DISP e on st.site_code=E.StoreCode \
left join DISP_I f on st.site_code=F.StoreCode \
left join claim CL  on st.site_code=CL.StoreCode \
LEFT JOIN CLAIM_I CLI ON st.site_code=CLI.StoreCode \
where st.PRICING_REGION_NUMBER=1 \
AND st.SCD_ACTIVE_FLAG='Y'")
TMP_ELECTRONIC_SCRIPTS_TOT_DF.createOrReplaceTempView('TMP_ELECTRONIC_SCRIPTS_TOT')

# COMMAND ----------

EX_BF= spark.sql("select \
STORECODE, \
sum(case when EXPIRY_DATE \
BETWEEN (select V_START from DATE_RANGE_OF_WEEK ) AND (select V_END from DATE_RANGE_OF_WEEK ) THEN 1 ELSE 0 END) EXPIRED_BEFORE \
from ( \
select STORECODE,SourceKey,max(EXPIRY_DATE) EXPIRY_DATE \
from TMP_ELECTRONIC_SCRIPTS_NO_CLAIM \
group by STORECODE,SourceKey) GROUP BY STORECODE")
EX_BF.createOrReplaceTempView('EX_BF')

# COMMAND ----------

EX_DF = spark.sql("select \
STORECODE, \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+14 THEN 1 ELSE 0 END) EXPIRED_14DD , \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+35 THEN 1 ELSE 0 END) EXPIRED_5W , \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+70 THEN 1 ELSE 0 END) EXPIRED_10W \
from (select STORECODE,SourceKey as ID,max(EXPIRY_DATE) EXPIRY_DATE \
from TMP_ELECTRONIC_SCRIPTS_EX \
group by STORECODE,SourceKey) \
GROUP BY STORECODE")
EX_DF.createOrReplaceTempView('EX_DF')

# COMMAND ----------

EX_BF_CD_DF = spark.sql("select \
StoreCode, \
sum(case when EXPIRY_DATE  BETWEEN (select V_START from DATE_RANGE_OF_WEEK ) AND (select V_END from DATE_RANGE_OF_WEEK ) AND CD_DRUG=0 THEN 1 ELSE 0 END) EXPIRED_BEFORE_NON_CD , \
sum(case when EXPIRY_DATE  BETWEEN (select V_START from DATE_RANGE_OF_WEEK ) AND (select V_END from DATE_RANGE_OF_WEEK ) AND CD_DRUG=1 THEN 1 ELSE 0 END) EXPIRED_BEFORE_CD \
from ( \
SELECT DISTINCT A.StoreCode,A.EXPIRY_DATE,A.SourceKey,A.CD_DRUG \
FROM TMP_ELECTRONIC_SCRIPTS_NO_CLAIM A \
INNER JOIN ( select StoreCode,SourceKey,max(EXPIRY_DATE) EXPIRY_DATE \
from TMP_ELECTRONIC_SCRIPTS_NO_CLAIM \
group by StoreCode,SourceKey ) b on b.StoreCode=a.StoreCode and b.SourceKey=a.SourceKey and b.EXPIRY_DATE=a.EXPIRY_DATE) \
GROUP BY StoreCode ")
EX_BF_CD_DF.createOrReplaceTempView('EX_BF_CD')

# COMMAND ----------

HAND_EX_DF = spark.sql("select \
STORECODE, \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+14 THEN 1 ELSE 0 END) HAND_EXPIRED_14DD , \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+35 THEN 1 ELSE 0 END) HAND_EXPIRED_5W , \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+70 THEN 1 ELSE 0 END) HAND_EXPIRED_10W \
from  ( \
select STORECODE,SourceKey as ID,max(EXPIRY_DATE) EXPIRY_DATE \
from TMP_HND_SCRIPTS_EX \
group by StoreCode,SourceKey) \
GROUP BY StoreCode")
HAND_EX_DF.createOrReplaceTempView('HAND_EX')

# COMMAND ----------

HAND_EX_BF_CD_DF = spark.sql("select \
StoreCode, \
sum(case when EXPIRY_DATE  BETWEEN (select V_start from DATE_RANGE_OF_WEEK ) AND (select V_END from DATE_RANGE_OF_WEEK ) AND CD_DRUG=0 THEN 1 ELSE 0 END) HAND_EXPIRED_BEFORE_NON_CD , \
sum(case when EXPIRY_DATE  BETWEEN (select V_start from DATE_RANGE_OF_WEEK ) AND (select V_END from DATE_RANGE_OF_WEEK ) AND CD_DRUG=1 THEN 1 ELSE 0 END) HAND_EXPIRED_BEFORE_CD \
from ( \
SELECT DISTINCT A.StoreCode,A.EXPIRY_DATE,A.SourceKey As ID,A.CD_DRUG \
FROM TMP_HND_NO_CLAIM A \
INNER JOIN ( select StoreCode,Sourcekey AS ID,max(EXPIRY_DATE) EXPIRY_DATE \
from TMP_HND_NO_CLAIM \
group by StoreCode,Sourcekey ) b on b.StoreCode=a.StoreCode and b.ID=a.Sourcekey and b.EXPIRY_DATE=a.EXPIRY_DATE \
) GROUP BY StoreCode")
HAND_EX_BF_CD_DF.createOrReplaceTempView('HAND_EX_BF_CD')

# COMMAND ----------

EX_CD_DF = spark.sql("select \
STORECODE, \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+14 AND CD_DRUG=1 THEN 1 ELSE 0 END) EXPIRED_2W_CD , \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+28 AND CD_DRUG=1 THEN 1 ELSE 0 END) EXPIRED_4W_CD , \
sum(case when EXPIRY_DATE  BETWEEN (select V_END from DATE_RANGE_OF_WEEK )+1 AND (select V_END from DATE_RANGE_OF_WEEK )+28 AND CD_DRUG=0 THEN 1 ELSE 0 END) EXPIRED_4W_NO_CD \
from ( \
SELECT DISTINCT A.STORECODE,A.EXPIRY_DATE,A.SourceKey,A.CD_DRUG \
FROM TMP_ELECTRONIC_SCRIPTS_EX A \
INNER JOIN ( select STORECODE,SourceKey as ID,max(EXPIRY_DATE) EXPIRY_DATE \
from TMP_ELECTRONIC_SCRIPTS_EX \
group by STORECODE,SourceKey ) b \
on b.storecode=a.storecode and b.ID=a.SourceKey and b.EXPIRY_DATE=a.EXPIRY_DATE \
) GROUP BY STORECODE")
EX_CD_DF.createOrReplaceTempView('EX_CD')

# COMMAND ----------

OutputDF = spark.sql("SELECT cast(A.SK_STORE as int) as SK_STORE, \
cast(date_format((select V_END from DATE_RANGE_OF_WEEK ),'yyyMMdd') as int) as SK_DATE, \
cast(DOWNLOADED_SCRIPTS as int) as DOWNLOADED_SCRIPTS_COUNT, \
cast(DOWNLOADED_ITEMS as int) as DOWNLOADED_ITEMS_COUNT , \
cast(DISPENSED_SCRIPTS as int) as DISPENSED_SCRIPTS_COUNT, \
cast(DISPENSED_ITEMS as int) as DISPENSED_ITEMS_COUNT, \
cast(RETURNED_TO_SURGERY as int) as RETURNED_TO_SURGERY_COUNT, \
cast(CLAIM_SUCCESS_SCRIPTS as int) as CLAIM_SUCCESS_SCRIPTS_COUNT, \
cast(CLAIM_SUCCESS_ITEMS as int) as CLAIM_SUCCESS_ITEMS, \
cast(NVL(EXPIRED_14DD,0) as int) as EXPIRED_14DD_FLAG  , \
cast(NVL(EXPIRED_BEFORE,0) as int) as EXPIRED_BEFORE_FLAG , \
cast(NVL(EXPIRED_5W,0) as int) as EXPIRED_5W_FLAG , \
cast(NVL(EXPIRED_10W,0) as int) as EXPIRED_10W_FLAG, \
cast(NVL(EXPIRED_BEFORE_CD,0) as int) as EXPIRED_BEFORE_CD_FLAG, \
cast(NVL(EXPIRED_BEFORE_NON_CD,0) as int) as EXPIRED_BEFORE_NON_CD_FLAG, \
cast(NVL(HAND_EXPIRED_14DD,0) as int) as HAND_EXPIRED_14DD_FLAG , \
cast(NVL(HAND_EXPIRED_5W,0) as int) as  HAND_EXPIRED_5W_FLAG, \
cast(NVL(HAND_EXPIRED_10W,0) as int) as HAND_EXPIRED_10W_FLAG, \
cast(NVL(HAND_EXPIRED_BEFORE_CD,0) as int) as HAND_EXPIRED_BEFORE_CD_FLAG, \
cast(NVL(HAND_EXPIRED_BEFORE_NON_CD,0) as int) as HAND_EXPIRED_BEFORE_NON_CD_FLAG, \
cast(NVL(EXPIRED_2W_CD,0) as int) as EXPIRED_2W_CD_FLAG , \
cast(NVL(EXPIRED_4W_CD,0) as int) as EXPIRED_4W_CD_FLAG , \
cast(NVL(EXPIRED_4W_NO_CD,0) as int) as EXPIRED_4W_NO_CD_FLAG, \
current_timestamp as CREATE_DATETIME, \
current_timestamp as UPDATE_DATETIME \
from TMP_ELECTRONIC_SCRIPTS_TOT A \
LEFT JOIN EX_BF B  ON B.StoreCode=A.SITE_CODE \
LEFT JOIN  EX_DF C ON C.StoreCode=A.SITE_CODE \
LEFT JOIN  EX_BF_CD D ON D.StoreCode=A.SITE_CODE \
LEFT JOIN  HAND_EX E ON E.StoreCode=A.SITE_CODE \
LEFT JOIN  HAND_EX_BF_CD F ON F.StoreCode=A.SITE_CODE \
LEFT JOIN EX_CD G ON G.StoreCode=A.SITE_CODE \
")

# COMMAND ----------

append_to_synapse(OutputDF, 'con_columbus.FACT_ELECTRONIC_SCRIPTS_RISK')