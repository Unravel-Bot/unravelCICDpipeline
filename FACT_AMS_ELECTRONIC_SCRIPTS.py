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

max_load_date=last_loaded_date("con_columbus.FACT_AMS_ELECTRONIC_SCRIPTS")
print(max_load_date)

# COMMAND ----------

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,PRICING_REGION_number,PRICING_REGION_name,scd_active_flag,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')


# COMMAND ----------

#if(LoadType=="I"):
 # DatetimeFilter1="PF.Rundatetime > CAST(\'"+str(max_load_date)+"\' as timestamp)"
#else:
 #DatetimeFilter1="PF.PrescriptionDate between cast(\'"+str(DateFrom)+"\' as timestamp) and cast(\'"+str(DateTo)+"\' as timestamp)" 

#print(DatetimeFilter1)


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


# COMMAND ----------

# DBTITLE 1,TMP_AMS_DWN_SCRIPTS
TMP_AMS_DWN_SCRIPTS_DF=spark.sql(" SELECT \
eps.STORECODE, \
EPS.ElectronicPrescriptionSKID id  , \
eps.ElectronicPrescriptionStatus, \
epi.NHSSTATUS \
 from columbus_curation.curateadls_ELECTRONICPRESCRIPTION EPS \
inner join columbus_curation.curateadls_ELECTRONICPRESCRIBEDITEM Epi  \
on epi.ELECTRONICPRESCRIPTIONID= EPS.ElectronicPrescriptionSKID \
where \
eps.ElectronicPrescriptionStatus='TBD' \
and eps.SourceFormType='AMS' \
")
TMP_AMS_DWN_SCRIPTS_DF.createOrReplaceTempView('TMP_AMS_DWN_SCRIPTS')
##--EPS.RECEIVEDTIME>=v_start --and EPS.RECEIVEDTIME<=v_end --and \

# COMMAND ----------

# DBTITLE 1,TMP_AMS_ELECTRONIC_SCRIPTS_EX
TMP_AMS_ELECTRONIC_SCRIPTS_EX_DF=spark.sql(" SELECT \
eps.STORECODE, \
EPS.ElectronicPrescriptionSKID id , \
CASE \
WHEN NVL(pp.ControlledDrugCode,'0') IN ('1','2','11','0') \
THEN \
CASE \
WHEN NONCDEXPIRYDATE IS NULL \
THEN \
CASE \
WHEN EPS.PRODUCEDDATE IS NULL \
THEN date_add(cast(EPS.RECEIVEDTIME as date),180) \
ELSE date_add(cast(EPS.PRODUCEDDATE  as date),180)\
END  \
ELSE date_add(cast(NONCDEXPIRYDATE  as date),1)END \
ELSE \
CASE WHEN CDEXPIRYDATE IS NULL \
THEN CASE WHEN EPS.PRODUCEDDATE IS NULL  \
THEN date_add(cast(EPS.RECEIVEDTIME as date),28) \
ELSE date_add(cast(EPS.PRODUCEDDATE  as date),28)\
END \
ELSE date_add(cast(CDEXPIRYDATE as date),1) END END EXPIRY_DATE, \
EPS.CLAIMSTATUS \
FROM columbus_curation.curateadls_electronicprescription eps \
inner join columbus_curation.curateadls_ELECTRONICPRESCRIBEDITEM epi  \
on epi.ELECTRONICPRESCRIPTIONID= EPS.ElectronicPrescriptionSKID \
inner join columbus_curation.curateadls_pharmacyproduct pp \
on pp.CommonDrugServiceCode = epi.CommonDrugServiceCode \
and pp.PRODUCTCLASS<>'Sub Generic' \
where \
pp.ProductSTATUS='Valid' \
and EPS.PRODUCEDDATE>=current_date()-300 \
and eps.type='AMS' \
AND eps.ElectronicPrescriptionStatus not in ('DEL','PR','TBD','EXP','PRA','IS') \
AND (EPS.CLAIMSTATUS IN ('To Be Claimed','To be Claimed') OR (eps.claimstatus ='Claim Error') \
and eps.BUSINESSSTATUS in ( 'CN - In Exception','Unsuccessful','CN- Timed Out')) \
 ") 
TMP_AMS_ELECTRONIC_SCRIPTS_EX_DF.createOrReplaceTempView('TMP_AMS_ELECTRONIC_SCRIPTS_EX')
### --and EPS.PRODUCEDDATE<=v_end


# COMMAND ----------

# DBTITLE 1,TMP_AMS_SCRIPTS_NO_CLAIM

TMP_AMS_SCRIPTS_NO_CLAIM_DF=spark.sql(" SELECT \
EPS.STORECODE, \
EPS.ElectronicPrescriptionSKID ID, \
CASE  \
WHEN NVL(pp.CONTROLLEDDRUGCODE,'0') IN ('1','2','11','0') \
THEN \
CASE WHEN NONCDEXPIRYDATE IS NULL \
THEN \
CASE WHEN EPS.PRODUCEDDATE IS NULL THEN date_add(cast(EPS.RECEIVEDTIME as date),180) ELSE date_add(cast(EPS.PRODUCEDDATE  as date),180) END \
ELSE date_add(cast(NONCDEXPIRYDATE  as date),1) END \
ELSE \
CASE WHEN CDEXPIRYDATE IS NULL \
THEN \
CASE \
WHEN EPS.PRODUCEDDATE IS NULL \
THEN date_add(cast(EPS.RECEIVEDTIME as date),28) \
ELSE date_add(cast(EPS.PRODUCEDDATE  as date),28) \
END \
ELSE date_add(cast(CDEXPIRYDATE  as date),1) END \
END EXPIRY_DATE, \
EPS.CLAIMSTATUS, \
CASE WHEN NVL(pp.CONTROLLEDDRUGCODE,'0') IN ('1','2','11','0') \
THEN 0 ELSE 1  END CD_DRUG \
FROM columbus_curation.curateadls_electronicprescription eps \
inner join columbus_curation.curateadls_ELECTRONICPRESCRIBEDITEM epi \
on epi.ELECTRONICPRESCRIPTIONID= EPS.ElectronicPrescriptionSKID \
inner join columbus_curation.curateadls_pharmacyproduct pp  \
on pp.CommonDrugServiceCode = epi.CommonDrugServiceCode and pp.PRODUCTCLASS<>'Sub Generic' \
where \
pp.ProductSTATUS='Valid' \
and EPS.PRODUCEDDATE>=current_date()-300 \
and eps.type='AMS' \
AND eps.electronicprescriptionSTATUS not in ('DEL','PR','EXP','PRA','IS') \
AND EPS.CLAIMSTATUS <> 'Claimed' \
")
TMP_AMS_SCRIPTS_NO_CLAIM_DF.createOrReplaceTempView('TMP_AMS_SCRIPTS_NO_CLAIM')
 ##--and EPS.PRODUCEDDATE<=v_end

# COMMAND ----------

# DBTITLE 1,TMP_AMS_CLAIM
TMP_AMS_CLAIM_DF=spark.sql(" SELECT \
eps.STORECODE, \
EPS.ElectronicPrescriptionSKID  ID, \
epi.NHSSTATUS, \
EPS.CLAIMSTATUS \
from columbus_curation.curateadls_ELECTRONICPRESCRIPTION EPS \
INNER join columbus_curation.curateadls_ELECTRONICPRESCRIBEDITEM Epi  \
on epi.ELECTRONICPRESCRIPTIONid= EPS.ElectronicPrescriptionSKID  \
INNER JOIN dimPharmacySite ds on ds.SITE_CODE=EPS.STORECODE  and ds.PRICING_REGION_number=3  and DS.SCD_ACTIVE_FLAG='Y'  \
where eps.ELECTRONICPRESCRIPTIONSTATUS not in ('DEL','PR','EXP','PRA','IS') \
and eps.type='AMS' \
and eps.RECEIVEDTIME is not null \
AND (eps.claimstatus in  ('Claimed','Claim Sent') or (eps.claimstatus in  ('Claim Error')  \
and BUSINESSSTATUS in ( 'CN - In Exception','Unsuccessful','CN- Timed Out') )) \
")
TMP_AMS_CLAIM_DF.createOrReplaceTempView('TMP_AMS_CLAIM')
##--EPS.CLAIMSENDDATETIME>=V_START --and EPS.CLAIM_SEND_DATE_TIME<=V_END --AND 

# COMMAND ----------

# DBTITLE 1,TMP_AMS_DISP_SCRIPTS
TMP_AMS_DISP_SCRIPTS_DF=spark.sql("SELECT \
eps.STORECODE, \
EPS.ElectronicPrescriptionSKID  ID, \
epi.NHSSTATUS, \
EPS.CLAIMSTATUS \
from columbus_curation.curateadls_ELECTRONICPRESCRIPTION EPS \
INNER join columbus_curation.curateadls_ELECTRONICPRESCRIBEDITEM Epi \
on epi.ELECTRONICPRESCRIPTIONid= EPS.ElectronicPrescriptionSKID \
where \
eps.ELECTRONICPRESCRIPTIONstatus in ('ND','FD','PD') \
and eps.type='AMS' \
AND NVL(epi.IsNotDispensableIndicator,0)<>1  \
and eps.RECEIVEDTIME is not null \
AND (eps.claimstatus in  ('Claim Sent','Claimed Cancelled') \
or (eps.claimstatus in  ('Claim Error') \
and eps.BUSINESSSTATUS in ( 'CN - In Exception','Unsuccessful','CN- Timed Out')) ) \
 ")
TMP_AMS_DISP_SCRIPTS_DF.createOrReplaceTempView('TMP_AMS_DISP_SCRIPTS')
# --EPS.UPDATETIME>=V_START -- and EPS.UPDATE_TIME<=V_END  -- and 

# COMMAND ----------

# DBTITLE 1,TMP_HND_AMS_NO_CLAIM
#The bag table is not available curateadls for now using columbus_curation.curatestandard_BAG
#The table will be avilable by Friday
#Checked with DE curation team
TMP_HND_AMS_NO_CLAIM_DF=spark.sql(" SELECT \
BEF.STORECODE, BEF.ID, BEF.EXPIRY_DATE,BEF.CD_DRUG \
 FROM columbus_curation.curateadls_PRESCRIPTIONFORM PF \
 INNER JOIN TMP_AMS_SCRIPTS_NO_CLAIM BEF ON BEF.ID=PF.ELECTRONICPRESCRIPTIONid \
 INNER JOIN columbus_curation.curateadls_PRESCRIPTIONGROUP PG ON PG.PRESCRIPTIONgroupskID=PF.PRESCRIPTIONgroupid \
 INNER JOIN columbus_curation.curatestandard_BAG BA ON BA.PRESCRIPTION_GROUP_CODE=PG.PatientCODE \
 WHERE BA.STATUS='HANDOUT' \
 ")
TMP_HND_AMS_NO_CLAIM_DF.createOrReplaceTempView('TMP_HND_AMS_NO_CLAIM')

# COMMAND ----------

# DBTITLE 1,TMP_HND_AMS_SCRIPTS_EX
TMP_HND_AMS_SCRIPTS_EX_DF=spark.sql(" SELECT \
BEF.STORECODE, BEF.ID, BEF.EXPIRY_DATE\
 FROM columbus_curation.curateadls_PRESCRIPTIONFORM PF \
 INNER JOIN TMP_AMS_ELECTRONIC_SCRIPTS_EX BEF ON BEF.ID=PF.ELECTRONICPRESCRIPTIONid \
 INNER JOIN columbus_curation.curateadls_PRESCRIPTIONGROUP PG ON PG.PRESCRIPTIONgroupskID=PF.PRESCRIPTIONgroupid \
 INNER JOIN columbus_curation.curatestandard_BAG BA ON BA.PRESCRIPTION_GROUP_CODE=PG.PatientCODE \
 WHERE BA.STATUS='HANDOUT' \
 ")
TMP_HND_AMS_SCRIPTS_EX_DF.createOrReplaceTempView('TMP_HND_AMS_SCRIPTS_EX')

# COMMAND ----------

DWN_DF=spark.sql ("SELECT STORECODE,COUNT(*) SCRIPTS \
FROM ( SELECT STORECODE,ID FROM TMP_AMS_DWN_SCRIPTS \
GROUP BY STORECODE,ID) \
GROUP BY STORECODE \
") 
DWN_DF.createOrReplaceTempView('DWN')

DWN_I_DF=spark.sql ("SELECT  \
STORECODE,sum(item) ITEMS \
FROM ( SELECT STORECODE,id,COUNT(*) ITEM FROM TMP_AMS_DWN_SCRIPTS WHERE NHSSTATUS='PEN' \
GROUP BY STORECODE,id ) \
GROUP BY STORECODE \
") 
DWN_I_DF.createOrReplaceTempView('DWN_I')

# COMMAND ----------

 DISP_DF=spark.sql ("SELECT  \
STORECODE,COUNT(*) DISP_SCRIPTS \
FROM ( \
select STORECODE,ID \
FROM TMP_AMS_DISP_SCRIPTS \
GROUP BY STOREcODE,ID) \
GROUP BY STORECODE \
") 
DISP_DF.createOrReplaceTempView('DISP')
DISP_i_DF=spark.sql(" SELECT \
STORECODE,SUM(ITEMS) DISP_ITEMS \
FROM ( \
select STORECODE,ID,COUNT(*) ITEMS \
FROM TMP_AMS_DISP_SCRIPTS \
WHERE NHSSTATUS IN ( 'POW', 'DIS','FOW') \
GROUP BY STORECODE,ID) \
GROUP BY STORECODE \
") 
DISP_i_DF.createOrReplaceTempView('DISP_i')

# COMMAND ----------

CLAIM_DF=spark.sql ("SELECT \
sTORECODE,COUNT(*) CLAIM_SUCCESS_SCRIPTS \
FROM ( \
select STORECODE,ID \
FROM TMP_AMS_CLAIM \
GROUP BY STORECODE,ID) \
GROUP BY STORECODE \
")
CLAIM_DF.createOrReplaceTempView('CLAIM')

CLAIM_I_DF=spark.sql( "SELECT  \
STOREcODE,SUM(ITEMS) CLAIM_SUCCESS_ITEMS \
FROM ( \
select STORECODE,ID,COUNT(*) ITEMS \
FROM TMP_AMS_CLAIM \
WHERE NHSSTATUS='DIS' \
GROUP BY STORECODE,ID) \
GROUP BY STOREcODE   \
")
CLAIM_I_DF.createOrReplaceTempView('CLAIM_I')  

# COMMAND ----------

# DBTITLE 1,TMP_AMS_ELE_SCRIPTS_TOT
TMP_AMS_ELE_SCRIPTS_TOT_DF=spark.sql( "SELECT  \
st.SITE_CODE STORE_CODE, \
ST.SK_STORE, \
NVL(D.SCRIPTS,0) DOWNLOADED_SCRIPTS, \
NVL(DI.ITEMS,0) DOWNLOADED_ITEMS, \
nvl(DISP_SCRIPTS,0) DISPENSED_SCRIPTS, \
nvl(DISP_ITEMS,0) DISPENSED_ITEMS, \
nvl(CLAIM_SUCCESS_SCRIPTS,0), CLAIM_SUCCESS_SCRIPTS ,\
nvl(CLAIM_SUCCESS_ITEMS,0) CLAIM_SUCCESS_ITEMS \
FROM \
dimPharmacySite st \
LEFT join dwn d on st.site_code=d.STORECODE \
left join DWN_I di on st.site_code=di.STORECODE \
left join DISP e on st.site_code=E.STORECODE \
left join DISP_I f on st.site_code=F.STORECODE \
left join claim CL  on st.site_code=CL.STORECODE \
LEFT JOIN CLAIM_I CLI ON st.site_code=CLI.STOREcODE \
where  PRICING_REGION_number=3 \
AND st.SCD_ACTIVE_FLAG='Y'  \
")
TMP_AMS_ELE_SCRIPTS_TOT_DF.createOrReplaceTempView('TMP_AMS_ELE_SCRIPTS_TOT')  

# COMMAND ----------

# DBTITLE 1,EX_BF
EX_BF_DF=spark.sql("select \
STORECODE, \
sum(case when EXPIRY_DATE  BETWEEN (select min(cast(DATE_TIME_START as date)) from dimCalendar) AND (select max(cast(DATE_TIME_START as date)) from dimCalendar) \
    THEN 1 ELSE 0 END) EXPIRED_BEFORE \
from ( \
select STORECODE,ID,min(EXPIRY_DATE) EXPIRY_DATE \
from TMP_AMS_SCRIPTS_NO_CLAIM \
group by STORECODE,ID) GROUP BY STORECODE \
")
EX_BF_DF.createOrReplaceTempView('EX_BF') 

# COMMAND ----------

# DBTITLE 1,EX
EX_DF=spark.sql ("select \
STORECODE, \
sum( case  when EXPIRY_DATE  BETWEEN date_add((select min(cast(DATE_TIME_START as date)) from dimCalendar),1)   AND date_add((select max(cast(DATE_TIME_START as date)) from dimCalendar),14) THEN 1 ELSE 0 END  ) EXPIRED_14DD  , \
sum(case when EXPIRY_DATE  BETWEEN date_add((select min(cast(DATE_TIME_START as date)) from dimCalendar),1) AND date_add((select max(cast(DATE_TIME_START as date)) from dimCalendar),35) THEN 1 ELSE 0 END ) EXPIRED_5W ,\
sum(case when EXPIRY_DATE  BETWEEN date_add((select min(cast(DATE_TIME_START as date)) from dimCalendar),1) AND date_add((select max(cast(DATE_TIME_START as date)) from dimCalendar),70) THEN 1 ELSE 0 END  ) EXPIRED_10W \
from  ( \
select STORECODE,ID,min(EXPIRY_DATE) EXPIRY_DATE \
from TMP_AMS_ELECTRONIC_SCRIPTS_EX   \
group by STORECODE,ID ) GROUP BY STORECODE \
")
EX_DF.createOrReplaceTempView('EX')

# COMMAND ----------

# DBTITLE 1,HAND_EX
HAND_EX_DF=spark.sql ("select \
STORECODE, \
sum( case  when EXPIRY_DATE  BETWEEN date_add((select min(cast(DATE_TIME_START as date)) from dimCalendar),1)   AND date_add((select max(cast(DATE_TIME_START as date)) from dimCalendar),14) THEN 1 ELSE 0 END  ) HAND_EXPIRED_14DD  , \
sum(case when EXPIRY_DATE  BETWEEN date_add((select min(cast(DATE_TIME_START as date)) from dimCalendar),1) AND date_add((select max(cast(DATE_TIME_START as date)) from dimCalendar),35) THEN 1 ELSE 0 END )HAND_EXPIRED_5W ,\
sum(case when EXPIRY_DATE  BETWEEN date_add((select min(cast(DATE_TIME_START as date)) from dimCalendar),1) AND date_add((select max(cast(DATE_TIME_START as date)) from dimCalendar),70) THEN 1 ELSE 0 END  )HAND_EXPIRED_10W \
from  ( \
select STORECODE,ID,min(EXPIRY_DATE) EXPIRY_DATE \
from TMP_HND_AMS_SCRIPTS_EX   \
group by STORECODE,ID ) GROUP BY STORECODE \
")
HAND_EX_DF.createOrReplaceTempView('HAND_EX')



# COMMAND ----------

# DBTITLE 1,HAND_EX_BF_CD
HAND_EX_BF_CD_DF=spark.sql ("select \
STORECODE, \
sum(case when EXPIRY_DATE  BETWEEN (select min(cast(DATE_TIME_START as date)) from dimCalendar) AND (select min(cast(DATE_TIME_START as date)) from dimCalendar) AND CD_DRUG=0 THEN 1 ELSE 0 END) HAND_EXPIRED_BEFORE_NON_CD , \
sum(case when EXPIRY_DATE  BETWEEN (select min(cast(DATE_TIME_START as date)) from dimCalendar) AND (select min(cast(DATE_TIME_START as date)) from dimCalendar )AND CD_DRUG=1 THEN 1 ELSE 0 END) HAND_EXPIRED_BEFORE_CD \
from ( \
SELECT DISTINCT A.STORECODE,A.EXPIRY_DATE,A.ID,A.CD_DRUG \
FROM TMP_HND_AMS_NO_CLAIM A \
INNER JOIN ( select STORECODE,ID,min(EXPIRY_DATE) EXPIRY_DATE \
from TMP_HND_AMS_NO_CLAIM \
group by STORECODE,ID ) b on b.storecode=a.storecode and b.id=a.id and b.EXPIRY_DATE=a.EXPIRY_DATE \
 ) GROUP BY STORECODE \
    ")
HAND_EX_BF_CD_DF.createOrReplaceTempView('HAND_EX_BF_CD')

# COMMAND ----------

OutputDF=spark.sql("SELECT A.SK_STORE, \
(select max (cast(DATE_TIME_START  as int)) from dimCalendar) , \
NVL(DOWNLOADED_SCRIPTS,0) as DOWNLAODEd_SCRIPTS ,\
NVL(DOWNLOADED_ITEMS,0), \
NVL(DISPENSED_SCRIPTS,0), \
NVL(DISPENSED_ITEMS,0), \
NVL(CLAIM_SUCCESS_SCRIPTS,0), \
NVL(CLAIM_SUCCESS_ITEMS,0), \
NVL(EXPIRED_14DD,0), \
NVL(EXPIRED_BEFORE,0), \
NVL(EXPIRED_5W,0), \
NVL(EXPIRED_10W,0), \
NVL(HAND_EXPIRED_14DD,0) , \
NVL(HAND_EXPIRED_5W,0) , \
NVL(HAND_EXPIRED_10W,0), \
NVL(HAND_EXPIRED_BEFORE_CD,0), \
NVL(HAND_EXPIRED_BEFORE_NON_CD,0) \
from TMP_AMS_ELE_SCRIPTS_TOT A \
LEFT JOIN EX_BF B  ON B.STORECODE=A.STORE_CODE \
LEFT JOIN  EX C ON C.STORECODE=A.STORE_CODE \
LEFT JOIN  HAND_EX E ON E.STORECODE=A.STORE_CODE \
LEFT JOIN  HAND_EX_BF_CD F ON F.STORECODE=A.STORE_CODE \
")


# COMMAND ----------

#append_to_synapse(OutputDF,'con_columbus.FACT_AMS_ELECTRONIC_SCRIPTS')