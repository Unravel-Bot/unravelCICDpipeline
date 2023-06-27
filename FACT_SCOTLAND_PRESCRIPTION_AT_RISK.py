# Databricks notebook source
import datetime
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

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,scd_active_flag,pricing_region_number from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dim_Pharmacy_Site')

dimCalendarDF = synapse_sql("select DATE_KEY,DATE_TIME_START,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')


# COMMAND ----------

Fis_Values_DF=spark.sql("select min(DATE_TIME_START) as V_SUN_DATE,V_FIN_WEEK,V_FIN_YEAR,FINANCIAL_YEAR,FINANCIAL_WEEK_NUMBER \
from DIMCALENDAR \
join (SELECT CASE \
WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM DIMCALENDAR WHERE DATE_TIME_START=current_Date()-7)  ELSE FINANCIAL_WEEK_NUMBER-1 END AS V_FIN_WEEK, \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_YEAR FROM DIMCALENDAR WHERE DATE_TIME_START=current_Date()-7) ELSE FINANCIAL_YEAR END AS V_FIN_YEAR \
FROM DIMCALENDAR \
WHERE DATE_TIME_START=current_date()) a \
where FINANCIAL_YEAR=V_FIN_YEAR \
and FINANCIAL_WEEK_NUMBER=V_FIN_WEEK group by \
V_FIN_WEEK,V_FIN_YEAR,FINANCIAL_YEAR,FINANCIAL_WEEK_NUMBER")
Fis_Values_DF.createOrReplaceTempView("Fis_Values")
display(Fis_Values_DF)

# COMMAND ----------

#columbus_curation.curateadls_PharmacyService is not present currently 
TMP_MAS_DF=spark.sql("SELECT DS.SITE_CODE,\
PS.StoreCode,\
COUNT(*) SCRIPTS \
FROM columbus_curation.curateadls_PharmacyService PS \
INNER JOIN DIM_PHARMACY_SITE DS \
Inner join FIs_Values \
ON DS.SITE_CODE=PS.StoreCode \
AND DS.PRICING_REGION_NUMBER=3 \
AND DS.scd_active_flag='Y' \
WHERE PS.ServiceType='MAS' \
AND PS.PharmacyServiceStatus NOT IN ('Claimed','DEL') \
AND cast(PS.CreationTime as date) <=V_SUN_DATE-10 \
GROUP BY \
DS.SITE_CODE,\
PS.StoreCode")
TMP_MAS_DF.createOrReplaceTempView("TMP_MAS")
display(TMP_MAS_DF)

# COMMAND ----------

TMP_UCF_CONSDF=spark.sql("SELECT DS.SK_STORE,PS.StoreCode,COUNT(*) SCRIPTS \
FROM columbus_curation.curateadls_PharmacyService PS \
INNER JOIN DIM_PHARMACY_SITE DS ON DS.SITE_CODE=PS.StoreCode AND DS.PRICING_REGION_number=3 AND DS.scd_active_flag='Y' \
Inner join FIs_Values \
WHERE PS.ServiceType='UCF' \
AND PS.PharmacyServiceStatus NOT IN ('Claimed','DEL') \
AND cast(PS.CreationTime as date) <=cast(V_SUN_DATE as date)-10 \
GROUP BY DS.SK_STORE,PS.StoreCode \
")
TMP_UCF_CONSDF.createOrReplaceTempView("TMP_UCF_CONS")

# COMMAND ----------

TMP_UCF_DF=spark.sql("SELECT \
DS.SK_STORE,EPS.StoreCode,COUNT(*) SCRIPTS \
FROM columbus_curation.curateadls_ElectronicPrescription EPS \
INNER JOIN DIM_PHARMACY_SITE DS ON DS.SITE_CODE=EPS.StoreCode AND DS.PRICING_REGION_number=3 AND DS.scd_active_flag='Y' \
Inner join FIs_Values \
WHERE \
EPS.TYPE='UCF' AND \
EPS.ElectronicPrescriptionStatus NOT IN ('DEL','PR','PRA','IS') \
AND EPS.ClaimStatus NOT IN ('Claimed','DEL') \
AND cast(EPS.ProducedDate as Date) <=cast(V_SUN_DATE as date)-10 \
GROUP BY DS.SK_STORE,EPS.StoreCode \
")
TMP_UCF_DF.createOrReplaceTempView("TMP_UCF")
display(TMP_UCF_DF)

# COMMAND ----------

TMP_MCR_DF=spark.sql("SELECT DS.SK_STORE,PG.StoreCode,COUNT(DISTINCT DE.PrescriptionFormCode) SCRIPTS \
FROM columbus_curation.curateadls_DispensingEvent DE \
INNER JOIN columbus_curation.curateadls_PrescriptionForm PF ON PF.SourceKey=DE.PrescriptionFormCode \
INNER JOIN columbus_curation.curateadls_PrescriptionGroup PG ON PG.PrescriptionGroupSKID=PF.PrescriptionGroupID \
INNER JOIN columbus_curation.curateadls_PrescribedItem PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN DIM_PHARMACY_SITE DS ON DS.SITE_CODE=PG.StoreCode AND DS.PRICING_REGION_number=3 AND DS.scd_active_flag='Y' \
inner join Fis_Values \
WHERE DE.Status NOT IN ('Claimed','Deleted') AND cast(DE.DispensingDate as date)<=cast(V_SUN_DATE as date)-35 \
AND PF.PrescriptionFormTypeID IN (Select PrescriptionFormTypeSKID from columbus_curation.curateadls_PrescriptionFormType where SourceSystemID in(12438,12439) ) \
AND PI.PrescribedItemSKID NOT IN ( \
SELECT DISTINCT OW.PrescribedItemID \
FROM columbus_curation.curateadls_Owing OW \
INNER JOIN DIM_PHARMACY_SITE DS ON DS.SITE_CODE=OW.StoreCode AND DS.PRICING_REGION_number=3 AND DS.scd_Active_flag='Y' \
WHERE OW.OwingStatus='Outstanding') \
GROUP BY DS.SK_STORE,PG.StoreCode ")
TMP_MCR_DF.createOrReplaceTempView("TMP_MCR")
display(TMP_MCR_DF)

# COMMAND ----------

TMP_AMS_DF=spark.sql("SELECT DS.SK_STORE,EPS.StoreCode,COUNT(DISTINCT EPS.SourceKey) SCRIPTS \
FROM columbus_Curation.curateadls_ElectronicPrescription EPS \
INNER JOIN DIM_PHARMACY_SITE DS ON DS.SITE_CODE=EPS.StoreCode AND DS.PRICING_REGION_number=3 AND DS.scd_active_flag='Y' \
INNER JOIN columbus_Curation.curateadls_PrescriptionForm PF ON PF.ElectronicPrescriptionID=EPS.ElectronicPrescriptionSKID \
INNER JOIN columbus_Curation.curateadls_PrescribedItem PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
inner join Fis_Values \
WHERE \
cast(EPS.AtRiskDate as date)<=cast(V_SUN_DATE as date)-35 \
AND EPS.ElectronicPrescriptionStatus NOT IN ('DEL','PR','PRA','IS') \
AND EPS.Type='AMS' \
AND EPS.ClaimStatus <>'Claimed' \
AND PI.PrescribedItemSKID NOT IN ( \
SELECT DISTINCT INS.PrescribedItemID \
FROM columbus_Curation.curateadls_Instalment INS \
INNER JOIN columbus_Curation.curateadls_InstalmentItem II ON II.InstalmentID=INS.InstalmentSKID \
INNER JOIN DIM_PHARMACY_SITE DS ON DS.SITE_CODE=II.StoreCode AND DS.PRICING_REGION_number=3 AND DS.scd_active_flag='Y' \
WHERE II.InstalmentStatus='Pending' \
UNION \
SELECT OW.PrescribedItemID \
FROM columbus_Curation.curateadls_Owing OW \
INNER JOIN DIM_PHARMACY_SITE DS ON DS.SITE_CODE=OW.StoreCode AND DS.PRICING_REGION_number=3 AND DS.scd_active_flag='Y' \
WHERE OW.OwingStatus='Outstanding') \
GROUP BY DS.SK_STORE,EPS.StoreCode")
TMP_AMS_DF.createOrReplaceTempView("TMP_AMS")
display(TMP_AMS_DF)

# COMMAND ----------

FCT_SCOTLAND_PRESCRIPTION_AT_RISK=spark.sql("SELECT SK_STORE, \
date_Format(cast(V_SUN_DATE as date),'yyyyMMdd'), \
V_FIN_WEEK, \
V_FIN_YEAR, \
MAS, \
UCF+UCFC as UCF, \
MCR, \
AMS, \
SUM(MAS+UCF+MCR+AMS+UCFC) as TOTAL \
FROM ( \
SELECT DS.SK_STORE, \
SUM(CASE WHEN NVL(MA.SCRIPTS,0)>0 THEN MA.SCRIPTS ELSE 0 END ) MAS, \
SUM(CASE WHEN NVL(UF.SCRIPTS,0)>0 THEN UF.SCRIPTS ELSE 0 END) UCF, \
SUM(CASE WHEN NVL(UFC.SCRIPTS,0)>0 THEN UFC.SCRIPTS ELSE 0 END) UCFC, \
(CASE WHEN NVL(MC.SCRIPTS,0)>0 THEN MC.SCRIPTS ELSE 0 END) MCR,\
SUM(CASE WHEN NVL(AM.SCRIPTS,0)>0 THEN AM.SCRIPTS ELSE 0 END) AMS \
FROM DIM_PHARMACY_SITE DS \
LEFT JOIN TMP_MAS MA ON MA.StoreCode=DS.SITE_CODE \
LEFT JOIN TMP_UCF UF ON UF.StoreCode=DS.SITE_CODE \
LEFT JOIN TMP_UCF_CONS UFC ON UFC.StoreCode=DS.SITE_CODE \
LEFT JOIN TMP_MCR MC ON MC.StoreCode=DS.SITE_CODE \
LEFT JOIN TMP_AMS AM ON AM.StoreCode=DS.SITE_CODE \
inner join Fis_Values \
WHERE DS.PRICING_REGION_number=3 AND DS.scd_active_flag='Y' \
GROUP BY DS.SK_STORE) \
GROUP BY SK_STORE,MAS,UCF,UCFC,MCR,AMS")

# COMMAND ----------

dwhStagingTable =None
synapse_output_table = 'con_columbus.FACT_SCOTLAND_PRESCRIPTION_AT_RISK'
key_columns = None
deltaName =None
dwhStagingDistributionColumn =None
#stgTableOptions =None

# COMMAND ----------

append_to_synapse (OutputDF,'con_columbus.FACT_SCOTLAND_PRESCRIPTION_AT_RISK')