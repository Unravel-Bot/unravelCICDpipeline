# Databricks notebook source
##### COBA-118329
##### USAIN
##### SPRINT 2

# COMMAND ----------

dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01 00:00:00","")
dbutils.widgets.text("Dateto","2022-06-30 23:59:59","")

# COMMAND ----------

LoadType = dbutils.widgets.get("Loadtype")

DateFrom = dbutils.widgets.get("Datefrom")

DateTo = dbutils.widgets.get("Dateto")
#print(LoadType)
#print(DateFrom)
#print(DateTo)

# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,PRICING_REGION_number,PRICING_REGION_name,scd_active_flag,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

DIM_PRESCRIBABLE_PRODUCT_DF = synapse_sql("select PRESCRIBABLE_PRODUCT_CODE,RECORD_START_DATE,RECORD_END_DATE,SK_PRESCRIBABLE_PRODUCT from con_columbus.DIM_PRESCRIBABLE_PRODUCT")
DIM_PRESCRIBABLE_PRODUCT_DF.createOrReplaceTempView('DIM_PRESCRIBABLE_PRODUCT')

DIM_DISPENSING_PACK_SIZE_DF = synapse_sql("select SK_DISPENSING_PACK_SIZE,RECORD_START_DATE,RECORD_END_DATE,DISPENSING_PACK_SIZE_code from con_columbus.DIM_DISPENSING_PACK_SIZE")
DIM_DISPENSING_PACK_SIZE_DF.createOrReplaceTempView('DIM_DISPENSING_PACK_SIZE')



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

if(LoadType=="I"):
      DatetimeFilter1="cast(di.DISPENSEDDATE as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
else:
     DatetimeFilter1="cast(di.DISPENSEDDATE as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)"


# COMMAND ----------

TMP_DISP_EMERGENCY_DF=spark.sql("select di.DispensedItemSKID as id_dispensed_item,\
di.PrescribedItemID,\
EmergencyDispensedItemID,\
ReconcilingDispensedItemID,\
di.DISPENSEDDATE \
from columbus_curation.curateadls_DISPENSEDITEM DI \
left join columbus_curation.curateadls_DISPENSEDRECONCILED ric on ric.EmergencyDispensedItemID=di.DispensedItemSKID \
where "+str(DatetimeFilter1)+" ")
TMP_DISP_EMERGENCY_DF.createOrReplaceTempView("TMP_DISP_EMERGENCY")

# COMMAND ----------

OutputDF=spark.sql("SELECT \
cast(DS.SK_STORE as int) as SK_STORE, \
cast(date_format(di.dispenseddate,'yyyyMMdd') as int) as SK_DISPENSED_DATE, \
cast(dpp.SK_PRESCRIBABLE_PRODUCT as int) as SK_PRESCRIBABLE_PRODUCT, \
cast(dps.SK_DISPENSING_PACK_SIZE as int) as SK_DISPENSING_PACK_SIZE, \
CASE WHEN DS.PRICING_REGION_number=1 AND (PA.SOURCEKEY IS NULL OR pa.Optedinindicator='F') THEN 'NA' else \
CASE WHEN pf.IsElectronicIndicator=1 then UniquePrescriptionNumber else PF.SourceKey end END as PRESCRIPTION_IDENTIFIER, \
pf.REASON as EMERGENCY_REASON,\
case when DI.EmergencyDispensedItemID is null then 'NO' else 'YES' end as EMERGENCY_SUPPLY_RECONCILIATION_FLAG, \
di.dispenseddate as DISPENSED_DATE,di_rec.dispenseddate RECONCILIATION_DATE \
,current_timestamp as CREATE_DATETIME,cast(Null as timestamp) as UPDATE_DATETIME \
from columbus_curation.curateadls_PRESCRIPTIONGROUP PG \
INNER JOIN columbus_curation.curateadls_PRESCRIPTIONFORM PF ON PF.PrescriptionGroupID = PG.PrescriptionGroupSKID \
inner join columbus_curation.curateadls_PRESCRIPTIONFORMTYPE FT ON FT.PrescriptionFormTypeSKID=PF.PrescriptionFormTypeID \
INNER JOIN columbus_curation.curateadls_PRESCRIBEDITEM PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
inner join columbus_curation.curateadls_prescribedproduct pp on PP.PRESCRIBEDITEMid = PI.PrescribedItemSKID \
inner join dimPharmacySite ds on ds.site_code=pg.storecode \
INNER JOIN TMP_DISP_EMERGENCY DI ON PI.PrescribedItemSKID=DI.PrescribedItemID \
inner join columbus_curation.curateadls_dispensedproduct dpi on dpi.DispensedItemID=di.id_dispensed_item \
INNER JOIN DIM_PRESCRIBABLE_PRODUCT dpp on  dpp.PRESCRIBABLE_PRODUCT_CODE=pp.Productcode \
INNER JOIN DIM_DISPENSING_PACK_SIZE dps on dps.DISPENSING_PACK_SIZE_code=dpi.PRODUCTSKUCODE \
left join columbus_curation.curateadls_DISPENSEDITEM di_rec on di_rec.DispensedItemSKID=DI.ReconcilingDispensedItemID \
left join columbus_curation.curateadls_PatientOptedInOut pa on pa.sourcekey=PG.PATIENTCODE AND pa.SCDactiveflag='Y' \
where \
ft.SourceKey in (8,9,10) \
AND pg.PRESCRIPTIONgROUPSTATUS<>'Drafted' \
and UPPER(pg.PRESCRIPTIONgROUPSTATUS) <>'DELETED' \
and UPPER(pf.PrescriptionFormStatus) <>'DELETED' and (cast(Di.DISPENSEDDATE as date)>= DS.RECORD_START_DATE AND cast(Di.DISPENSEDDATE as date)< DS.RECORD_END_DATE) \
and (cast(Di.DISPENSEDDATE as date)>= DPP.RECORD_START_DATE AND cast(Di.DISPENSEDDATE as date)< DPP.RECORD_END_DATE) \
and (cast(Di.DISPENSEDDATE as date)>= DPS.RECORD_START_DATE AND cast(Di.DISPENSEDDATE as date)< DPS.RECORD_END_DATE)")

# COMMAND ----------

append_to_synapse(OutputDF,'con_columbus.FACT_EMERGENCY_SUPPLY_RECONCILIATION')