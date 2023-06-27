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

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,PRICING_REGION_number,PRICING_REGION_name,scd_active_flag,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

DIM_PRESCRIBABLE_PRODUCT_DF = synapse_sql("select PRESCRIBABLE_PRODUCT_CODE,RECORD_START_DATE,RECORD_END_DATE,SK_PRESCRIBABLE_PRODUCT from con_columbus.DIM_PRESCRIBABLE_PRODUCT")
DIM_PRESCRIBABLE_PRODUCT_DF.createOrReplaceTempView('DIM_PRESCRIBABLE_PRODUCT')

DIM_DISPENSING_PACK_SIZE_DF = synapse_sql("select SK_DISPENSING_PACK_SIZE,RECORD_START_DATE,RECORD_END_DATE,DISPENSING_PACK_SIZE_code from con_columbus.DIM_DISPENSING_PACK_SIZE")
DIM_DISPENSING_PACK_SIZE_DF.createOrReplaceTempView('DIM_DISPENSING_PACK_SIZE')



# COMMAND ----------

if(LoadType=="I"):
    WEEK_TO_PROCESS_DF=spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=Current_date()")
    WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")


# COMMAND ----------

if(LoadType=="I"):
    DATE_RANGE_OF_WEEK_DF=spark.sql("select \
min(cast(DATE_TIME_START as date)) as v_start,\
max(cast(DATE_TIME_START as date)) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
    DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")


# COMMAND ----------

if(LoadType=="I"):
      DatetimeFilter1="and cast(DI.DISPENSEDDATE as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
else:
     DatetimeFilter1="and cast(DI.DISPENSEDDATE as date) between cast(\'"+str(DateFrom)+"\' as date) and cast(\'"+str(DateTo)+"\' as date)"
print(DatetimeFilter1)

# COMMAND ----------

TMP_FCT_DDS_PRESCRIPTION_DF=spark.sql("SELECT  \
PG.PATIENTCODE \
,CASE WHEN PF.IsElectronicIndicator=1 THEN UniquePrescriptionNumber ELSE PF.SourceKey END PRESCRIPTION_IDENTIFIER \
,PG.STORECODE  STORE_CODE\
,PI.DOSAGEDIRECTIONS DOSAGE_DIRECTIONS \
,PP.PRODUCTCODE PRODUCT_CODE \
,DP.ProductSKUCode \
,PI.QUANTITY PRESCRIBED_QTY   \
,DI.QUANTITY DISPENSED_QTY  \
,DI.DISPENSEDDATE DISPENSED_DATE \
,DP.DispensedProductName \
FROM columbus_curation.curateadls_PRESCRIPTIONGROUP PG \
INNER JOIN columbus_curation.curateadls_PRESCRIPTIONFORM PF ON PF.PrescriptionGroupID = PG.PrescriptionGroupSKID \
INNER JOIN columbus_curation.curateadls_PRESCRIBEDITEM PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN columbus_curation.curateadls_PRESCRIBEDPRODUCT PP ON PP.PRESCRIBEDITEMid = PI.PrescribedItemSKID \
INNER JOIN columbus_curation.curateadls_DISPENSEDITEM DI ON DI.PRESCRIBEDITEMID = PI.PrescribedItemSKID \
INNER JOIN columbus_curation.curateadls_DISPENSEDPRODUCT DP ON DP.DISPENSEDITEMid=DI.DispensedItemSKID \
INNER JOIN columbus_curation.curateadls_PATIENT PA ON PA.SourceKey=PG.PatientCode \
WHERE PG.SERVICETYPE='DDS' \
AND DI.DISPENSEDQUANTITY>0 \
AND UPPER(PG.PRESCRIPTIONgROUPSTATUS) <> 'DELETED' \
AND UPPER(PF.PRESCRIPTIONFORMSTATUS) <> 'DELETED' \
AND PG.IsCentralPharmacyIndicator<>1 \
AND PA.patientSTATUS='Active' \
"+str(DatetimeFilter1)+" \
")
TMP_FCT_DDS_PRESCRIPTION_DF.createOrReplaceTempView("TMP_FCT_DDS_PRESCRIPTION")


# COMMAND ----------

OutputDF=spark.sql("SELECT \
cast(DS.SK_STORE as int) as SK_STORE , \
cast(date_format(DDS.DISPENSED_DATE,'yyyyMMdd') as int) as SK_DISPENSED_DATE , \
DDS.PATIENTCODE as PATIENT_CODE, \
dds.PRESCRIPTION_IDENTIFIER, \
cast(DPP.SK_PRESCRIBABLE_PRODUCT as int) as SK_PRESCRIBABLE_PRODUCT , \
cast(DDS.PRESCRIBED_QTY as decimal(15,5)) as PRESCRIBED_QUANTITY, \
cast(DPS.SK_DISPENSING_PACK_SIZE as int) as SK_DISPENSING_PACK_SIZE , \
cast(DDS.DISPENSED_QTY as decimal(15,5)) as DISPENSED_QUANTITY, \
DDS.DOSAGE_DIRECTIONS \
,DDS.DISPENSEDPRODUCTNAME AS DISPENSED_PRODUCT_NAME \
,current_timestamp as CREATE_DATETIME \
,cast(Null as timestamp) as UPDATE_DATETIME \
FROM TMP_FCT_DDS_PRESCRIPTION DDS \
INNER JOIN DIM_PRESCRIBABLE_PRODUCT dpp on  dpp.PRESCRIBABLE_PRODUCT_CODE=DDS.product_code \
INNER JOIN DIM_DISPENSING_PACK_SIZE dps on dps.DISPENSING_PACK_SIZE_code=DDS.PRODUCTSKUCODE \
INNER JOIN dimPharmacySite DS ON DS.SITE_CODE=DDS.STORE_CODE \
WHERE ( DDS.DISPENSED_DATE>= DS.RECORD_START_DATE AND DDS.DISPENSED_DATE< DS.RECORD_END_DATE)  \
AND( DDS.DISPENSED_DATE>= DPP.RECORD_START_DATE AND DDS.DISPENSED_DATE< DPP.RECORD_END_DATE)  \
AND ( DDS.DISPENSED_DATE>= DPS.RECORD_START_DATE   AND DDS.DISPENSED_DATE< DPS.RECORD_END_DATE )")
OutputDF.createOrReplaceTempView("Output")

# COMMAND ----------

append_to_synapse(OutputDF,'con_columbus.FACT_DDS_PRESCRIPTION')