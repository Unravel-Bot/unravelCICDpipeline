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

max_load_date=last_loaded_date("con_columbus.FCT_DISPENSED_NO_BARCODE")
print(max_load_date)

# COMMAND ----------

dimPharmacySiteDF = synapse_sql("select SITE_CODE,SK_STORE,RECORD_START_DATE,RECORD_END_DATE,SCD_ACTIVE_FLAG,company_code from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_KEY,DATE_TIME_START,DAY_OF_WEEK_SDESC,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,MONTH_VALUE,YEAR_VALUE from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

#DimActualProductPackDF = synapse_sql("select * from con_columbus.DIM_ACTUAL_PRODUCT_PACK")
#DimActualProductPackDF.createOrReplaceTempView('DimActualProductPack')

DimPrescribableProductDF = synapse_sql("select * from con_columbus.DIM_PRESCRIBABLE_PRODUCT ")
DimPrescribableProductDF.createOrReplaceTempView('DimPrescribableProduct')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC DispensedDate SK_DISPENSED_DATE 
# MAGIC ,CAL.FINANCIAL_WEEK_NUMBER 
# MAGIC ,CAL.MONTH_VALUE MONTH 
# MAGIC ,CAL.YEAR_VALUE YEAR 
# MAGIC ,CAL.FINANCIAL_YEAR 
# MAGIC ,CASE WHEN pf.IsElectronicIndicator=1 then UniquePrescriptionNumber else PF.SourceKey end ORDER_CODE 
# MAGIC ,ST.SITE_CODE SK_STORE 
# MAGIC ,DECODE(SK_PRESCRIBABLE_PRODUCT,NULL,'UNPRD',SK_PRESCRIBABLE_PRODUCT) SK_PRESCRIBABLE_PRODUCT 
# MAGIC ,DECODE(SK_ACTUAL_PRODUCT_PACK,NULL,'UNPRD',SK_ACTUAL_PRODUCT_PACK) SK_ACTUAL_PRODUCT_PACK 
# MAGIC ,PP.PrescribedProductType 
# MAGIC FROM columbus_curation.curateadls_prescriptiongroup  PG 
# MAGIC INNER JOIN columbus_curation.curateadls_prescriptionform PF ON PF.PrescriptionGroupID=PG.PrescriptionGroupSKID 
# MAGIC INNER JOIN columbus_curation.curateadls_prescribeditem PI ON PF.PrescriptionFormSKID=PI.PrescriptionFormID 
# MAGIC INNER JOIN columbus_curation.curateadls_prescribedproduct PP ON PP.PrescribedItemID=PI.PrescribedItemSKID 
# MAGIC INNER JOIN columbus_curation.curateadls_dispenseditem DI ON PI.PrescribedItemSKID=DI.PrescribedItemID 
# MAGIC INNER JOIN columbus_curation.curateadls_dispensedproduct DP ON DP.DispensedItemID = DI.DispensedItemSKID 
# MAGIC INNER JOIN dimPharmacySite ST ON ST.SITE_CODE=PG.StoreCode 
# MAGIC INNER JOIN dimCalendar CAL ON CAL.DATE_TIME_START=DI.DispensedDate
# MAGIC LEFT JOIN  DimActualProductPack AP ON AP.ACTUAL_PRODUCT_PACK_CODE=DP.ActualProductPackCode and AP.SCD_Active_Flag='Y'
# MAGIC LEFT JOIN  DimPrescribableProduct DPP ON DPP.PRESCRIBABLE_PRODUCT_CODE=PP.ProductCode and dpp.SCD_Active_Flag='Y' 
# MAGIC WHERE 
# MAGIC ST.SCD_Active_Flag='Y' 
# MAGIC and pf.ClaimStatus<>'Deleted' --Check
# MAGIC and PI.STATUS ='Completed' 
# MAGIC and PG.IsCentralPharmacyIndicator<>1 
# MAGIC AND PG.SourceGroupID is null 
# MAGIC AND NVL(PI.IsNotDispensedIndicator,0)=0 
# MAGIC AND NOT exists (select * from columbus_curation.curateadls_instalment INS where INS.PrescribedItemID = pi.PrescribedItemSKID) 
# MAGIC AND DI.ReasonManualConfirmation='No Barcode Available' 
# MAGIC --and FINANCIAL_WEEK_NUMBER=V_FIN_WEEK 
# MAGIC --AND FINANCIAL_YEAR=V_FINANCIAL_YEAR;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC TMP.SK_DISPENSED_DATE
# MAGIC ,TMP.FINANCIAL_WEEK_NUMBER
# MAGIC ,TMP.MONTH
# MAGIC ,TMP.YEAR
# MAGIC ,TMP.FINANCIAL_YEAR
# MAGIC ,TMP.ORDER_CODE
# MAGIC ,TMP.SK_STORE
# MAGIC ,TMP.SK_PRESCRIBABLE_PRODUCT
# MAGIC ,TMP.SK_ACTUAL_PRODUCT_PACK
# MAGIC ,CASE WHEN NVL(AP2.IsSpecialItemIndicator,0)=0 THEN 'NO' ELSE 'YES' END SPECIAL
# MAGIC ,CASE WHEN AP2.SourceKey IS NULL THEN 'NO' ELSE 'YES' END ISPARALLELIMPORT
# MAGIC ,CASE WHEN PrescribedProductType='PRD' THEN 'NO' ELSE 'YES' END UNCATALOGUE 
# MAGIC ,case when ActualProductPackID is null then 'NO' ELSE 'YES'END BARCODES_AVAILABLE
# MAGIC FROM TMP_DISPENSED_NO_BARCODE TMP
# MAGIC LEFT JOIN DimActualProductPack AP ON AP.SK_ACTUAL_PRODUCT_PACK=DECODE(TMP.SK_ACTUAL_PRODUCT_PACK,'UNPRD',0,TMP.SK_ACTUAL_PRODUCT_PACK)
# MAGIC LEFT JOIN DimPrescribableProduct DPP ON DPP.SK_PRESCRIBABLE_PRODUCT=DECODE(TMP.SK_PRESCRIBABLE_PRODUCT,'UNPRD',0,TMP.SK_PRESCRIBABLE_PRODUCT) 
# MAGIC LEFT JOIN columbus_curation.curateadls_actualproductpack AP2 ON AP2.SourceKey=AP.ACTUAL_PRODUCT_PACK_CODE --not created
# MAGIC left JOIN columbus_curation.curateadls_product PP ON PP.SourceKey=DPP.PRESCRIBABLE_PRODUCT_CODE--CODE
# MAGIC LEFT JOIN (SELECT SourceKey FROM columbus_curation.curateadls_actualproductpack WHERE PackName LIKE '%(PI)%') IM ON IM.SourceKey=AP2.SourceKey
# MAGIC --LEFT JOIN (SELECT DISTINCT ActualProductPackID FROM PharmacyProductBarCode) BAR ON BAR.ActualProductPackID=AP2.ActualProductPackSKID

# COMMAND ----------

FCT_DNB_DF.count()

# COMMAND ----------

append_to_synapse (FCT_DNB_DF,'con_columbus.FCT_DISPENSED_NO_BARCODE')