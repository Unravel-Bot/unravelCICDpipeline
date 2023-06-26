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

dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,PRICING_REGION_number,scd_active_flag,RECORD_START_DATE,RECORD_END_DATE from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

# COMMAND ----------

if(LoadType=='I'):
    WEEK_TO_PROCESS_DF=spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=Current_date()")
    WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")
    display(WEEK_TO_PROCESS_DF)

# COMMAND ----------

if(LoadType=='I'):
    DATE_RANGE_OF_WEEK_DF=spark.sql("select \
min(date_Add(DATE_TIME_START,1)) as v_start,\
max(date_Add(DATE_TIME_START,1)) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
    DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")
    display(DATE_RANGE_OF_WEEK_DF)


# COMMAND ----------

if(LoadType=="I"):
  DatetimeFilter1="PF.PrescriptionDate >v_start and PF.PrescriptionDate<=v_end"
else:
 DatetimeFilter1="PF.PrescriptionDate between cast(\'"+str(DateFrom)+"\' as timestamp) and cast(\'"+str(DateTo)+"\' as timestamp)" 

print(DatetimeFilter1)


# COMMAND ----------

TMP_ADD_ENDORSEMENT_df=spark.sql("SELECT DS.SK_STORE,\
DATE_FORMAT(PF.PrescriptionDate,'yyyMMdd') as SK_PRESCRIPTION_DATE,\
CASE WHEN PF.IsElectronicIndicator=1 THEN UniquePrescriptionNumber ELSE PF.SourceKey END PRESCRIPTION_ID,\
AD.AdditionalDescription, \
case when AD.AdditionalEndorsementSKID=N.NoCheaperStockObtainableSKID THEN 'YES'  END NCSO, \
CASE  WHEN AD.AdditionalEndorsementSKID = S.SpecialItemSKID THEN  'YES'  END  SPECIAL, \
CASE  WHEN AD.IsPackagedDoseIndicator      =1 THEN        'YES'  END  PACKAGED_DOSE ,\
CASE  WHEN AD.AdditionalEndorsementSKID = O.OutOfPocketExpensesSKID THEN 'YES'  END OOPE, \
CASE  WHEN AD.IsRebatClaimedIndicator=1 THEN 'YES'  END REBAT_CLAIMED, \
CASE  WHEN AD.IsInvoicePriceIndicator=1 THEN 'YES'  END INVOICE_PRICE, \
CASE  WHEN AD.IsPrescriberPCFlagIndicator=1 THEN 'YES'  END PRESCRIBER_CONTACTED, \
CASE  WHEN AD.IsContraceptiveItemIndicator=1 THEN 'YES'  END CONTRACEPTIVE_ITEM, \
CASE  WHEN AD.IsMeasuredFittedIndicator=1 THEN 'YES'  END MEASURED_FITTED, \
CASE  WHEN AD.IsLevyChargedIndicator=1 THEN 'YES'  END LEVY_CHARGED, \
CASE  WHEN AD.IsBrokenBulkIndicator=1 THEN 'YES'  END BROKEN_BULK, \
CASE  WHEN AD.IsExtempIndicator=1 THEN 'YES'  END EXTEMP, \
CASE  WHEN AD.AdditionalEndorsementSKID=A.AdditionalItemSKID THEN 'YES'  END ADDITIONAL_ITEM,\
CASE  WHEN AD.IsUrgentDispensingIndicator=1 THEN 'YES'  END URGENT_DISPENSING, \
CASE  WHEN AD.IsLimitedLifeIndicator=1 THEN 'YES'  END LIMITED_LIFE, \
CASE  WHEN AD.IsMadeToMeasureIndicator=1 THEN 'YES'  END MADE_TO_MEASURE,\
CASE  WHEN AD.IsInstalmentDispenseIndicator=1 THEN 'YES'  END INSTALMENT_DISPENSE,\
CASE  WHEN AD.IsOtherEndorsementIndicator  =1 THEN 'YES'  END  OTHER_ENDORSEMENT,\
CASE WHEN O.IsOutOfPocketExpensesPostagePackagingIndicator =1 THEN 'YES'  END OOPE_POSTAGE_PACKAGING,\
CASE WHEN O.IsOutOfPocketExpensesHandlingChargeIndicator=1 THEN 'YES'  END OOPE_HANDLING_CHARGE,\
CASE WHEN O.IsOutOfPocketExpensesOtherChargeIndicator=1 THEN 'YES'  END OOPE_OTHER_CHARGE,\
CASE WHEN AD.IsNotCollectedIndicator=1 THEN 'YES'  END NOT_COLLECTED, \
case when AD.IsShortSupplyIndicator=1 then 'YES'  END as SHORT_SUPPLY,\
CASE WHEN AD.IsBrokenBulkSurplusIndicator     =1 THEN 'YES'  END BROKEN_BULK_SURPLUS, \
CASE WHEN AD.IsExtraContainerIndicator        =1 THEN       'YES'  END EXTRA_CONTAINER       ,\
CASE WHEN AD.IsExtraItemIndicator             =1 THEN       'YES'  END EXTRA_ITEM            ,\
CASE WHEN AD.BrandOrManufacturer              =1 THEN       'YES'  END BRAND_OR_MANUFACTURER ,\
CASE WHEN AD.IsRepeatDispensedIndicator       =1 THEN       'YES'  END REPEAT_DISPENSED      ,\
CASE WHEN AD.IsRepeatInterventionIndicator    =1 THEN       'YES'  END REPEAT_INTERVENTION   ,\
CASE WHEN AD.IsUrgentSupplyIndicator          =1 THEN       'YES'  END URGENT_SUPPLY         ,\
CASE WHEN AD.IsWasteReductionIndicator        =1 THEN       'YES'  END WASTE_REDUCTION \
FROM columbus_curation.curateadls_PrescriptionGroup PG \
INNER JOIN columbus_curation.curateadls_PrescriptionForm PF ON PF.PrescriptionGroupID=PG.SourceSystemID \
INNER JOIN columbus_curation.curateadls_PRESCRIBEDITEM PI ON PI.PrescriptionFormID=PF.SourceSystemID \
INNER JOIN columbus_curation.curateadls_ADDITIONALENDORSEMENT AD ON AD.PrescribedItemID=PI.SourceSystemID \
INNER JOIN dimPharmacySite DS ON DS.SITE_CODE=PG.STORECODE \
LEFT JOIN columbus_curation.curateadls_AdditionalItem A on AD.AdditionalEndorsementSKID = A.AdditionalEndorsementID \
LEFT JOIN columbus_curation.curateadls_outofpocketexpenses O on AD.AdditionalEndorsementSKID=O.AdditionalEndorsementID \
LEFT JOIN columbus_curation.curateadls_nocheaperstockobtainable N on AD.AdditionalEndorsementSKID=N.AdditionalEndorsementID \
LEFT JOIN columbus_curation.curateadls_specialitem S on AD.AdditionalEndorsementSKID=S.AdditionalEndorsementID \
inner join DATE_RANGE_OF_WEEK \
WHERE \
UPPER(PG.PrescriptionGroupStatus) <>'DELETED' \
AND UPPER(PF.PrescriptionFormStatus) <>'DELETED' \
and "+str(DatetimeFilter1)+" \
AND (PF.PrescriptionDate>= DS.RECORD_START_DATE \
AND PF.PrescriptionDate < DS.RECORD_END_DATE) \
")
TMP_ADD_ENDORSEMENT_df.createOrReplaceTempView("TMP_ADD_ENDORSEMENT")

#Need confirmation on DISP_ADDITIONAL_ENDORSEMENT table in line 95. Currently used Additionalendorsement table.

# COMMAND ----------

# DBTITLE 1,Commented for now-Reporting needs to provide confirmation if it is required
TMP_ADD_ENDORSEMENTDF2=spark.sql("SELECT \
DS.SK_STORE,\
DATE_FORMAT(PF.PrescriptionDate,'yyyMMdd') as SK_PRESCRIPTION_DATE,\
CASE WHEN PF.IsElectronicIndicator=1 THEN UniquePrescriptionNumber ELSE PF.SourceKey END PRESCRIPTION_ID,\
'' as AdditionalDescription,\
'' as   NCSO,\
CASE  WHEN AD.AdditionalEndorsementSKID = S.SpecialItemSKID THEN  'YES'  END  SPECIAL, \
'' as   PACKAGED_DOSE,\
'' as   OOPE,\
'' as   REBAT_CLAIMED,\
'' as   INVOICE_PRICE,\
'' as   PRESCRIBER_CONTACTED,\
'' as   CONTRACEPTIVE_ITEM,\
'' as   MEASURED_FITTED,\
'' as   LEVY_CHARGED,\
CASE  WHEN AD.IsBrokenBulkIndicator=1 THEN 'YES'  END BROKEN_BULK, \
'' as   EXTEMP,\
'' as   ADDITIONAL_ITEM,\
'' as   URGENT_DISPENSING,\
'' as   LIMITED_LIFE,\
'' as   MADE_TO_MEASURE,\
'' as   INSTALMENT_DISPENSE,\
'' as   OTHER_ENDORSEMENT,\
CASE WHEN O.IsOutOfPocketExpensesPostagePackagingIndicator =1 THEN 'YES' END OOPE_POSTAGE_PACKAGING,\
CASE WHEN O.IsOutOfPocketExpensesHandlingChargeIndicator=1 THEN 'YES' END OOPE_HANDLING_CHARGE,\
CASE WHEN O.IsOutOfPocketExpensesOtherChargeIndicator=1 THEN 'YES'  END OOPE_OTHER_CHARGE,\
CASE WHEN AD.IsNotCollectedIndicator=1 THEN 'YES'  END NOT_COLLECTED, \
'' as   SHORT_SUPPLY,\
'' as   BROKEN_BULK_SURPLUS,\
'' as   EXTRA_CONTAINER,\
'' as   EXTRA_ITEM,\
'' as   BRAND_OR_MANUFACTURER,\
'' as   REPEAT_DISPENSED,\
'' as   REPEAT_INTERVENTION,\
'' as   URGENT_SUPPLY,\
'' as   WASTE_REDUCTION \
FROM  columbus_curation.curateadls_PrescriptionGroup PG \
INNER JOIN columbus_curation.curateadls_PrescriptionForm PF ON PF.PrescriptionGroupID=PG.SourceSystemID \
INNER JOIN columbus_curation.curateadls_PRESCRIBEDITEM PI ON PI.PrescriptionFormID=PF.SourceSystemID \
INNER JOIN columbus_curation.curateadls_dispenseditem DI ON DI.PrescribedItemID=PI.SourceSystemID \
INNER JOIN columbus_curation.curateadls_ADDITIONALENDORSEMENT AD ON AD.PrescribedItemID=DI.SourceSystemID \
INNER JOIN dimPharmacySite DS ON DS.SITE_CODE=PG.STORECODE \
LEFT JOIN columbus_curation.curateadls_outofpocketexpenses O on AD.AdditionalEndorsementSKID=O.AdditionalEndorsementID \
LEFT JOIN columbus_curation.curateadls_SpecialItem S on AD.AdditionalEndorsementSKID=S.AdditionalEndorsementID \
inner join DATE_RANGE_OF_WEEK \
WHERE \
UPPER(PG.PrescriptionGroupStatus) <>'DELETED' \
AND UPPER(PF.PrescriptionFormStatus) <>'DELETED' \
and "+str(DatetimeFilter1)+" \
AND (PF.PrescriptionDate>= DS.RECORD_START_DATE \
AND PF.PrescriptionDate < DS.RECORD_END_DATE) \
AND DS.PRICING_REGION_NUMBER=3 ")
TMP_ADD_ENDORSEMENTDF2.createOrReplaceTempView('TMP_ADD_ENDORSEMENTDF2')

# COMMAND ----------

TMP_ADD_ENDORSEMENT=spark.sql("select SK_STORE, \
cast(SK_PRESCRIPTION_DATE as INT), \
PRESCRIPTION_ID, \
AdditionalDescription, \
NCSO, \
SPECIAL, \
PACKAGED_DOSE, \
OOPE, \
REBAT_CLAIMED, \
INVOICE_PRICE, \
PRESCRIBER_CONTACTED,\
CONTRACEPTIVE_ITEM, \
MEASURED_FITTED, \
LEVY_CHARGED, \
BROKEN_BULK, \
EXTEMP, \
ADDITIONAL_ITEM, \
URGENT_DISPENSING, \
LIMITED_LIFE, \
MADE_TO_MEASURE, \
INSTALMENT_DISPENSE,\
OTHER_ENDORSEMENT, \
OOPE_POSTAGE_PACKAGING, \
OOPE_HANDLING_CHARGE, \
OOPE_OTHER_CHARGE, \
NOT_COLLECTED, \
SHORT_SUPPLY, \
BROKEN_BULK_SURPLUS, \
EXTRA_CONTAINER, \
EXTRA_ITEM, \
BRAND_OR_MANUFACTURER, \
REPEAT_DISPENSED, \
REPEAT_INTERVENTION, \
URGENT_SUPPLY, \
WASTE_REDUCTION \
from TMP_ADD_ENDORSEMENT union all \
select \
SK_STORE, \
cast(SK_PRESCRIPTION_DATE as INT), \
PRESCRIPTION_ID, \
AdditionalDescription, \
NCSO, \
SPECIAL, \
PACKAGED_DOSE, \
OOPE, \
REBAT_CLAIMED, \
INVOICE_PRICE, \
PRESCRIBER_CONTACTED,\
CONTRACEPTIVE_ITEM, \
MEASURED_FITTED, \
LEVY_CHARGED, \
BROKEN_BULK, \
EXTEMP, \
ADDITIONAL_ITEM, \
URGENT_DISPENSING, \
LIMITED_LIFE, \
MADE_TO_MEASURE, \
INSTALMENT_DISPENSE,\
OTHER_ENDORSEMENT, \
OOPE_POSTAGE_PACKAGING, \
OOPE_HANDLING_CHARGE, \
OOPE_OTHER_CHARGE, \
NOT_COLLECTED, \
SHORT_SUPPLY, \
BROKEN_BULK_SURPLUS, \
EXTRA_CONTAINER, \
EXTRA_ITEM, \
BRAND_OR_MANUFACTURER, \
REPEAT_DISPENSED, \
REPEAT_INTERVENTION, \
URGENT_SUPPLY, \
WASTE_REDUCTION \
from TMP_ADD_ENDORSEMENTDF2 ")
TMP_ADD_ENDORSEMENT.createOrReplaceTempView('TMP_ADD_ENDORSEMENT')

# COMMAND ----------

FACT_ADDITIONAL_END_DF=spark.sql("select *,Current_timestamp() as CREATE_DATETIME,cast(Null as timestamp) as UPDATE_DATETIME from (SELECT \
SK_STORE,SK_PRESCRIPTION_DATE,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,PRESCRIPTION_ID,\
max(AdditionalDescription) as ADDITIONAL_DESCRIPTION ,\
max(ADDITIONAL_ITEM) as ADDITIONAL_ITEM_FLAG ,\
max(INVOICE_PRICE) as INVOICE_PRICE_FLAG,\
max(BRAND_OR_MANUFACTURER) as BRAND_OR_MANUFACTURER_FLAG ,\
max(BROKEN_BULK) as BROKEN_BULK_FLAG ,\
max(BROKEN_BULK_SURPLUS) as BROKEN_BULK_SURPLUS_FLAG ,\
max(CONTRACEPTIVE_ITEM) as CONTRACEPTIVE_ITEM_FLAG ,\
max(EXTEMP) as EXTEMP_FLAG ,\
max(EXTRA_CONTAINER) as EXTRA_CONTAINER_FLAG ,\
max(EXTRA_ITEM) as EXTRA_ITEM_FLAG,\
max(INSTALMENT_DISPENSE) as INSTALMENT_DISPENSE_FLAG,\
max(LEVY_CHARGED) as LEVY_CHARGED_FLAG,\
max(LIMITED_LIFE) as LIMITED_LIFE_FLAG,\
max(MADE_TO_MEASURE) as MADE_TO_MEASURE_FLAG,\
max(MEASURED_FITTED) as MEASURED_FITTED_FLAG ,\
max(NCSO) as NO_CHEAPER_STOCK_OBTANAIBLE_FLAG ,\
max(NOT_COLLECTED) as NOT_COLLECTED_FLAG ,\
max(OOPE) as OUT_OF_POCKET_EXPENSES_FLAG,\
max(OOPE_HANDLING_CHARGE) as OOPE_HANDLING_CHARGE_FLAG ,\
max(OOPE_OTHER_CHARGE) as OOPE_OTHER_CHARGE_FLAG,\
max(OOPE_POSTAGE_PACKAGING) as OOPE_POSTAGE_PACKAGING_FLAG ,\
max(OTHER_ENDORSEMENT) as OTHER_ENDORSEMENT_FLAG ,\
max(PACKAGED_DOSE) as PACKAGED_DOSE_FLAG ,\
max(PRESCRIBER_CONTACTED) as PRESCRIBER_CONTACTED_FLAG,\
max(REBAT_CLAIMED) as REBAT_CLAIMED_FLAG ,\
max(REPEAT_DISPENSED) as REPEAT_DISPENSED_FLAG ,\
max(REPEAT_INTERVENTION) as REPEAT_INTERVENTION_FLAG ,\
max(SHORT_SUPPLY) as SHORT_SUPPLY_FLAG ,\
max(SPECIAL) as SPECIAL_FLAG,\
max(URGENT_DISPENSING) as URGENT_DISPENSING_FLAG,\
max(URGENT_SUPPLY) as URGENT_SUPPLY_FLAG,\
max(WASTE_REDUCTION) as WASTE_REDUCTION_FLAG FROM TMP_ADD_ENDORSEMENT inner join Dimcalendar on SK_PRESCRIPTION_DATE=DATE_KEY \
group by SK_STORE,SK_PRESCRIPTION_DATE,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,PRESCRIPTION_ID)a")
    

# COMMAND ----------

FACT_ADDITIONAL_END_DF=spark.sql("select *,Current_timestamp() as CREATE_DATETIME,cast(null as timestamp) as UPDATE_DATETIME from (SELECT \
SK_STORE, \
SK_PRESCRIPTION_DATE, \
PRESCRIPTION_ID, \
max(AdditionalDescription) as ADDITIONAL_DESCRIPTION ,\
max(ADDITIONAL_ITEM) as ADDITIONAL_ITEM_FLAG ,\
max(INVOICE_PRICE) as INVOICE_PRICE_FLAG,\
max(BRAND_OR_MANUFACTURER) as BRAND_OR_MANUFACTURER_FLAG ,\
max(BROKEN_BULK) as BROKEN_BULK_FLAG ,\
max(BROKEN_BULK_SURPLUS) as BROKEN_BULK_SURPLUS_FLAG ,\
max(CONTRACEPTIVE_ITEM) as CONTRACEPTIVE_ITEM_FLAG ,\
max(EXTEMP) as EXTEMP_FLAG ,\
max(EXTRA_CONTAINER) as EXTRA_CONTAINER_FLAG ,\
max(EXTRA_ITEM) as EXTRA_ITEM_FLAG,\
max(INSTALMENT_DISPENSE) as INSTALMENT_DISPENSE_FLAG,\
max(LEVY_CHARGED) as LEVY_CHARGED_FLAG,\
max(LIMITED_LIFE) as LIMITED_LIFE_FLAG,\
max(MADE_TO_MEASURE) as MADE_TO_MEASURE_FLAG,\
max(MEASURED_FITTED) as MEASURED_FITTED_FLAG ,\
max(NCSO) as NO_CHEAPER_STOCK_OBTANAIBLE_FLAG ,\
max(NOT_COLLECTED) as NOT_COLLECTED_FLAG ,\
max(OOPE) as OUT_OF_POCKET_EXPENSES_FLAG,\
max(OOPE_HANDLING_CHARGE) as OOPE_HANDLING_CHARGE_FLAG ,\
max(OOPE_OTHER_CHARGE) as OOPE_OTHER_CHARGE_FLAG,\
max(OOPE_POSTAGE_PACKAGING) as OOPE_POSTAGE_PACKAGING_FLAG ,\
max(OTHER_ENDORSEMENT) as OTHER_ENDORSEMENT_FLAG ,\
max(PACKAGED_DOSE) as PACKAGED_DOSE_FLAG ,\
max(PRESCRIBER_CONTACTED) as PRESCRIBER_CONTACTED_FLAG,\
max(REBAT_CLAIMED) as REBAT_CLAIMED_FLAG ,\
max(REPEAT_DISPENSED) as REPEAT_DISPENSED_FLAG ,\
max(REPEAT_INTERVENTION) as REPEAT_INTERVENTION_FLAG ,\
max(SHORT_SUPPLY) as SHORT_SUPPLY_FLAG ,\
max(SPECIAL) as SPECIAL_FLAG,\
max(URGENT_DISPENSING) as URGENT_DISPENSING_FLAG,\
max(URGENT_SUPPLY) as URGENT_SUPPLY_FLAG,\
max(WASTE_REDUCTION) as WASTE_REDUCTION_FLAG FROM TMP_ADD_ENDORSEMENT inner join WEEK_TO_PROCESS on 1=1 group by SK_STORE,SK_PRESCRIPTION_DATE,V_FIN_WEEK,V_FIN_YEAR,PRESCRIPTION_ID)a")

# COMMAND ----------

append_to_synapse(FACT_ADDITIONAL_END_DF,'con_columbus.FACT_ADDITIONAL_ENDORSEMENT')