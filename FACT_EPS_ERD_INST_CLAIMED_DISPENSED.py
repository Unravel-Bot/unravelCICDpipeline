# Databricks notebook source
dbutils.widgets.text("Loadtype","I","")
dbutils.widgets.text("Datefrom","2019-01-01","")
dbutils.widgets.text("Dateto","2022-06-30","")

LoadType = dbutils.widgets.get("Loadtype")
DateFrom = dbutils.widgets.get("Datefrom")
DateTo = dbutils.widgets.get("Dateto")

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


dimPharmacySiteDF = synapse_sql("select site_code,SK_STORE,PRICING_REGION_number,scd_active_flag,RUN_DATE_TIME from con_columbus.dim_pharmacy_site")
dimPharmacySiteDF.createOrReplaceTempView('dimPharmacySite')

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

# COMMAND ----------

max_load_date=last_loaded_date("con_columbus.FACT_EPS_ERD_INSTALMENT_CLAIMED_DISPENSED")
print(max_load_date)

# COMMAND ----------

WEEK_TO_PROCESS_DF = spark.sql("SELECT \
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT FINANCIAL_WEEK_NUMBER FROM dimCalendar WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE (FINANCIAL_WEEK_NUMBER-1)  END as V_FIN_WEEK,\
CASE WHEN FINANCIAL_WEEK_NUMBER=1 THEN (SELECT Financial_year FROM dimCalendar  WHERE cast(DATE_TIME_START as date)=(Current_date()-7)) ELSE Financial_year END as V_FIN_YEAR \
FROM dimCalendar \
WHERE cast(DATE_TIME_START as date)=Current_date()")
WEEK_TO_PROCESS_DF.createOrReplaceTempView("WEEK_TO_PROCESS")
display(WEEK_TO_PROCESS_DF)


# COMMAND ----------

DATE_RANGE_OF_WEEK_DF=spark.sql("select \
min(cast(DATE_TIME_START as date) +1) as v_start,\
max(cast(DATE_TIME_START as date)+ 1) as v_end \
FROM dimCalendar \
where Financial_year=(select V_FIN_YEAR from WEEK_TO_PROCESS) \
and FINANCIAL_WEEK_NUMBER=(select V_FIN_WEEK from WEEK_TO_PROCESS)")
DATE_RANGE_OF_WEEK_DF.createOrReplaceTempView("DATE_RANGE_OF_WEEK")
display(DATE_RANGE_OF_WEEK_DF)

# COMMAND ----------

if(LoadType=="I"):
  ERD_EPSFilter="eps.UPDATETIME between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
  INSTALMENT_Filter="DI.DISPENSEDDATE between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
  Claimed_filter="eps.ClaimSendDateTime between  (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK)"
else:
 ERD_EPSFilter="eps.UPDATETIME between cast(\'"+str(DateFrom)+"\' as timestamp) and cast(\'"+str(DateTo)+"\' as timestamp)"
 INSTALMENT_Filter="DI.DISPENSEDDATE between cast(\'"+str(DateFrom)+"\' as timestamp) and cast(\'"+str(DateTo)+"\' as timestamp)"
 Claimed_filter="eps.ClaimSendDateTime between cast(\'"+str(DateFrom)+"\' as timestamp) and cast(\'"+str(DateTo)+"\' as timestamp)" 
print(ERD_EPSFilter)
print(INSTALMENT_Filter)
print(Claimed_filter)

# COMMAND ----------

TMP_ERDDF=spark.sql("SELECT eps.STORECODE,dc.FINANCIAL_WEEK_NUMBER,dc.FINANCIAL_YEAR, \
COUNT(DISTINCT EPS.ElectronicPrescriptionskID) as scripts , \
COUNT(DISTINCT epi.ElectronicPrescribedItemSKID)  as items  \
from Columbus_Curation.curateadls_ElectronicPrescription EPS \
inner join Columbus_Curation.curateadls_ElectronicPrescribedItem Epi on epi.ElectronicPrescriptionID= EPS.ElectronicPrescriptionSKID \
inner join DimCalendar dc on cast(eps.UPDATETIME as date) = cast(dc.DATE_TIME_START as date) \
where "+str(ERD_EPSFilter)+" \
and eps.Type='EPS' \
and length (EPS.SourceElectronicPrescriptionID)=20 \
AND eps.ElectronicPrescriptionStatus in ('FD','PD') \
and eps.ReceivedTime is not null \
AND NVL(epi.IsNotDispensableIndicator,0)<>1 \
AND eps.ClaimStatus <>'Claimed' \
AND eps.RepeatIssueNumber is not null \
GROUP BY eps.StoreCode,dc.FINANCIAL_WEEK_NUMBER,dc.FINANCIAL_YEAR ")
#display(TMP_ERDDF)
TMP_ERDDF.createOrReplaceTempView('ERD')

# COMMAND ----------

INSTALMENTDF=spark.sql("select  \
PG.StoreCode STORECODE,\
dc.FINANCIAL_WEEK_NUMBER,dc.FINANCIAL_YEAR,\
COUNT(DISTINCT PF.SourceKey) SCRIPTS, \
COUNT(DISTINCT PI.SourceKey)ITEMS \
FROM Columbus_Curation.curateadls_PrescriptionGroup PG \
INNER JOIN Columbus_Curation.curateadls_PrescriptionForm PF ON PF.PrescriptionGroupID=PG.PrescriptionGroupSKID \
INNER JOIN columbus_curation.curateadls_prescriptionformtype  FT ON FT.PrescriptionFormTypeSKID=PF.PrescriptionFormTypeID \
INNER JOIN Columbus_Curation.curateadls_PrescribedItem PI ON PI.PrescriptionFormID=PF.PrescriptionFormSKID \
INNER JOIN Columbus_Curation.curateadls_DispensedItem DI ON DI.PrescribedItemID=PI.PrescribedItemSKID \
INNER JOIN Columbus_Curation.curateadls_InstalmentItem II ON II.DispensedItemCode=DI.DispensedItemSKID AND II.InstalmentStatus='Dispensed' \
inner join DimCalendar dc on cast(DI.DispensedDate as date) = cast(dc.DATE_TIME_START as date) \
WHERE "+str(INSTALMENT_Filter)+" \
and FT.Description ='FP10MDA' \
AND FT.PrescriptionFormTypeStatus='Active' \
AND DI.Quantity>0 \
and nvl(PI.IsNotDispensedIndicator,0) <>1 \
AND UPPER(PG.PrescriptionGroupStatus) NOT IN ('DRAFTED','DELETED') \
AND UPPER(PF.PrescriptionFormStatus) <> 'DELETED' \
GROUP BY PG.StoreCode,dc.FINANCIAL_WEEK_NUMBER,dc.FINANCIAL_YEAR") 
INSTALMENTDF.createOrReplaceTempView('INSTALMENT')

# COMMAND ----------

TMP_CLAIMEDDF=spark.sql("select \
eps.StoreCode STORECODE, \
dc.FINANCIAL_WEEK_NUMBER,dc.FINANCIAL_YEAR,\
COUNT(DISTINCT EPI.SourceKey) ITEMS, \
max(eps.RunDateTime) as RUN_DATE_TIME \
from Columbus_Curation.curateadls_ElectronicPrescription eps \
inner join Columbus_Curation.curateadls_ElectronicPrescribedItem Epi \
on epi.ElectronicPrescriptionID= EPS.ElectronicPrescriptionSKID \
inner join DimCalendar dc on cast(eps.ClaimSendDateTime as date) = cast(dc.DATE_TIME_START as date) \
WHERE "+str(Claimed_filter)+" \
and eps.Type='EPS' and \
eps.ElectronicPrescriptionStatus not in ('DEL','PR','EXP','PRA','IS') \
and length(EPS.SourceElectronicPrescriptionID)=20 \
AND eps.RepeatIssueNumber is not null \
AND epi.NHSStatus='DIS' \
AND eps.ClaimStatus='Claimed' \
GROUP BY eps.StoreCode,dc.FINANCIAL_WEEK_NUMBER,dc.FINANCIAL_YEAR\
") 
TMP_CLAIMEDDF.createOrReplaceTempView('CLAIMED')
display(TMP_CLAIMEDDF)
    

# COMMAND ----------


 OutputDF=spark.sql("select SK_STORE,SK_WEEK_ENDING_DATE,DISPENSED_ERD_SCRIPTS,DISPENSED_ERD_ITEMS,DISPENSED_INSTALMENT_SCRIPTS,CLAIMED_ERD_ITEMS,RUN_DATE_TIME,CREATE_DATETIME,UPDATE_DATETIME from (SELECT \
 ST.SK_STORE, \
 cast(date_format(max(DATE_TIME_END), 'yyyyMMdd') as int) as SK_WEEK_ENDING_DATE,\
 NVL(D.SCRIPTS,0) as DISPENSED_ERD_SCRIPTS, \
 NVL(D.ITEMS,0) as DISPENSED_ERD_ITEMS, \
 nvl(I.SCRIPTS,0) as DISPENSED_INSTALMENT_SCRIPTS, \
 nvl(I.ITEMS,0) as DISPENSED_INSTALMENT_ITEMS, \
 nvl(C.ITEMS,0) as CLAIMED_ERD_ITEMS, \
 max(C.RUN_DATE_TIME) as RUN_DATE_TIME, \
 dc.FINANCIAL_WEEK_NUMBER,dc.FINANCIAL_YEAR, \
 current_timestamp() as CREATE_DATETIME \
 ,cast(Null as timestamp) as UPDATE_DATETIME \
 FROM dimPharmacySite st \
 LEFT join ERD D on st.site_code=D.STORECODE \
 left join INSTALMENT I on st.site_code=I.STORECODE \
 left join CLAIMED C on st.site_code=C.STORECODE \
 inner join dimCalendar dc on D.FINANCIAL_WEEK_NUMBER=dc.FINANCIAL_WEEK_NUMBER and D.FINANCIAL_YEAR=dc.FINANCIAL_YEAR \
 where PRICING_REGION_number=1 \
 AND st.scd_active_flag='Y' \
 group by ST.SK_STORE,D.SCRIPTS,D.ITEMS,I.SCRIPTS,I.ITEMS,C.ITEMS,dc.FINANCIAL_WEEK_NUMBER,dc.FINANCIAL_YEAR)a \
 ")
 display(OutputDF)

# COMMAND ----------

append_to_synapse (OutputDF,'con_columbus.FACT_EPS_ERD_INSTALMENT_CLAIMED_DISPENSED')