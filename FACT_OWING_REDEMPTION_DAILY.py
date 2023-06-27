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

dimCalendarDF = synapse_sql("select DATE_VALUE,DATE_KEY,DATE_TIME_START,DATE_TIME_END,FINANCIAL_WEEK_NUMBER,FINANCIAL_YEAR,year_value,calendar_week from con_columbus.dim_calendar")
dimCalendarDF.createOrReplaceTempView('dimCalendar')

dim_dispensing_pack_sizedf = synapse_sql("select dispensing_pack_size_CODE,record_start_date,record_end_date,SK_DISPENSING_PACK_SIZE from con_columbus.dim_dispensing_pack_size")
dim_dispensing_pack_sizedf.createOrReplaceTempView('dim_dispensing_pack_size')

# COMMAND ----------

if(LoadType=="I"):
     DatetimeFilter1="cast(ow.Creationdate as date) between (select v_start from DATE_RANGE_OF_WEEK) and (select v_end from DATE_RANGE_OF_WEEK) "
else:
     DatetimeFilter1="cast(ow.Creationdate as date) between cast(\'"+str(DateFrom)+"\' as timestamp) and cast(\'"+str(DateTo)+"\' as timestamp)" 

print(DatetimeFilter1)


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

OutputDF1=spark.sql("SELECT \
ds.sk_store SK_STORE \
,dc.DATE_KEY SK_DATE \
,pg.PATIENTCODE PATIENT_CODE \
,CASE WHEN pf.IsElectronicIndicator = 1 then UNIQUEPRESCRIPTIONNUMBER else PF.sourcekey end PRESCRIPTION_ID \
,dps.SK_DISPENSING_PACK_SIZE SK_DISPENSING_PACK_SIZE \
,ow.creationdate OWING_DATE, \
 ow.OwingStatus , \
 cast(ow.Creationdate as date)as Creationdate ,di.prescribeditemid \
,CASE WHEN ow.OwingStatus = 'Redeemed' THEN ow.LASTCHANGEDATE ELSE NULL END REDEEMED_DATE , \
 ds.site_code,concat(year_value,calendar_week) as yearweek,  \
current_timestamp() as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
from columbus_curation.curateadls_prescriptiongroup pg \
inner join columbus_curation.curateadls_prescriptionform pf on pf.PrescriptionGroupID = pg.PrescriptionGroupSKID \
inner join columbus_curation.curateadls_prescribeditem pi on trim(pi.prescriptionformid) = trim(pf.prescriptionformskid) \
inner join columbus_curation.curateadls_dispenseditem di on di.prescribeditemid = pi.prescribeditemskid \
inner join columbus_curation.curateadls_DISPENSEDPRODUCT dp on dp.DISPENSEDITEMid = di.dispenseditemskID \
inner join columbus_curation.curateadls_OWING ow on di.DispensedItemSKID = ow.DispensedItemID \
inner join dimPharmacySite ds on ds.SITE_CODE = ow.storecode \
inner join dim_script_mode dsm on dsm.SCRIPT_MODE = pg.SERVICEENTRYMODE \
inner join dimCalendar dc on cast(dc.DATE_TIME_START as date) = cast(ow.CREATIONDATE as date) \
left  join dim_dispensing_pack_size dps on dps.dispensing_pack_size_CODE = dp.PRODUCTSKUCODE \
where "+str(DatetimeFilter1)+" and (PG.PrescriptionSourceType,PG.SERVICETYPE) not in (('PMS','CHS')) \
and ow.OwingStatus != 'Deleted' \
and pg.MESSAGETYPE IS NULL \
and pg.prescriptiongroupstatus <> 'Drafted' \
and pf.prescriptionformstatus <> 'Drafted' \
and pi.status <> 'Drafted' ") 
OutputDF1.createOrReplaceTempView('Owing_Redemption')

# COMMAND ----------

Quantity1DF=spark.sql("select sum(nvl(dit.quantity,0)) qty,ot.PrescribedItemID,cast(ot.Creationdate as date) as Creationdate from columbus_curation.curateadls_owing ot inner join columbus_curation.curateadls_dispenseditem dit on ot.PrescribedItemID = dit.PrescribedItemID join dimCalendar dc on cast(ot.Creationdate as date)=dc.date_time_start inner join Owing_Redemption ow on dit.PrescribedItemID =ow.PrescribedItemID and cast(ot.Creationdate as date) =cast(ow.Creationdate as date) where dit.owingid is not null group by ot.PrescribedItemID,cast(ot.Creationdate as date) ")
Quantity1DF.createOrReplaceTempView('Quantity1')

# COMMAND ----------

Quantity2DF=spark.sql("select sum(nvl(ob.owingquantity,0)) qty,ob.PrescribedItemID,cast(ob.Creationdate as date) as Creationdate from columbus_curation.curateadls_OWING ob inner join Owing_Redemption ow  on ob.PrescribedItemID=ow.PrescribedItemID and cast(ob.Creationdate as date) =cast(ow.Creationdate as date) and  ob.OwingStatus = 'Outstanding' and ob.storecode=ow.site_code inner join dimCalendar dc on cast(ob.Creationdate as date)=dc.date_time_start group by ob.PrescribedItemID,cast(ob.Creationdate as date)")
Quantity2DF.createOrReplaceTempView('Quantity2')

# COMMAND ----------

outputDF=spark.sql("select \
cast(SK_STORE as int) as SK_STORE \
, cast(SK_DATE as int) as SK_DATE \
,PATIENT_CODE \
, PRESCRIPTION_ID as PRESCRIPTION_IDENTIFIER\
,cast(SK_DISPENSING_PACK_SIZE as int) as SK_DISPENSING_PACK_SIZE, \
 OWING_DATE, \
cast(nvl(qt.qty,0)+nvl(qt2.qty,0) as int) as OWED_QUANTITY,\
 CASE WHEN ow.OwingStatus = 'Redeemed' then cast(nvl(qt.qty,0) as int)  else 0 end REDEEMED_QUANTITY ,\
 REDEEMED_DATE ,\
 CASE WHEN ow.OwingStatus = 'Outstanding' then cast(nvl(qt2.qty,0) as int)  else 0 end OUTSTANDING_QUANTITY , \
current_timestamp() as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME  from Owing_Redemption ow left join quantity1 qt on qt.PrescribedItemID=ow.PrescribedItemID and qt.Creationdate=ow.Creationdate left join quantity2 qt2 on qt2.PrescribedItemID=ow.PrescribedItemID and qt2.Creationdate=ow.Creationdate")

# COMMAND ----------

append_to_synapse(outputDF,'con_columbus.FACT_OWINGS_REDEMPTION_DAILY')