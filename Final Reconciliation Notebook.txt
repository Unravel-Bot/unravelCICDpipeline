# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import *
from delta.tables import *
import requests
from requests.auth import HTTPDigestAuth
Recon_Date=datetime.utcnow()

# COMMAND ----------

dbutils.widgets.text("Supplier", "","")
Supplier = dbutils.widgets.get("Supplier")

dbutils.widgets.text("SourceSystemName", "","")
SourceSystemName = dbutils.widgets.get("SourceSystemName")

dbutils.widgets.text("FeedName", "","")
FeedName = dbutils.widgets.get("FeedName")

dbutils.widgets.text("FeedID", "","")
FeedID = dbutils.widgets.get("FeedID")

dbutils.widgets.text("Frequency", "","")
Frequency = dbutils.widgets.get("Frequency")

dbutils.widgets.text("BatchID", "","")
BatchID = dbutils.widgets.get("BatchID")

dbutils.widgets.text("ADLSRunDateTime", "","")
ADLSRunDateTime = dbutils.widgets.get("ADLSRunDateTime")

dbutils.widgets.text("SynapseRunDateTime", "","")
SynapseRunDateTime = dbutils.widgets.get("SynapseRunDateTime")

dbutils.widgets.text("RunDate", "","")
RunDate = dbutils.widgets.get("RunDate")

dbutils.widgets.text("RunTime", "","")
RunTime = dbutils.widgets.get("RunTime")

dbutils.widgets.text("IsHeader", "","")
IsHeader = dbutils.widgets.get("IsHeader")

dbutils.widgets.text("Classification", "","")
Classification = dbutils.widgets.get("Classification")

dbutils.widgets.text("SynapseSchema", "","")
SynapseSchema = dbutils.widgets.get("SynapseSchema")

dbutils.widgets.text("SynapseTableName", "","")
SynapseTableName = dbutils.widgets.get("SynapseTableName")

dbutils.widgets.text("SourceSystemId", "","")
SourceSystemId = dbutils.widgets.get("SourceSystemId")

dbutils.widgets.text("SourceTableName", "","")
SourceTableName = dbutils.widgets.get("SourceTableName")


# COMMAND ----------

# MAGIC %run "./Get_record_counts"

# COMMAND ----------

# DBTITLE 1,Create Database and Delta Table
# MAGIC %sql
# MAGIC create Database if not exists RPI_Reference_DB;
# MAGIC
# MAGIC create table if not exists RPI_Reference_DB.Curation_Reconciliation(Recon_Date timestamp,BatchID string,SourceName string,EntityName string,FeedName string,ProcessStage string,Count string,SynapseRunDateTime timestamp,ADLSRunDateTime timestamp,FeedID string,BadRowCount int)
# MAGIC using delta 
# MAGIC partitioned by (FeedName,Recon_Date)
# MAGIC location '/mnt/idf-reports/RPI_Reference/Curation/ReconciliationDetails';

# COMMAND ----------

# DBTITLE 1,Get Record Counts
Stage1='CurateStage'
Stage2='CurateStandard'
Stage3='CurateAdls'
Stage4='CurateSynapse'
curatestagecount = getcuratestagecount(Supplier,SourceTableName,SourceSystemName,FeedName,Frequency,RunDate,RunTime,IsHeader)
curatestandardcount = getcuratestandardcount(Supplier,SourceSystemName,FeedName,SourceTableName,SourceSystemId,ADLSRunDateTime)
curateadlscount = getcurateadlscount(SourceTableName,Classification,SourceSystemId,ADLSRunDateTime)
curatesynapsecount = querysynapse_on_rundate(SynapseTableName,SynapseSchema,SynapseRunDateTime,jdbcUrl,SourceSystemName)

# COMMAND ----------

# DBTITLE 1,Delta Table Update
StageValue=[Stage1,Stage2,Stage3,Stage4]
deltaTable = DeltaTable.forPath(spark, "/mnt/idf-reports/RPI_Reference/Curation/ReconciliationDetails")
sourceData=[] 
Count='0'
BadRowCount=''
if(curatestagecount==0):
    dbutils.notebook.exit ('stop') 
else:
    for ProcessStage in StageValue:
        sourceData.append((Recon_Date ,BatchID ,SourceSystemName,SourceTableName,FeedName ,ProcessStage,Count,SynapseRunDateTime,ADLSRunDateTime,FeedID,BadRowCount))
        updatedDF=spark.createDataFrame(sourceData,schema=['Recon_Date','BatchID','SourceName','EntityName','FeedName','ProcessStage','Count','SynapseRunDateTime','ADLSRunDateTime','FeedID','BadRowCount']).withColumn("Recon_Date",col("Recon_Date").cast('Timestamp')).withColumn("ADLSRunDateTime",col("ADLSRunDateTime").cast('Timestamp')).withColumn("SynapseRunDateTime",col("SynapseRunDateTime").cast('Timestamp'))
        updatedDF1=updatedDF.withColumn("ADLSRunDateTime",when(updatedDF.ProcessStage == "CurateAdls" ,ADLSRunDateTime)).withColumn("ADLSRunDateTime",col("ADLSRunDateTime").cast('Timestamp')).withColumn("BadRowCount",col("BadRowCount").cast('Integer'))
        updatedDF2=updatedDF1.withColumn("SynapseRunDateTime",when(updatedDF1.ProcessStage == "CurateSynapse",SynapseRunDateTime)).withColumn("SynapseRunDateTime",col("SynapseRunDateTime").cast('Timestamp'))
        updatedDF3=updatedDF2.withColumn("EntityName",when(updatedDF2.ProcessStage == "CurateAdls",SourceTableName).when(updatedDF2.ProcessStage == "CurateSynapse",SourceTableName))
        updatedDF4=updatedDF3.withColumn("EntityName",when(updatedDF3.SourceName=="SAPCAR",SourceTableName).when(updatedDF3.SourceName=="RLINK",SourceTableName).when(updatedDF3.SourceName=="COLUMBUS",SourceTableName).when(updatedDF3.SourceName=="HRSS",SourceTableName).when(updatedDF3.SourceName=="RSC",SourceTableName).when(updatedDF3.SourceName=="KENEXA",SourceTableName).when(updatedDF3.SourceName=="KALLIDUS",SourceTableName).when(updatedDF3.SourceName=="SAP_ECC",SourceTableName))
        updatedDF5=updatedDF4.withColumn("Count",when(updatedDF4.ProcessStage == "CurateStage",curatestagecount).when(updatedDF4.ProcessStage == "CurateStandard",curatestandardcount).when(updatedDF4.ProcessStage == "CurateAdls",curateadlscount).when(updatedDF4.ProcessStage == "CurateSynapse",curatesynapsecount)).withColumn("Count",col("Count").cast('String'))
     
    updatedDF5.write.format("delta").mode("append").partitionBy("FeedName","Recon_Date").saveAsTable("RPI_Reference_DB.Curation_Reconciliation")
    display(updatedDF5)
    DF=spark.sql("select * from RPI_Reference_DB.Curation_BadRowCount")
    if ((SourceSystemName=='SAPCAR') | (SourceSystemName=='RLINK') | (SourceSystemName=='COLUMBUS') |(SourceSystemName=='RSC')|(SourceSystemName=='HRSS') | (SourceSystemName=='KENEXA') | (SourceSystemName=='KALLIDUS') | (SourceSystemName=="SAP_ECC")):
        deltaTable.alias("events").merge(DF.alias("updates"),"events.FeedID = updates.FeedID and events.BatchID = updates.BatchID and events.EntityName = updates.EntityName and events.FeedID='"+FeedID+"'and events.BatchID ='"+BatchID+"'and events.EntityName ='"+SourceTableName+"'and events.ProcessStage ='"+"CurateStandard"+"'").whenMatchedUpdate(set = { "BadRowCount" :"updates.BadRowCount"} ).execute()
    else:
        deltaTable.alias("events").merge(DF.alias("updates"),"events.FeedID = updates.FeedID and events.BatchID = updates.BatchID and events.FeedID='"+FeedID+"'and events.BatchID ='"+BatchID+"'and events.ProcessStage ='"+"CurateStandard"+"'").whenMatchedUpdate(set = { "BadRowCount" :"updates.BadRowCount"} ).execute()
     

# COMMAND ----------

try:
    if((SourceSystemName=='SAPCAR') | (SourceSystemName=='RLINK') | (SourceSystemName=='COLUMBUS') |(SourceSystemName=='RSC')|(SourceSystemName=='HRSS') | (SourceSystemName=='KENEXA')|(SourceSystemName=='KALLIDUS') | (SourceSystemName=="SAP_ECC")) :
        df=spark.sql('select * from RPI_Reference_DB.Curation_BadRowCount')
        df.createOrReplaceTempView("df")
        df=df.filter(col("BatchID")==BatchID).filter(col("FeedID")== FeedID).filter(col("EntityName")== SourceTableName)
    else:
        df=spark.sql('select * from RPI_Reference_DB.Curation_BadRowCount')
        df.createOrReplaceTempView("df")
        df=df.filter(col("BatchID")==BatchID).filter(col("FeedID")== FeedID)   
    Value=df.collect()[0]
    BadCount=Value[3]
    print(BadCount)
except IndexError:
    BadCount=0
    print(BadCount)

# COMMAND ----------

if ((SourceSystemName=='RLINK') | (SourceSystemName=='COLUMBUS') |(SourceSystemName=='RSC')|(SourceSystemName=='HRSS') | (SourceSystemName=='KENEXA') | (SourceSystemName=='KALLIDUS') |(SourceSystemName=="SAP_ECC") | (SourceSystemName=='SAPBW'))  and (curateadlscount==curatesynapsecount) and (curatestagecount>0) :
    print("Data is Matching across the layers")
elif((SourceSystemName!='RLINK') | (SourceSystemName !='COLUMBUS') |(SourceSystemName !='RSC')|(SourceSystemName !='HRSS')|(SourceSystemName !='KENEXA')| (SourceSystemName!='KALLIDUS') |(SourceSystemName!="SAP_ECC") | (SourceSystemName!='SAPBW')) and(curatestagecount==curatestandardcount==curateadlscount==curatesynapsecount) and (curatestagecount>0):
    print("Data is Matching across the layers")  
else:
    LogicAppURL = "https://prod-58.northeurope.logic.azure.com:443/workflows/dc2ac967bf074d4a854d84f1761c08d2/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=h6AYw5lWMnZLRdcOfx4axp2bAllULinZDFkwsLBExhk"
    data ={"FileName":'Reconciliation Details',
         "ErrorDetails":" The Counts are not Matching for " + " SynapseTableName : " + str(SynapseTableName) + "  CurateStage_count = " + str(curatestagecount)+ "\n" +"  CurateStandard_count = " + str(curatestandardcount)+ "\n" +"  CurateADLS_count = " + str(curateadlscount)+ "\n" +"  CurateSynapse_count = " + str(curatesynapsecount)+ "\n" +"  BadRow_count = " + str(BadCount),
         "status":'Failed',
         "Pipeline_Process_Name":'PL_RECONCILIATION'}
    #myResponse = requests.post(LogicAppURL,params = data) 

dbutils.notebook.exit({" message " : " Reconciliation is Completed ", " SynapseTableName " : SynapseTableName," CurateStageCount" :  curatestagecount, "  CurateStandardCount" : curatestandardcount,"  CurateADLSCount" : curateadlscount,"  CurateSynapseCount" : curatesynapsecount})