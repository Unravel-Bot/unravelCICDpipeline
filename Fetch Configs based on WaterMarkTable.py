# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
import datetime
import glob
import os
import time
import pandas as pd
from delta.tables import *
from pyspark.sql import SparkSession

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, "/mnt/idf-reports/Collibra/CollibraWatermarkTable/")

os.chdir(r'/dbfs/mnt/idf-config/')
files = glob.glob('CurateStandard/**/*.json',recursive = True)
updatedvalue=[]
fileNamestandard=spark.sql("select FileName from Collibra_ReferenceDB.CollibraWatermarkTable where ProcessStage='CurateStandard'")
fileNamestandard=fileNamestandard.toPandas()
for filepath in files:
  fileName_absolute = os.path.basename(filepath)
  filepath=os.path.dirname(filepath)
  filepathname=filepath+'/'+fileName_absolute
  updatedfilename='/dbfs/mnt/idf-config/'+filepathname
    
  Lastmodifiedtime= os.path.getmtime(filepathname)
  LPT_time = datetime.datetime.utcfromtimestamp(Lastmodifiedtime)
  standard_LPTDF=spark.sql("select LastProcessedTime from Collibra_ReferenceDB.CollibraWatermarkTable where ProcessStage='CurateStandard' and FileName =='"+str(updatedfilename)+"'")
  if standard_LPTDF.rdd.isEmpty():
        standard_LPTUpdated ='1900-01-01 00:00:00'
  else:
    standard_LPTNew=standard_LPTDF.withColumn("LastProcessedTime", split(col("LastProcessedTime"),"\.").getItem(0))
    standard_LPTUpdated=standard_LPTNew.collect()[0][0]
  
  standard_LPT = datetime.datetime.strptime(standard_LPTUpdated,'%Y-%m-%d %H:%M:%S')
  file = updatedfilename in fileNamestandard.values
  if ((file == True) and (LPT_time <=standard_LPT)):
    
    updatedvalue.append(('Null','Null',0.00))

  else:
    
    updatedvalue.append((fileName_absolute,filepath,Lastmodifiedtime))
                     
                   
updatedDF=spark.createDataFrame(updatedvalue,schema=['FileName','FilePath','Lastmodifiedtime']).withColumn("Lastmodifiedtime",col("Lastmodifiedtime").cast('String'))

updatedDFF=updatedDF.filter(col("FileName")!='Null')
finaldf=updatedDFF.toPandas()

standardfiles=finaldf.values.tolist()


os.chdir(r'/dbfs/mnt/idf-config/')
filesadls = glob.glob('CurateADLS/**/*.json',recursive = True)
updatedvalue1=[]
fileNameADLS=spark.sql("select FileName from Collibra_ReferenceDB.CollibraWatermarkTable where ProcessStage='CurateADLS'")
fileNameADLS=fileNameADLS.toPandas()
for filepathadls in filesadls:
  fileName_absolute_ADLS = os.path.basename(filepathadls)
  filepath_ADLS=os.path.dirname(filepathadls)
  adlsfilepath=filepath_ADLS+'/'+fileName_absolute_ADLS
  
    
  updatedADLSfilename='/dbfs/mnt/idf-config/'+adlsfilepath

  LPT=os.path.getmtime(adlsfilepath)
  LPT_updated = datetime.datetime.utcfromtimestamp(LPT)
  
  CurateADLSLPT=spark.sql("select LastProcessedTime from Collibra_ReferenceDB.CollibraWatermarkTable where ProcessStage='CurateADLS' and FileName =='"+str(updatedADLSfilename)+"'")
  if CurateADLSLPT.rdd.isEmpty():
        CurateADLSLPTUpdated ='1900-01-01 00:00:00'
  else:
    CurateADLSLPTNew=CurateADLSLPT.withColumn("LastProcessedTime", split(col("LastProcessedTime"),"\.").getItem(0))
    CurateADLSLPTUpdated=CurateADLSLPTNew.collect()[0][0]
  
  adls_time = datetime.datetime.strptime(CurateADLSLPTUpdated,'%Y-%m-%d %H:%M:%S')
  
                    
  fileADLS = updatedADLSfilename in fileNameADLS.values
  if ((fileADLS == True) and (LPT_updated <= adls_time)):
    
    updatedvalue1.append(('Null','Null',0.00))
  else:
    
    updatedvalue1.append((fileName_absolute_ADLS,filepath_ADLS,LPT))
    
updatedDF1=spark.createDataFrame(updatedvalue1,schema=['FileName','FilePath','LPT']).withColumn("LPT",col("LPT").cast('String'))
updatedDFF1=updatedDF1.filter(col("FileName")!='Null')
finalnewdf=updatedDFF1.toPandas()
ADLSfiles=finalnewdf.values.tolist()
dbutils.notebook.exit({"message" : "File is fetched", "standardfiles": standardfiles ,"ADLSfiles": ADLSfiles})