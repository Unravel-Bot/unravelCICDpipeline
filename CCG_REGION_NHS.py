# Databricks notebook source
import pandas as pd
import os
import glob
from pyspark.sql.functions import explode
import datetime



# COMMAND ----------

# MAGIC %run "../../Common/Upsert_to_Synapse"

# COMMAND ----------

# MAGIC %run "../../Common/read_from_synapse"

# COMMAND ----------

def getlatestfile(path):
    LMT=datetime.datetime(1970, 1, 1, 0, 0, 0,0)
    for file_item in os.listdir(path):
        file_path = os.path.join(path, file_item)
        ti_m = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if ti_m>LMT:
            LMT=ti_m
            LastModifiedFile=file_path[5:]
    return(LastModifiedFile)

# COMMAND ----------

nhsSource=getlatestfile('/dbfs/mnt/self-serve/DSE/CCIEX/BootsUK_SHP_Boots_area_boundaries_NHSX/Default/')
print(nhsSource)
CCGSource=getlatestfile('/dbfs/mnt/self-serve/DSE/CCIEX/BootsUK_SHP_Clinical_Commissioning_Grp_Boundaries_ArcGIS/Default/')
print(CCGSource)
regionsSource=getlatestfile('/dbfs/mnt/self-serve/DSE/CCIEX/BootsUK_SHP_Boots_Region_Boundaries_ArGIS/Default/')
print(regionsSource)

# COMMAND ----------


 #nhsx = spark.read.option("multiline", "true").json('/mnt/self-serve/DSE/CCIEX/BootsUK_SHP_Boots_area_boundaries_NHSX/Default/*')
 nhsx = spark.read.option("multiline", "true").json(nhsSource) 
 #CCG = spark.read.option("multiline", "true").json('/mnt/self-serve/DSE/CCIEX/BootsUK_SHP_Clinical_Commissioning_Grp_Boundaries_ArcGIS/Default/*')
CCG = spark.read.option("multiline", "true").json(CCGSource) 
#regions = spark.read.option("multiline", "true").json('/mnt/self-serve/DSE/CCIEX/BootsUK_SHP_Boots_Region_Boundaries_ArGIS/Default/*')
regions = spark.read.option("multiline", "true").json(regionsSource) 

# COMMAND ----------


nhsxDF=nhsx.withColumn("features", explode("features").alias('features')).select('features.properties.*')
nhsxDF.createOrReplaceTempView('NHS_AREA')

CCGDF=CCG.withColumn("features", explode("features").alias('features')).select('features.properties.*')
CCGDF.createOrReplaceTempView('CCG_AREA')

RegionDF=regions.withColumn("features", explode("features").alias('features')).select('features.properties.*')
RegionDF.createOrReplaceTempView('Region')

# COMMAND ----------

NHS_DF=spark.sql("select cast(NVL(NHS_AREA.AREA,0) as int) as AREA_S_KEY,(case when AREA_NAME is null then 'UNKNOWN' else AREA_NAME end) as AREA_NAME_P_KEY,cast(NVL(NHS_AREA.BTC_AREA,0) as int) as BTC_AREA,cast((case when NHS_AREA.DIVISION is null then -1 else NHS_AREA.DIVISION end) as int) as DIVISION_CODE,(case when DIVISION_N is null then 'UNKNOWN' else DIVISION_N end) as DIVISION_NAME,cast(NHS_AREA.XCENTER as decimal(30,5)) as LATITUDE,cast(NHS_AREA.YCENTER as decimal(30,5)) as LONGITUDE,current_timestamp as CREATE_DATETIME,cast(null as timestamp) as UPDATE_DATETIME from NHS_AREA")

# COMMAND ----------

CCG_DF=spark.sql("select cast(NVL(monotonically_increasing_id(),0) as int) as CCG_CODE_S_KEY,(case when A.CCGCD is null then 'UNKNOWN' else A.CCGCD end) as CCG_CODE_P_KEY,(case when A.CCGNM is null then 'UNKNOWN' else A.CCGNM end) as CCG_NAME,cast(NVL(A.`long`,0) as decimal(30,5)) as LONGITUDE,cast(NVL(A.lat,0) as decimal(30,5)) as LATITUDE,current_timestamp() AS CREATE_DATETIME,cast(null as timestamp) as UPDATE_DATETIME from CCG_AREA A")
CCG_DF.createOrReplaceTempView('CCG_DF')

# COMMAND ----------

Region_DF=spark.sql("select cast((case when A.REGION is null then -1 else A.REGION end) as int) as REGION_CODE_P_KEY,cast((case when A.DIVISION is null then -1 else A.DIVISION end) as int) as DIVISION_CODE,(case when A.REGION_NAM is null then 'UNKNOWN' else A.REGION_NAM end ) as REGION_NAME,(case when A.DIVISION_N is null then  'UNKNOWN' else A.DIVISION_N end ) as DIVISION_NAME,cast(A.X_CENTER as decimal(30,5)) as LONGITUDE,cast(A.Y_CENTER as decimal(30,5)) as LATITUDE,current_timestamp as CREATE_DATETIME,cast(null as timestamp) as UPDATE_DATETIME from Region AS A")

# COMMAND ----------

truncate_and_load_synapse (NHS_DF,'con_dwh.SS_DIM_NHS_AREA')

# COMMAND ----------

truncate_and_load_synapse (CCG_DF,'con_dwh.SS_DIM_CLINICAL_COMMISSIONING_GROUPS_AREA')

# COMMAND ----------

truncate_and_load_synapse (Region_DF,'con_dwh.SS_DIM_PHARMACY_REGION_AREA')
