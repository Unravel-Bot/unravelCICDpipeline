# Databricks notebook source
import pandas as pd
import os
import glob
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

SS_DIM_PHARMACY=getlatestfile('/dbfs/mnt/self-serve/DSE/CCIEX/BootUK_NHS_market_data_NHSX/Default/')
SourceDF=spark.read.format('csv').option('header',True).option('delimiter',',').load(SS_DIM_PHARMACY)
SourceDF.createOrReplaceTempView('SS_DIM_PHARMACY')

# COMMAND ----------

SS_DIM_PHARMACYDF=spark.sql("select distinct case when cast(min(date) over (partition by odscode,`CONTRACTOR NAME`,POSTCODE,DSP,TYPE,CHAIN,LONGITUDE,LATITUDE,country)  as date)  is null then cast('1900-01-01' as date) else cast(min(date) over (partition by odscode,`CONTRACTOR NAME`,POSTCODE,DSP,TYPE,CHAIN,LONGITUDE,LATITUDE,country)  as date)  end as ODS_DATE_FROM, case when cast(max(date) over (partition by odscode,`CONTRACTOR NAME`,POSTCODE,DSP,TYPE,CHAIN,LONGITUDE,LATITUDE,country)  as date) is null then cast('9999-12-31' as date) else cast(max(date) over (partition by odscode,`CONTRACTOR NAME`,POSTCODE,DSP,TYPE,CHAIN,LONGITUDE,LATITUDE,country)  as date) end  as 0DS_DATE_TO, cast(NVL(DENSE_RANK() over (order by odscode,`CONTRACTOR NAME`, POSTCODE, DSP, TYPE, CHAIN, LONGITUDE, LATITUDE,country),0) as int) as ODS_CODE_S_KEY,(case when(TRIM(ODSCODE))is null then 'UNKNOWN' else TRIM(ODSCODE) end) AS ODS_CODE_P_KEY,(case when TRIM(COUNTRY) is null then'UNKNOWN' else TRIM(COUNTRY) end) AS REPORTING_COUNTRY_NAME, CASE WHEN TRIM(DSP) IS NULL OR DSP='NA' THEN 'N' when  trim(DSP)='Y' then 'Y' else -1 END AS DSP,(case when CASE WHEN TRIM(DSP)='Y' THEN 'ONLINE' WHEN TRIM(DSP) IS NULL OR DSP='NA' THEN 'INSTORE' END is null then 'UNKNOWN' else (CASE WHEN TRIM(DSP)='Y' THEN 'ONLINE' WHEN TRIM(DSP) IS NULL OR DSP='NA' THEN 'INSTORE' END) end) AS MARKET_SPLIT, (case when TRIM(TYPE) is null then 'UNKNOWN' else TRIM(TYPE) end) AS PHARMACY_TYPE, (case when TRIM(CHAIN) is null then 'UNKNOWN' else TRIM(CHAIN) end) AS PHARMACY_CHAIN,(case when TRIM(`CONTRACTOR NAME`) is null then 'UNKNOWN' else TRIM(`CONTRACTOR NAME`) end) AS CONTRACTOR_NAME,(case when POSTCODE is null then 'UNKNOWN' else POSTCODE end)AS POSTAL_CODE,case when cast(LONGITUDE as decimal(30,5))  is null then 0.00000 else  cast(LONGITUDE as decimal(30,5)) end  AS LONGITUDE,case when cast(LATITUDE as decimal(30,5)) is null then  0.0000 else cast(LATITUDE as decimal(30,5)) end  AS LATITUDE, current_timestamp() AS CREATE_DATETIME,cast('null' as timestamp) as UPDATE_DATETIME from SS_DIM_PHARMACY")

# COMMAND ----------

truncate_and_load_synapse (SS_DIM_PHARMACYDF,'con_dwh.SS_DIM_PHARMACY')