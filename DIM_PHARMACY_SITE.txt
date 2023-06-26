# Databricks notebook source
# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------


Source='/mnt/idf-cleansed/SAPCOE/SAPMDM/IF_00122/Incremental/SAPCOE_SAPMDM_IF_00122_STORES_F_-424_20200430_180133.csv'
SourceDF=spark.read.format('csv').option('header',False).option("encoding","windows-1252").option('delimiter',',').load(Source)
SourceDF.createOrReplaceTempView('SAP_MDM_Store')

# COMMAND ----------

RefLOVDF = synapse_sql("select LOVId,LOVName,LOVKey  from [SER_PHARMACEUTICALS].[RefLOV]")
RefLOVDF.createOrReplaceTempView('RefLOV')
SITEROLEDF = synapse_sql("select siteid,siteroleid  from ser.siterole")
SITEROLEDF.createOrReplaceTempView('SITEROLE')

# COMMAND ----------

# method is loading data to synapse stage table and by using post action query upserting from synapse stage table to synapse main table.
# we should have both staging table and  main table DDL's similar in syanapse.
def upsertDWHSCD1(df,dwhStagingTable,synapse_output_table,key_columns,deltaName,dwhStagingDistributionColumn,columns):
    #STEP1: Derive dynamic delete statement to delete existing record from main table if the source record is older
    lookupCols =key_columns.split(",")
    whereClause=""
    insertWhereClause=""
    for col in lookupCols:
        whereClause= whereClause + "t."+ col  + "="+ synapse_output_table +"." + col + " and "
        insertWhereClause= insertWhereClause + dwhStagingTable  +"."+ col  + "="+ synapse_output_table +"." + col + " and "
 
    if deltaName is not None and  len(deltaName) >0:
        #Check if the last updated is greater than existing record
        whereClause= whereClause + "t."+ deltaName  + ">="+ synapse_output_table +"." + deltaName
        insertWhereClause= insertWhereClause + dwhStagingTable  +"."+ deltaName  + ">="+ synapse_output_table +"." + deltaName
    else:
        #remove last "and"
        remove="and"
        reverse_remove=remove[::-1]
        whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]
        insertWhereClause = insertWhereClause[::-1].replace(reverse_remove,"",1)[::-1]
 
    
    updateQuery = "with t as (select * from "+dwhStagingTable+" ) update "+synapse_output_table+" SET "
    
    columnList=['SITE_CODE','STORE_DESCRIPTION','COMPANY_CODE','REGION_NUMBER','REGION_NAME','LOCAL_SERVICE_CENTRE','PRICING_REGION_NUMBER','PRICING_REGION_NAME','AREA_NAME','AREA_NUMBER','STORE_STATUS','RUN_DATE_TIME','RECORD_START_DATE','RECORD_END_DATE']
    print(columnList)
    for colm in columnList:
        if colm not in lookupCols:
            updateQuery=updateQuery+ colm +"=t."+ colm +", "
    
    updateQuery=updateQuery+"UPDATE_DATETIME=CURRENT_TIMESTAMP from t where exists (select 1 from t where "+whereClause +");"
    
    #STEP2: Insert SQL to main table
    insertQuery ="Insert Into " + synapse_output_table + " select "+columns+"  from " + dwhStagingTable +" where not exists (select 1 from "+synapse_output_table+" where "+insertWhereClause+");"
    
    print(insertQuery)
  
    
    #consolidate post actions SQL
    #postActionsSQL = deleteSQL + insertSQL + deleteOutdatedSQL
    postActionsSQL = updateQuery + insertQuery 
    #print("postActionsSQL={}".format(postActionsSQL))
    
    #Use Hash Distribution on STG table where possible
    if dwhStagingDistributionColumn is not None and len(dwhStagingDistributionColumn) > 0:
        stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH (" +  dwhStagingDistributionColumn + ")"
    else:
        stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN"
     
    #Upsert/Merge to Target using STG postActions
    df.write.format("com.databricks.spark.sqldw")\
    .option("url",jdbcUrl)\
    .option("useAzureMSI","true")\
    .mode("overwrite")\
    .option("dbTable",dwhStagingTable)\
    .option("tableOptions",stgTableOptions)\
    .option('postActions', postActionsSQL)\
    .option("tempDir",temp_dir)\
    .option("maxStrLength","4000")\
    .save() 
    

# COMMAND ----------

max_load_date=last_loaded_date("con_columbus.DIM_PHARMACY_SITE")
print(max_load_date)

# COMMAND ----------

maxSKIDdf = get_max_SK('con_columbus.DIM_PHARMACY_SITE','SK_STORE')
maxSKID = maxSKIDdf.select("SK_STORE").first()[0]
maxSKID = int(maxSKID or 0)

# COMMAND ----------

OutputDF=spark.sql("SELECT  \
cast('"+str(maxSKID)+"' + row_number() over (order by s.sourcekey ) as int) as SK_STORE, \
s.sourcekey as site_code, \
s.Description as STORE_DESCRIPTION, \
S.companycode as Company_CODE, \
SAP._c14 REGION_NUMBER, \
SAP._c15 REGION_NAME, \
SC.WHOLESALERCODE LOCAL_SERVICE_CENTRE, \
LV.LOVName PRICING_REGION_NAME, \
LV.LOVKey PRICING_REGION_NUMBER, \
SAP._c16 AREA_NUMBER, \
SAP._c17 AREA_NAME, \
S.StoreStatus AS STORE_STATUS, \
s.RunDateTime as RUN_DATE_TIME, \
current_timestamp as RECORD_START_DATE, \
cast('9999-12-31 11:59:59' as timestamp ) as RECORD_END_DATE, \
current_timestamp as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_pharmacystore S \
LEFT OUTER JOIN \
( \
SELECT SSC.SourceKey, SC.WHOLESALERCODE, SC.servicecentreSTATUS,sc.ServiceCentreSiteRoleID  as  id \
FROM columbus_curation.curateadls_storeservicecentre SSC \
INNER JOIN columbus_curation.curateadls_servicecentre sc ON ssc.ServiceCentreSiteRoleId = sc.ServiceCentreSiteRoleID AND SSC.TYPE = 'LSC') sc \
ON s.pharmacystoresiteroleid = sc.id \
LEFT OUTER JOIN SAP_MDM_Store SAP ON S.COMPANYCODE = SAP._c0 \
INNER JOIN (select distinct  sr.SiteRoleId,RefLOV.LOVName,RefLOV.LOVKey \
from RefLOV inner join columbus_curation.curateadls_siteterritory  st \
on RefLOV.LOVId = st.LOVTerritoryId \
inner join SITEROLE SR  ON st.SiteId = SR.siteId ) LV \
ON LV.SiteRoleId= S.PharmacyStoreSiteRoleId \
WHERE S.IsCentralPharmacyIndicator != 1 \
and  S.RunDateTime > CAST('"+str(max_load_date)+"'as timestamp) ")

# COMMAND ----------

Column="cast('"+str(maxSKID)+"' + row_number() over (order by SK_STORE) as int),SITE_CODE,STORE_DESCRIPTION,COMPANY_CODE,REGION_NUMBER,REGION_NAME,LOCAL_SERVICE_CENTRE,PRICING_REGION_NUMBER,PRICING_REGION_NAME,AREA_NAME,AREA_NUMBER,STORE_STATUS,'Y' as SCD_ACTIVE_FLAG,RUN_DATE_TIME,RECORD_START_DATE,RECORD_END_DATE,current_timestamp,current_timestamp"

# COMMAND ----------

upsertDWHscd2(OutputDF,'con_stg.DIM_PHARMACY_SITE','con_columbus.DIM_PHARMACY_SITE','SK_STORE',None,None,Column)