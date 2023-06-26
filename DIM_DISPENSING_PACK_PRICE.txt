# Databricks notebook source
# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

max_load_date=last_loaded_date("con_columbus.DIM_DISPENSING_PACK_PRICE")
print(max_load_date)

# COMMAND ----------

RefLOVDF = synapse_sql("select LOVDescription,LOVName from ser_pharmaceuticals.RefLOV where lovname in ('England','Scotland','Northern Ireland','Wales')")
RefLOVDF.createOrReplaceTempView('RefLOV')

# COMMAND ----------

maxSKIDdf = get_max_SK('con_columbus.DIM_DISPENSING_PACK_PRICE','SK_DISPENSING_PACK_PRICE')
maxSKID = maxSKIDdf.select("SK_DISPENSING_PACK_PRICE").first()[0]
maxSKID = int(maxSKID or 0)

# COMMAND ----------

temp_df  = spark.sql("select distinct b.* from (select * from columbus_curation.curateadls_pharmacyproductsku) b, (select SourceKey,max(RunDateTime) as RunDateTime  from columbus_curation.curateadls_pharmacyproductsku group by SourceKey ) a  where a.RunDateTime = b.RunDateTime and a.SourceKey = b.SourceKey")
temp_df.createOrReplaceTempView('pharmacyproductsku')

# COMMAND ----------

BTDF = spark.sql("SELECT \
BTSP.PriceAmount AS BT_PRICE_AMOUNT, \
BTSP.PharmacyProductID as BT_ID_PRODUCT, \
BTSP.Region as BT_REGION \
FROM columbus_curation.curateadls_BasicTariffSellingPrice BTSP \
WHERE current_date() > BTSP.StartDate AND (current_date() < BTSP.EndDate OR BTSP.EndDate IS NULL)")
BTDF.createOrReplaceTempView('BT')

# COMMAND ----------

AVDF = spark.sql("SELECT \
ATSP.PriceAmount As AV_PRICE_AMOUNT, \
ATSP.PharmacyProductID as AV_ID_PRODUCT, \
cast(LV.KEY as int) as AV_REGION \
FROM columbus_curation.curateadls_AverageTradeSellingPrice ATSP, \
(select LEFT(RefLOV.LOVDescription,1) AS KEY ,RefLOV.LOVName from RefLOV) LV \
WHERE current_date() > ATSP.StartDate AND (current_date() < ATSP.EndDate OR ATSP.EndDate IS NULL) \
and ATSP.PharmacyProductID NOT IN (select BT_ID_PRODUCT from BT)")
AVDF.createOrReplaceTempView('AV')


# COMMAND ----------

DFDF=spark.sql("SELECT  \
PR.PharmacyProductID as DF_ID_PRODUCT, \
LV.KEY as DF_REGION \
FROM columbus_curation.curateadls_PharmacyProduct PR ,\
 (select RefLOV.LOVName,LEFT(RefLOV.LOVDescription,1) as kEY from RefLOV ) LV \
WHERE PR.ProductStatus = 'Valid' \
and PR.PharmacyProductID NOT IN (select BT_ID_PRODUCT from BT) \
and PR.PharmacyProductID NOT IN (select AV_ID_PRODUCT from AV) \
")
DFDF.createOrReplaceTempView('DF')

# COMMAND ----------


PDDF=spark.sql("SELECT  \
PR.Sourcekey PP, \
PPS.PharmacyProductSKUID as PD_PharmacyProductSKUID, \
BT.BT_PRICE_AMOUNT AS BT_PRICE, \
AV.AV_PRICE_AMOUNT AS AV_PRICE, \
CASE \
WHEN PR.PharmacyProductID = BT.BT_ID_PRODUCT \
THEN BT.BT_PRICE_AMOUNT \
WHEN PR.PharmacyProductID = AV.AV_ID_PRODUCT \
THEN AV.AV_PRICE_AMOUNT \
ELSE 0.01 \
END AS PP_PRICE,  \
CASE \
WHEN PR.PharmacyProductID = BT.BT_ID_PRODUCT \
THEN 'Basic' \
WHEN PR.PharmacyProductID = AV.AV_ID_PRODUCT \
THEN 'Average' \
ELSE 'Default' \
END PRICE_SOURCE, \
CASE \
WHEN PR.PharmacyProductID = BT.BT_ID_PRODUCT \
THEN BT.BT_REGION \
WHEN PR.PharmacyProductID = AV.AV_ID_PRODUCT \
THEN AV.AV_REGION \
ELSE DF.DF_REGION \
END REGION \
FROM columbus_curation.curateadls_PharmacyProduct PR \
LEFT JOIN columbus_curation.curateadls_productproductsku PPS ON PR.PharmacyProductID = PPS.PharmacyProductID \
LEFT JOIN BT ON BT.BT_ID_PRODUCT = PR.PharmacyProductID \
LEFT JOIN AV ON AV.AV_ID_PRODUCT = PR.PharmacyProductID \
LEFT JOIN DF ON DF.DF_ID_PRODUCT = PR.PharmacyProductID \
WHERE PR.ProductStatus = 'Valid' AND PPS.IsPrimarySKUIndicator = 1 \
 ")
PDDF.createOrReplaceTempView('PD')

# COMMAND ----------

# method is loading data to synapse stage table the by using post action query upserting to synapse main table using SCD2.
# we should have both staging table and  main table ddls similar in syanapse and main table should have isActive column at the end of the table.
def upsertDWHscd2(df,dwhStagingTable,synapse_output_table,key_columns,deltaName,dwhStagingDistributionColumn,Column):
     
    #STEP1: Derive dynamic soft delete statement to update existing record isActive column as 'N' in main table if the source record is older
    lookupCols =key_columns.split(",")
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhStagingTable  +"."+ col  + "="+ synapse_output_table +"." + col + " and "
 
    
    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is greater than existing record
      whereClause= whereClause + dwhStagingTable  +"."+ deltaName  + ">="+ synapse_output_table +"." + deltaName
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]

    deleteSQL = "update " + synapse_output_table + " set SCD_ACTIVE_FLAG = 'N', UPDATE_DATETIME = current_timestamp, RECORD_END_DATE = getdate()-1 where exists (select 1 from " + dwhStagingTable + " where " +whereClause +") and SCD_ACTIVE_FLAG='Y';"
 
    #STEP2: Insert with Updating isActive column as 'Y' in main table
    
    insertSQL ="Insert Into " + synapse_output_table + " select "  + Column + " from " + dwhStagingTable +" where RecordStatusFlag<>'D';"

    #STEP3: Delete existing records but outdated records from SOURCE
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + synapse_output_table  +"."+ col  + "="+ dwhStagingTable +"." + col + " and "
 
    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is lesser than existing record
      whereClause= whereClause + synapse_output_table  +"."+ deltaName  + "> "+ dwhStagingTable +"." + deltaName
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]
    #commenting below line because we using overwrite to stage table
    #deleteOutdatedSQL = "delete from " + dwhStagingTable + " where exists (select 1 from " + synapse_output_table + " where " + whereClause + " );"
 
    #consolidate post actions SQL
    #postActionsSQL = deleteSQL + insertSQL + deleteOutdatedSQL
    postActionsSQL = deleteSQL + insertSQL
    print("postActionsSQL={}".format(postActionsSQL))
    
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

OutputDF = spark.sql("SELECT distinct \
PS.SourceKey as PRODUCT_SKU_CODE, \
cast(PP_PRICE as decimal(24,4) ) AS PP_UNIT_PRICE, \
nvl(pd.region,-1) AS PRICING_REGION, \
pd.price_source AS PRICE_SOURCE, \
cast('9999-12-31 11:59:59' as timestamp ) as RECORD_END_DATE, \
PS.RunDateTime as RUN_DATE_TIME, \
current_timestamp as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME, \
RecordStatusFlag \
FROM PharmacyProductSKU PS \
JOIN PD ON PD.PD_PharmacyProductSKUID = PS.PharmacyProductSKUID \
WHERE PS.ProductSKUStatus  = 'Active' \
and PS.RunDateTime > CAST('"+str(max_load_date)+"' as timestamp) ")
OutputDF.createOrReplaceTempView("Output")

# COMMAND ----------

PACKPRICE = synapse_sql("select PRODUCT_SKU_CODE,Pricing_region from con_columbus.DIM_DISPENSING_PACK_PRICE where scd_active_flag='Y'")
PACKPRICE.createOrReplaceTempView('PACKPRICE')


# COMMAND ----------

DIM_DISPENSING_PACK_PRICE = synapse_sql("select * from con_columbus.DIM_DISPENSING_PACK_PRICE where scd_active_flag='Y'")
DIM_DISPENSING_PACK_PRICE.createOrReplaceTempView('DIM_DISPENSING_PACK_PRICE')

# COMMAND ----------

ddpp_df=spark.sql("select a.*,case when b.PRODUCT_SKU_CODE is not null then current_timestamp else cast('1900-01-01 00:00:00' as timestamp) end RECORD_START_DATE from output a left join PACKPRICE b on a.PRODUCT_SKU_CODE=b.PRODUCT_SKU_CODE and a.PRICING_REGION=b.PRICING_REGION")
ddpp_df.createOrReplaceTempView('dpp_df')

# COMMAND ----------

Final=spark.sql("select ot.* from dpp_df ot left join DIM_DISPENSING_PACK_PRICE DP on OT.PRODUCT_SKU_CODE=DP.PRODUCT_SKU_CODE and ot.PP_UNIT_PRICE=dp.PP_UNIT_PRICE and ot.PRICING_REGION=dp.PRICING_REGION and ot.PRICE_SOURCE=dp.PRICE_SOURCE where DP.PRODUCT_SKU_CODE is null")

# COMMAND ----------

Column = "cast('"+str(maxSKID)+"' + row_number() over (order by PRODUCT_SKU_CODE ) as int) as SK_DISPENSING_PACK_PRICE,PRODUCT_SKU_CODE,PP_UNIT_PRICE,PRICING_REGION,PRICE_SOURCE,RECORD_START_DATE,RECORD_END_DATE, 'Y' as SCD_ACTIVE_FLAG, RUN_DATE_TIME,CREATE_DATETIME, UPDATE_DATETIME"

# COMMAND ----------

upsertDWHscd2(Final,'con_stg.DIM_DISPENSING_PACK_PRICE','con_columbus.DIM_DISPENSING_PACK_PRICE','PRODUCT_SKU_CODE',None,None,Column)