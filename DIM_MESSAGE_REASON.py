# Databricks notebook source
import datetime


# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

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
    
    columnList=['DEFAULT_REASON', 'REASON_TYPE', 'MESSAGE_CODE', 'MESSAGE_BODY', 'CARER_MESSAGE_BODY', 'DISPLAY_ORDER', 'RUN_DATE_TIME']
    print(columnList)
    for colm in columnList:
        if colm not in lookupCols:
            updateQuery=updateQuery+ colm +"=t."+ colm +", "
    
    updateQuery=updateQuery+"UPDATE_DATETIME=CURRENT_TIMESTAMP from t where exists (select 1 from t where "+whereClause +");"
    
    print(updateQuery)
    print()
    print()
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

max_load_date=last_loaded_date("con_columbus.DIM_MESSAGE_REASON")

print(max_load_date)

# COMMAND ----------

maxSKIDdf = get_max_SK('con_columbus.DIM_MESSAGE_REASON','SK_REASON_TYPE')
maxSKID = maxSKIDdf.select("SK_REASON_TYPE").first()[0]
maxSKID = int(maxSKID or 0)


# COMMAND ----------

OutputDF=spark.sql("SELECT \
cast('"+str(maxSKID)+"' + row_number() over (order by SourceKey) as int)  AS SK_REASON_TYPE, \
cast(IsDefaultReasonIndicator as INT) as DEFAULT_REASON, \
ReasonType as REASON_TYPE, \
SourceKey as MESSAGE_CODE, \
MessageBody as MESSAGE_BODY, \
CarerMessageBody as CARER_MESSAGE_BODY , \
CAST(DisplayOrder AS INT) as DISPLAY_ORDER, \
Rundatetime as RUN_DATE_TIME \
FROM columbus_curation.curateadls_messagereason \
where Rundatetime > CAST('"+str(max_load_date)+"'as timestamp) \
")

display(OutputDF)

# COMMAND ----------

columns = "cast('"+str(maxSKID)+"' + row_number() over (order by MESSAGE_CODE) as int), DEFAULT_REASON, REASON_TYPE, MESSAGE_CODE, MESSAGE_BODY, CARER_MESSAGE_BODY, DISPLAY_ORDER, RUN_DATE_TIME,current_timestamp,current_timestamp"

# COMMAND ----------

upsertDWHSCD1(OutputDF,'CON_STG.DIM_MESSAGE_REASON','con_columbus.DIM_MESSAGE_REASON','MESSAGE_CODE',None,None,columns)


# COMMAND ----------

