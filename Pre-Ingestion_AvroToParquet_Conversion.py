# Databricks notebook source
from datetime import datetime
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("SourceFilePath","","")
dbutils.widgets.text("EventTypeList","","")
dbutils.widgets.text("DestinationPathDict","","")
dbutils.widgets.text("EventName","","")
dbutils.widgets.text("LoadType","","")
dbutils.widgets.text("MaxSize","","")

# COMMAND ----------

EntityList=eval(dbutils.widgets.get("EventTypeList"))
print(EntityList)
SourceFilePath=dbutils.widgets.get("SourceFilePath")
print(SourceFilePath)
DestinationPathDict=eval(dbutils.widgets.get("DestinationPathDict"))
print(DestinationPathDict)
EventName=dbutils.widgets.get("EventName")
print(EventName)
LoadType=dbutils.widgets.get("LoadType")
print(LoadType)
MaxSize=dbutils.widgets.get("MaxSize")
print(MaxSize)

RefSize=1024*1024*1024*3  ##3GB
print(RefSize)

# COMMAND ----------

# MAGIC %sql
# MAGIC create Database if not exists RPI_PreProcessing_DB;
# MAGIC
# MAGIC create table if not exists RPI_PreProcessing_DB.PreProcess_ReconciliationReference (EventName string, LoadType string, Batch string, MessageCount long, EntityName string, TotalEntityCount long, UniqueEntityCount long) 
# MAGIC using delta 
# MAGIC partitioned by (EventName,LoadType)
# MAGIC location '/mnt/idf-reports/RPI_PreProcessing_Reference/PreLanding/PreProcess_ReconciliationReference/';

# COMMAND ----------

#The function will extract Body from Avro file and extract message from body. Then each entity will be extracted and saved in temp location. Will check the file size in temp location and if it is within the reference size (2GB), then load the file to landing location. Else split the file as per the partition count and load it in landing location.

def SaveAsParquetHist(DataFrame,BatchId):
    print("Batch ",str(BatchId)," started")
    Timestamp=datetime.today().strftime('%Y%m%d%H%M%S')
    RefBatch=EventName+LoadType[:4]+Timestamp+str(BatchId)
    sourceData=[]
    jsonDF=(DataFrame.select(DataFrame.Body.cast("string")).rdd.map(lambda x: x[0]))  #Extracting the body part and converting to string format
    masterDF=spark.read.json(jsonDF)
    SourceCount=masterDF.count()
    #print(SourceCount)
    if SourceCount>0:
        messageDF=masterDF.select("message.*")  #Extracting the messages from Body dataframe
        sourceColumns=messageDF.columns
        print(sourceColumns)
        for Entity in EntityList:
            print(Entity)
            if Entity in sourceColumns:
                event=Entity+".*"
                temp_path=DestinationPathDict[Entity]["tempPath"]+"TempPath"+str(BatchId)+"/"
                temp_outpath=DestinationPathDict[Entity]["tempPath"]+str(BatchId)+"/"
                totalEntityDF=messageDF.selectExpr(event,'headers.operation as MESSAGE_OPERATION','headers.changeSequence as MESSAGE_SEQUENCE')  #Extracting the entity data and operation from message df
                collist=totalEntityDF.columns
                entityDF=totalEntityDF.na.drop(how='all',subset =collist[:-2])
                totalEntityCount=entityDF.count()
                uniqueEntityDF=entityDF.dropDuplicates().withColumn("EXTRACTION_TIME",lit('1900-01-01T00:00:00.000')) #remove duplicates and add extraction time for history load
                uniqueEntityCount=uniqueEntityDF.count()
                #display(eventDF)
                sourceData.append((EventName,LoadType,RefBatch,SourceCount,Entity,totalEntityCount,uniqueEntityCount))
                outputDF=uniqueEntityDF.select([col(c).cast("string") for c in uniqueEntityDF.columns])  #Converting the columns to string for maintaining same                                                                                                            data type 
                outputDF.coalesce(1).write.format("parquet").mode("append").save(temp_outpath)   #writing the whole dataframe to single file in temp path
                files = dbutils.fs.ls(temp_outpath)
                output_file = [x for x in files if x.name.startswith("part-")]
                #print(output_file)
                if output_file[0].size<=RefSize:  #checking the file size
                    #print(output_file[0].size)
                    print("File is within Size ")
                    print(output_file[0].size)
                    outpath=DestinationPathDict[Entity]["destPath"]+DestinationPathDict[Entity]["destFileName"]+Timestamp+"-0.parquet"
                    dbutils.fs.mv(output_file[0].path, outpath)   #moving the file to actual landing location
                    dbutils.fs.rm(temp_outpath,True)
                else:
                    #print(output_file[0].size)
                    print("File size is out of limit")
                    PartitionCount=(output_file[0].size//RefSize)+2    #defining the partition count for file with greater size
                    print(PartitionCount)
                    srcDF=spark.read.format("parquet").load(output_file[0].path)
                    sourceDF=srcDF.select([col(c).cast("string") for c in srcDF.columns])
                    sourceDF.repartition(PartitionCount).write.format("parquet").mode("append").save(temp_path)  #splitting the data to multiple partitions
                    dbutils.fs.rm(output_file[0].path,True)
                    partfiles = dbutils.fs.ls(temp_path)
                    partfilelist = [x for x in partfiles if x.name.startswith("part-")]
                    #print(partfilelist)
                    partcount=1
                    for file in partfilelist:
                        outpath=DestinationPathDict[Entity]["destPath"]+DestinationPathDict[Entity]["destFileName"]+Timestamp+'-'+str(partcount)+".parquet"
                        dbutils.fs.mv(file.path, outpath)   #moving each part files to actual landing location
                        partcount+=1
                    dbutils.fs.rm(temp_outpath,True)
                    dbutils.fs.rm(temp_path,True)
            else:
                    print(Entity,' is not available in source file')

        #tracking the details in reconciliation table
        sourceDF=spark.createDataFrame(sourceData,schema=['EventName','LoadType','Batch','MessageCount',"EntityName",'TotalEntityCount','UniqueEntityCount'])
        sourceDF.write.format("delta").mode("append").option("mergeSchema","true").partitionBy("EventName","LoadType").saveAsTable("RPI_PreProcessing_DB.PreProcess_ReconciliationReference")

    else:
        print("No messages available in the batch")
    

# COMMAND ----------

#The function will extract Body from Avro file and extract message from body. Then each entity will be extracted and saved in temp location. Will check the file size in temp location and if it is within the reference size (2GB), then load the file to landing location. Else split the file as per the partition count and load it in landing location.

def SaveAsParquetIncr(DataFrame,BatchId):
    print("Batch ",str(BatchId)," started")
    Timestamp=datetime.today().strftime('%Y%m%d%H%M%S')
    RefBatch=EventName+LoadType[:4]+Timestamp+str(BatchId)
    sourceData=[]
    jsonDF=(DataFrame.select(DataFrame.Body.cast("string")).rdd.map(lambda x: x[0]))  #Extracting the body part and converting to string format
    masterDF=spark.read.json(jsonDF)
    SourceCount=masterDF.count()
    if SourceCount>0:
        messageDF=masterDF.select("message.*")  #Extracting the messages from Body dataframe
        sourceColumns=messageDF.columns
        for Entity in EntityList:
            print(Entity)
            if Entity in sourceColumns:
                event=Entity+".*"
                temp_path=DestinationPathDict[Entity]["tempPath"]+"TempPath"+str(BatchId)+"/"
                temp_outpath=DestinationPathDict[Entity]["tempPath"]+str(BatchId)+"/"
                totalEntityDF=messageDF.selectExpr(event,'headers.operation as MESSAGE_OPERATION','headers.changeSequence as MESSAGE_SEQUENCE','headers.timestamp as EXTRACTION_TIME').withColumn("EXTRACTION_TIME", when((col("EXTRACTION_TIME")=='')|(col("EXTRACTION_TIME").isNull()),'1900-01-01T00:00:00.000').otherwise(col("EXTRACTION_TIME")))  #Extracting the entity data, operation and extraction time from message dataframe
                collist=totalEntityDF.columns
                entityDF=totalEntityDF.na.drop(how='all',subset =collist[:-3])
                totalEntityCount=entityDF.count()
                uniqueEntityDF=entityDF.dropDuplicates()  #remove duplicates
                uniqueEntityCount=uniqueEntityDF.count()
                #display(eventDF)
                sourceData.append((EventName,LoadType,RefBatch,SourceCount,Entity,totalEntityCount,uniqueEntityCount))
                outputDF=uniqueEntityDF.select([col(c).cast("string") for c in uniqueEntityDF.columns])  #Converting the columns to string for maintaining same                                                                                                            data type 
                outputDF.coalesce(1).write.format("parquet").mode("append").save(temp_outpath)   #writing the whole dataframe to single file in temp path
                files = dbutils.fs.ls(temp_outpath)
                output_file = [x for x in files if x.name.startswith("part-")]
                #print(output_file)
                if output_file[0].size<=RefSize:  #checking the file size
                    #print(output_file[0].size)
                    print("File is within Size ")
                    print(output_file[0].size)
                    outpath=DestinationPathDict[Entity]["destPath"]+DestinationPathDict[Entity]["destFileName"]+Timestamp+"-0.parquet"
                    dbutils.fs.mv(output_file[0].path, outpath)   #moving the file to actual landing location
                    dbutils.fs.rm(temp_outpath,True)
                else:
                    #print(output_file[0].size)
                    print("File size is out of limit")
                    PartitionCount=(output_file[0].size//RefSize)+2    #defining the partition count for file with greater size
                    print(PartitionCount)
                    srcDF=spark.read.format("parquet").load(output_file[0].path)
                    sourceDF=srcDF.select([col(c).cast("string") for c in srcDF.columns])
                    sourceDF.repartition(PartitionCount).write.format("parquet").mode("append").save(temp_path)  #splitting the data to multiple partitions
                    dbutils.fs.rm(output_file[0].path,True)
                    partfiles = dbutils.fs.ls(temp_path)
                    partfilelist = [x for x in partfiles if x.name.startswith("part-")]
                    #print(partfilelist)
                    partcount=1
                    for file in partfilelist:
                        outpath=DestinationPathDict[Entity]["destPath"]+DestinationPathDict[Entity]["destFileName"]+Timestamp+'-'+str(partcount)+".parquet"
                        dbutils.fs.mv(file.path, outpath)   #moving each part files to actual landing location
                        partcount+=1
                    dbutils.fs.rm(temp_outpath,True)
                    dbutils.fs.rm(temp_path,True)
            else:
                    print(Entity,' is not available in source file')

        #tracking the details in reconciliation table
        sourceDF=spark.createDataFrame(sourceData,schema=['EventName','LoadType','Batch','MessageCount',"EntityName",'TotalEntityCount','UniqueEntityCount'])
        sourceDF.write.format("delta").mode("append").option("mergeSchema","true").partitionBy("EventName","LoadType").saveAsTable("RPI_PreProcessing_DB.PreProcess_ReconciliationReference")

    else:
        print("No messages available in the batch")
    

# COMMAND ----------

#With autoloader, read the stream messages and split it into batches as per Maxsize (500GB) for History and Catchup load. "saveAsParquet" function will be applied on each batch. For incremental load, read the stream message in single batch and apply the "saveAsParquet" function. Checkpoint path will track the files to be processed for autoloader. 

path_to_checkpoint="/mnt/pre-ingestion/AutoloaderCheckPoint/Columbus/RegularLoad/"+EventName+"/"


#History load :- One time load or initial load
if LoadType=='History':
    #path_to_checkpoint="/mnt/pre-ingestion/AutoloaderCheckPoint/Columbus/History/"+EventName+"/"
    readStreamDF=spark.readStream.format("cloudFiles") \
                        .option("cloudFiles.format", "avro") \
                        .option("cloudFiles.allowOverwrites", True) \
                        .option("cloudFiles.maxBytesPerTrigger", MaxSize) \
                        .option("rescuedDataColumn", "_rescue") \
                        .schema("Body binary") \
                        .load(SourceFilePath)


    writeStreamQuery=(readStreamDF.writeStream \
                        .foreachBatch(SaveAsParquetHist) \
                        .option("checkpointLocation", path_to_checkpoint) \
                        .trigger(availableNow=True) \
                        .start()
                     )

    writeStreamQuery.awaitTermination()
    print("History Load Completed")

#Incremental Load:- Daily load
elif LoadType=='Incremental':
    #path_to_checkpoint="/mnt/pre-ingestion/AutoloaderCheckPoint/Columbus/Incremental/"+EventName+"/"
    readStreamDF=spark.readStream.format("cloudFiles") \
                        .option("cloudFiles.format", "avro") \
                        .option("cloudFiles.allowOverwrites", True) \
                        .option("rescuedDataColumn", "_rescue") \
                        .schema("Body binary") \
                        .load(SourceFilePath)
    
    writeStreamQuery=(readStreamDF.writeStream \
                        .foreachBatch(SaveAsParquetIncr) \
                        .option("checkpointLocation", path_to_checkpoint) \
                        .trigger(once=True) \
                        .start())
    writeStreamQuery.awaitTermination()
    print("Incremental Load Completed")
    
else:
    print("Invalid LoadType")