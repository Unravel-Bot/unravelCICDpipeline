# Databricks notebook source
# MAGIC %sql
# MAGIC create widget text FileList default "";
# MAGIC create widget text SourcePath default "";
# MAGIC create widget text DestinationPath default "";
# MAGIC create widget text RunDate default "";
# MAGIC create widget text RunTime default "";

# COMMAND ----------

import os
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *


mountPoint='/mnt/idf-cleansed/'
tgtmountPoint='/mnt/idf-curatestage/'
first_row_is_header = "true"
delimiter = "|"
filelist=dbutils.widgets.get("FileList")
SourcePath=dbutils.widgets.get("SourcePath")
RunDate=dbutils.widgets.get("RunDate")
RunTime=dbutils.widgets.get("RunTime")
DestinationPath=dbutils.widgets.get("DestinationPath")
print(filelist)
SourceFileCount=len(filelist.split(","))
OutputTableList=[]
#dfSchema = StructType([ StructField("PIPCode", StringType(), True) ])

dfSchema1 = StructType([ StructField("PIPCode", StringType(), True) ,
StructField("Creation_Timestamp", StringType(), True)
]) 
filelist1 = filelist.replace('"','')
filelist2 = filelist1[1:-1]
print(filelist2)
for file in filelist2.split(","):
   # print(dfo)    dfo=dfo[3]    print(dfo)    print(dfo[4])
    print(file)
    file1 = file
    df=spark.read.format('csv').option('delimiter',"|").load("/mnt/idf-cleansed/SAPCOE/BTCSAPECC/SAPCOE_BRAND_PRCNT_PIPCODE_LIST_BTCSAPECC_Incr/Incremental/"+file1)
    display(df)
    dfo1=file.split("_")
    x = dfo1[7][0:4] +  "-" + dfo1[7][4:6] + "-" +  dfo1[7][6:8] + " " + dfo1[8][0:2]+":"+dfo1[8][2:4]+":"+dfo1[8][4:6]
    df1=df.toPandas()
    df1.insert(1,'Creation_Timestamp',x,True)
    df1=df1[0:]
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    display(df2)
    df2.write.mode('append').option("Header",True).format("CSV").save(tgtmountPoint+DestinationPath+"/"+RunDate+'000000'+"/")
    
    # df.show()
    

# COMMAND ----------

import os
from os import listdir
for file1 in filelist.split(","):
    foldername=file1[0:-4]
    OutputTableList.append(foldername)
    DestinationTableCount=len(OutputTableList)

# COMMAND ----------

dbutils.notebook.exit({"SourceCount":SourceFileCount,"DestinationCount":DestinationTableCount,"DestinationTableList":OutputTableList})

# COMMAND ----------

