# Databricks notebook source
# DBTITLE 0,Libraries
#Import Libraries
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Get pipeline parameters
dbutils.widgets.text("RunDate", '', '')
RunDate = dbutils.widgets.get('RunDate') 
print(RunDate)
dbutils.widgets.text("RunTime", '', '')
RunTime = dbutils.widgets.get('RunTime') 
print(RunTime)
dbutils.widgets.text("SourcePath", '', '')
SourcePath = dbutils.widgets.get('SourcePath') 
print(SourcePath)
dbutils.widgets.text("DestinationPath", '', '')
DestinationPath = dbutils.widgets.get('DestinationPath')
print(DestinationPath)
dbutils.widgets.text("FileList", "","")
FileList = dbutils.widgets.get("FileList")
print(FileList)

# COMMAND ----------

#source file location - this will be wrangled location
source_path="/mnt/idf-cleansed/"+SourcePath+"/"
print(source_path)

#destination path 
dest_path="/mnt/idf-curatestage/"+DestinationPath+'/'+RunDate+RunTime+'/'
print(dest_path)

FileList=eval(FileList)
print(FileList)
print(type(FileList))

SourceFileCount=len(FileList)
#create list of file path
SourceList=[]
for file in FileList:
    SourceList.append(source_path+file) #source path + filename 
print(SourceList)

#SourceList=str(SourceList)[1:-1].replace("'","").replace(" ","")  #replacing string character and space
#print(SourceList)

# COMMAND ----------

DF=spark.read.format("xml").option("rootTag","soapenv:Envelope").option("rowTag","soapenv:Body").option("rowtTag","hr:QueryUserResponse").option("rowTag","hr:result").load(','.join(SourceList))
#dfxml1.show()
DF.printSchema()
#display(DF)

# COMMAND ----------

#Function for adding missing column and converting all columns to string
def detect_data(column, DF, data_type):
    if not column in DF.columns:
        ret = lit(None).cast(data_type)
    else:
        ret = col(column).cast(data_type)

    return ret

# COMMAND ----------

#removing hr: 
DF1 = DF.toDF(*[x.lstrip("hr:") for x in DF.columns])
#display(DF1)

# COMMAND ----------

if DF1.rdd.isEmpty():
    dbutils.notebook.exit({"SourceCount":0,"DestinationCount":0,"DestinationTableList":""})
    #dbutils.notebook.exit ('stop')
else:
    print('Source data is available')

# COMMAND ----------

DF2=DF1.selectExpr('id','Name','Email','UserName').withColumnRenamed("id","ID").withColumnRenamed("name","NAME").withColumnRenamed("Email","EMAIL").withColumnRenamed("UserName","USERNAME")
display(DF2)

# COMMAND ----------

ReqColumnlist=['ID','USERNAME','NAME','EMAIL']

# COMMAND ----------

for ReqCol in ReqColumnlist:
    Final_DF = DF2.withColumn(ReqCol, detect_data(ReqCol, DF2, StringType()))

# COMMAND ----------

CurateStageDF=Final_DF.select([col(c) for c in ReqColumnlist])

# COMMAND ----------

#Write final data to curatestage layer as parquet
CurateStageDF.write.mode('append').format("parquet").save(dest_path)

# COMMAND ----------

#exit notebook with table names
DestinationCount=CurateStageDF.count()
dbutils.notebook.exit({"SourceCount":DestinationCount,"DestinationCount":DestinationCount,"DestinationTableList":""})