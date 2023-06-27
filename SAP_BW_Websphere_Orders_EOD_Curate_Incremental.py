# Databricks notebook source
# DBTITLE 1,Importing useful libraries.
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Get pipeline parameters.
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

# DBTITLE 1,Defining mount path.
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

# DBTITLE 1,Reading the xml file (Path can be parameterise).
dfxml1=spark.read.format("xml").option("rootTag","CommerceToSAP").option("rowTag","mi_end_of_day_totals").option("inferSchema",False).load(','.join(SourceList))

# dfxml1.show()
# dfxml1.printSchema()
# display(dfxml1)

# COMMAND ----------

if dfxml1.rdd.isEmpty():
    dbutils.notebook.exit({"SourceCount":0,"DestinationCount":0,"DestinationTableList":""})
else:
    print('Source data is available')

# COMMAND ----------

# DBTITLE 1,Transforming the nested column.
dfxml1_1 = dfxml1.selectExpr('*','eod_totals.*','store_context.*')
dfxml1_1 = dfxml1_1.drop('eod_totals','store_context')


dfxml1_1=dfxml1_1.withColumn('eod_date', concat(lit("20"), col("eod_date"))) 


# display(dfxml1_1)

# COMMAND ----------

# DBTITLE 1,Creating a list of key-value pair to convert source column with target name.
list_pair=[{'storeId':'RT_LOCATIO'},
{'currency':'LOC_CURRCY'},
{'':'ZCNTRYDEL'},
{'':'ZCHANNEL'},
{'':'ZSUBCHN'},
{'':'ZORDTYPE'},
{'':'ZDEVSTFT'},
{'':'ZOFSNO'},
{'':'ZRPA_TCD1'},
{'':'ZRPA_TCD2'},
{'':'ZDEVLCHRG'},
{'':'SALES_UNIT'},
{'':'RPA_TTC'},
{'eod_date':'RPA_BDD'},
{'eod_time':'RPA_BTS2'},
{'net':'RPA_TAT'},
{'paypal':'ZRPA_TPAL'},
{'american_express':'ZRPA_TAMX'},
{'visa':'ZRPA_TVIS'},
{'mastercard':'ZRPA_TMAS'},
{'debit_mastercard':'ZRPA_TMSD'},
{'iso':'ZRPA_TISO'},
{'eod_totals_cheque':'ZEOD_TCHQ'},
{'credit_account':'ZEOD_TCRC'},
{'gift_voucher':'ZEOD_TGVC'},
{'points':'ZEOD_TPOI'},
{'boots_coupon':'ZEOD_TBCP'},
{'card':'ZEOD_TCRD'},
{'brand_coupon':'ZEOD_TBRC'},
{'captive_incentive_bond':'ZEOD_TINB'},
{'cash':'ZEOD_TCAS'},
{'high_street_voucher':'ZEOD_TVOC'},
{'bonus_bond':'ZEOD_TBON'},
{'luncheon_voucher':'ZEOD_TLVC'},
{'sodexho_voucher':'ZEOD_TSVC'},
{'':'ZEOD_TNET'},
{'Blank':'CURRENCY'},
{'Blank':'DOC_CURRCY'}]

# COMMAND ----------

#Function for adding missing column and converting all columns to string
def detect_data(column, DF, data_type):
    if not column in DF.columns:
        ret = lit(None).cast(data_type)
    else:
        ret = col(column).cast(data_type)

    return ret


# COMMAND ----------

# DBTITLE 1,Use the function to check the source file with defined columns and create a target list.
list_tgt=[]
for elements in list_pair:
    for key,val in elements.items():
        list_tgt.append(val)
        dfxml1_1 = dfxml1_1.withColumn(val, detect_data(key, dfxml1_1, StringType()))
        
print(list_tgt)

# COMMAND ----------

# DBTITLE 1,select only the target columns names from the Dataframe.
Final_1303_EOD_DF=dfxml1_1.select([col(c) for c in list_tgt])

# display(Final_1303_EOD_DF)
# Final_1303_EOD_DF.printSchema()

# COMMAND ----------

# DBTITLE 1,Assigned default value.
Final_1303_EOD_DF=Final_1303_EOD_DF.withColumn('RPA_TTC',lit('1303')) \
.withColumn('ZEOD_TNET',lit('0'))
# display(Final_1303_EOD_DF)

# COMMAND ----------

# DBTITLE 1,Write it as parquet file. (Path can be parameterise).
Final_1303_EOD_DF.write.mode('append').format("parquet").save(dest_path)


# COMMAND ----------

#exit notebook with table names
DestinationCount=Final_1303_EOD_DF.count()
dbutils.notebook.exit({"SourceCount":DestinationCount,"DestinationCount":DestinationCount,"DestinationTableList":""})