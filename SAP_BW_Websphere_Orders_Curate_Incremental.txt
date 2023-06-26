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

# DBTITLE 1,Reading the xml file .
dfxml1=spark.read.format("com.databricks.spark.xml").option("rootTag","IF_1303").option("rowTag","mi_summary").option("inferSchema",False).load(','.join(SourceList))

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
#To get all columns
dfxml1_1 = dfxml1.selectExpr('mi_order.*','mi_order_items.*','store_context.*')

data_type = dict(dfxml1_1.dtypes)['mi_order_item']
print(f'Data type of Rating is : {data_type}')

# display(dfxml1_1)
    
if data_type.startswith('array'):
    dfxml1_1=dfxml1_1.withColumn("mi_order_item",col('mi_order_item'))
else:
    dfxml1_1=dfxml1_1.withColumn("mi_order_item",array("mi_order_item"))

    
# display(dfxml1_1)
    

#To get nested columns from mi_order_item
dfxml1_1=dfxml1_1.withColumn("mi_order_item",explode_outer(dfxml1_1.mi_order_item))
#To get all columns and mi_order_items columns and drop the additional columns.
dfxml1_1=dfxml1_1.selectExpr('*','mi_order_item.*').drop('mi_order_item','_VALUE')


# display(dfxml1_1)


# COMMAND ----------

# DBTITLE 1,Removing additional prefixed underscore from column name and converts dates.
dfxml1_1 = dfxml1_1.toDF(*[x.lstrip("_") for x in dfxml1_1.columns])

# display(dfxml1_1)



dfxml1_1=dfxml1_1.withColumn('mi_date', date_format('mi_date', "yyyyMMdd")) \
.withColumn('mi_latest_dispatch_date', date_format('mi_latest_dispatch_date', "yyyyMMdd")) \
.withColumn('mi_promise_date', date_format('mi_promise_date', "yyyyMMdd"))

# display(dfxml1_1)

# COMMAND ----------

# DBTITLE 1,Creating a list of key-value pair to convert source column with target name.
list_pair=[{'storeId':'RT_LOCATIO'},
{'mi_currency':'LOC_CURRCY'},
{'mi_country_of_delivery':'ZCNTRYDEL'},
{'mi_order_number':'ZCOMORDNM'},
{'mi_original_order_number':'ZCOMORDN2'},
{'mi_sales_channel':'ZCHANNEL'},
{'mi_sales_sub_channel':'ZSUBCHN'},
{'mi_message_contents':'ZORDMESSA'},
{'mi_order_type':'ZORDTYPE'},
{'mi_date':'ZORDDATE'},
{'mi_time':'ZORDTIME'},
{'mi_cancellation_reason':'ZCREASON'},
{'mi_latest_dispatch_date':'ZDSPDATE'},
{'mi_promise_date':'ZDELVDATE'},
{'mi_delivery_service':'ZDELVPRO'},
{'mi_delivery_site':'ZDEVSTFT'},
{'mi_store_placed':'ZOFSNO'},
{'mi_cfs-store_number':'ZCFSNO'},
{'mi_pod':'ZPRDELV'},
{'mi_over18_pod':'ZPROFDEV'},
{'mi_aerosol_included':'ZAEROCEL'},
{'mi_tender_type_1':'ZRPA_TCD1'},
{'mi_credit_card_type_1':'ZRPA_TCC1'},
{'mi_tender_type_2':'ZRPA_TCD2'},
{'mi_credit_card_type_2':'ZRPA_TCC2'},
{'mi_delivery_charge':'ZDEVLCHRG'},
{'mi_delivery_currency':'ZDELVCHRC'},
{'mi_total_order_value':'ZTOTORDV'},
{'mi_order_level_discount':'ZORDDISC'},
{'mi_order_level_deal_number':'ZDEALNUM'},
{'mi_order_level_promotion_id':'ZDEALNAM'},
{'mi_order_level_media_code':'ZORDMCODE'},
{'mi_total_order_discount':'ZTOTODISC'},
{'mi_adcard_number':'ZCUSTCARN'},
{'mi_customer_number':'ZCUSTRID'},
{'mi_staff_discount_included':'ZDSTAFFF'},
{'mi_heavy':'ZHEAVYD'},
{'mi_item_code':'MATERIAL'},
{'mi_article_id':'ZOARTICLE'},
{'mi_qty_ordered':'RPA_RLQ'},
{'':'SALES_UNIT'},
{'mi_unit_price':'ZRPA_NSP'},
{'mi_unit_adjustment':'ZRPA_REP'},
{'mi_unit_paid':'ZRPA_SAP'},
{'mi_oi_total_product':'ZRPA_OLV'},
{'mi_oi_total_adjustment':'ZRPA_TOVD'},
{'mi_oi_value':'ZRPA_TOV'},
{'mi_excludes_vat':'RPA_TIF'},
{'mi_deal_number':'ZDEALI'},
{'mi_deal_promotion_id':'ZRT_PROMI'},
{'mi_article_media_code':'ZITMMCODE'},
{'mi_base_points':'ZLYPTBASE'},
{'mi_bonus_points':'ZEXTADPNT'},
{'mi_gift_boxed':'ZGIFTBOX'},
{'':'CURRENCY'},
{'':'DOC_CURRCY'},
{'':'RPA_TTC'},
{'':'RPA_BDD'},
{'':'RPA_BTS2'},
{'':'RPA_TAT'},
{'':'RPA_TCD'},
{'mi_postcode':'POSTAL_CD'},
{'mi_cust_vat_exempt':'ZCUSTVATF'},
{'mi_virtual_bundle_flag':'ZVIRBUN_F'}]

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


# COMMAND ----------

print(list_tgt)

# COMMAND ----------

# DBTITLE 1,select only the target columns names from the Dataframe.
Final_1303_DF=dfxml1_1.select([col(c) for c in list_tgt])

# display(Final_1303_DF)
# Final_1303_DF.printSchema()

# COMMAND ----------

# DBTITLE 1,Assigned default value.
Final_1303_DF=Final_1303_DF.withColumn('RPA_TTC',lit('1303')) \
.withColumn('ZEOD_TNET',lit('0'))
#display(Final_1303_DF)

# COMMAND ----------

# DBTITLE 1,Write it as parquet file.
Final_1303_DF.write.mode('append').format("parquet").save(dest_path)


# COMMAND ----------

#exit notebook with table names
DestinationCount=Final_1303_DF.count()
dbutils.notebook.exit({"SourceCount":DestinationCount,"DestinationCount":DestinationCount,"DestinationTableList":""})