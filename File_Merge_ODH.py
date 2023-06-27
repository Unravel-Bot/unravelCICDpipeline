# Databricks notebook source
# MAGIC %md
# MAGIC Import required Libraries

# COMMAND ----------

import os
import pandas as pd
import csv
from datetime import datetime
from os import listdir
import time

# COMMAND ----------

# MAGIC %md
# MAGIC Set The Input and Output Directory Paths

# COMMAND ----------

input_path_dir = '/mnt/odh-eventhub-capture/prep_unmerged_files/'
output_path_dir = '/mnt/odh-eventhub-capture/prep_merged_files/'
output_path_dir_temp = '/mnt/odh-eventhub-capture/prep_merged_files_temp/'
timestr = time.strftime("%Y%m%d%H%M%S")

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Temp Files from Temp Directory

# COMMAND ----------

dbutils.fs.rm(output_path_dir_temp, True)

# COMMAND ----------

# MAGIC %md
# MAGIC Get File Names from Unmerge Path

# COMMAND ----------

file_list = dbutils.fs.ls(input_path_dir)

names=[]

for filename in file_list:
    names.append(filename.name)

# COMMAND ----------

# MAGIC %md
# MAGIC Declare Table Names List

# COMMAND ----------

table_name_arr = ['ODH_AD_CLIENT','ODH_AUTH_USER','ODH_AD_ORGTYPE','ODH_AD_ORG','ODH_AD_USER','ODH_PRACTICE','ODH_M_PRODUCT_CATEGORY','ODH_M_BRAND','ODH_M_PRICELIST','ODH_FIN_PAYMENTMETHOD','ODH_M_OFFER_TYPE','ODH_C_DOCTYPE','ODH_M_PRICELIST_VERSION','ODH_PATIENT_DETAILS','ODH_PATIENT_HORIZON','ODH_HORCUS_REASON','ODH_PRACTICE_FRANCHISE_INFO','ODH_C_BPARTNER','ODH_M_OFFER','ODH_CONTACT','ODH_ADDRESS_HORIZON','ODH_M_PRODUCT','ODH_CUSTOMER_CONSENT_INFO','ODH_CL_SPECIFICATION','ODH_CUSTOMER_IDENTIFIER','ODH_CL_CARE_PLAN','ODH_CL_REWARD_SCHEME','ODH_DATA_PRIVACY_STATEMENT','ODH_SERVICE_GROUP','ODH_PRESCRIPTION','ODH_OBPOS_APPLICATIONS','ODH_OBPOS_APP_CASHUP','ODH_ADVICE_GIVEN','ODH_ADDITIONAL_INFORMATION','ODH_EYE_CHECK','ODH_C_ORDER','ODH_C_ORDERLINE','ODH_ORDER_HEADER','ODH_HORCUS_RECEIPT_HISTORY','ODH_HORCUS_ORDERLINE','ODH_ORDER_LINE','ODH_C_ORDERLINE_OFFER','ODH_FIN_PAYMENT ','ODH_OL_LENS','ODH_HORCUS_ORDERLINE_OFFER','ODH_HORCUS_ORDERLINEPAYMENTS ','ODH_APPOINTMENT_CUSTOMER','ODH_APPOINTMENT_BOOKING']

# COMMAND ----------

# MAGIC %md
# MAGIC Perform Merging of Files from Unmerge Path

# COMMAND ----------

for table_name in table_name_arr:

    for file_name in names:
        
        if table_name == file_name[:-34]:
    
            df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(input_path_dir + table_name + '_2*.csv')

            ## writing the dataframe into a merged file in the temporary path
            df.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').option("quoteAll", "true").save(output_path_dir_temp + timestr + '_' + table_name + '_merged/')

            ##  making a list of all the files inside the temporary path
            merged_file_list = dbutils.fs.ls(output_path_dir_temp + timestr + '_' + table_name + '_merged/')

            ## scanning through the files in the temporary path
            ## if the file name ends with .csv then rename it to the original file name and add 2 values of timestamp as expected file naming convention of ODH
            for file_name in merged_file_list:
                
                if file_name.name.endswith(".csv"):
                
                    dbutils.fs.mv(output_path_dir_temp + timestr + '_' + table_name + '_merged/'+file_name.name,output_path_dir + table_name + '_'+time.strftime("%Y%m%d%H%M%S")+'_'+time.strftime("%Y%m%d%H%M%S") + '.csv')

            break

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Files from Unmerge Path

# COMMAND ----------

for name in names:
    if name[-4:] == '.csv':
        dbutils.fs.rm(input_path_dir + name)  