# Databricks notebook source
from pyspark.sql.functions import col
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import os
import requests
from requests.auth import HTTPDigestAuth
import json
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import Row


dbutils.widgets.text("status", '', '')
status = dbutils.widgets.get('status')
dbutils.widgets.text("driver_config_file_name", '', '')
file_name = dbutils.widgets.get('driver_config_file_name')
dbutils.widgets.text("ErrorDetails", '', '')
error_details = dbutils.widgets.get('ErrorDetails')
dbutils.widgets.text("Pipeline_Process_Name", '', '')
Pipeline_Process_Name = dbutils.widgets.get('Pipeline_Process_Name')
print(status)
print(file_name)
print(error_details)
print(Pipeline_Process_Name)


# COMMAND ----------

LogicAppURL = "https://prod-07.northeurope.logic.azure.com:443/workflows/9efc70e9b11d41b28e58dfba2f8c2cff/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=yNZvZ9w7auklBBvbwEAV_vmrSXoOWgJYmZ9L-8LyrFo"
data ={"FileName":file_name,
        "ErrorDetails":error_details,
        "status":status,
        "Pipeline_Process_Name":Pipeline_Process_Name}
myResponse = requests.post(LogicAppURL,params = data)