# Databricks notebook source
dbutils.widgets.text("Query_Parameter", '')


# COMMAND ----------

tableName = dbutils.widgets.get("Query_Parameter")

# COMMAND ----------

sqlQuery = 'Delete from ' + dbutils.widgets.get("Query_Parameter")
sqlQuery

# COMMAND ----------

sqlTableExtended = 'describe table extended {}'.format(tableName)

# COMMAND ----------

sqlTableDesc = spark.sql(sqlTableExtended).select("col_name","data_type")
sqlTableLocation = sqlTableDesc.filter(sqlTableDesc.col_name=='Location').select("data_type").collect()[0][0]
sqlTableLocation

# COMMAND ----------

msg = ''
try:
    spark.sql(sqlQuery)
    #for i in dbutils.fs.ls(sqlTableLocation):
        #if '_delta_log' not in i[0]:
            #dbutils.fs.rm(i[0],True)
    msg = {"ProcedureStatus" : "0", "ProcedureMessage" : "OK"}
except Exception as e:
    msg = {"ProcedureStatus" : "-1", "ProcedureMessage" : "{}".format(e)}
    


# COMMAND ----------

dbutils.fs.ls(sqlTableLocation)

# COMMAND ----------

dbutils.notebook.exit(msg)