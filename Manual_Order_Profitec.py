# Databricks notebook source
dbutils.widgets.text("Query_Parameter",'')

# COMMAND ----------

EtlRunlogId=dbutils.widgets.get("Query_Parameter")

# COMMAND ----------

import datetime

try:
    restartflagcount=0
    previousenddate=''
    startextractdate=''
    endextractdate=''
    daterangeduration=0
    restartconfigtab='rpi_egress_db.egress_notebook_logs_columbus'
    dynamicparamstab='rpi_egress_db.egress_dynamic_params'
    msg=''
    getdateval=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("getdateval======="+getdateval)

    restartflagcount = spark.sql("select count(*) from "+restartconfigtab+" where restartflag = 'Y' and projectname = 'Profitec'").collect()[0][0]
    print("restartflagcount===="+str(restartflagcount))
    daterangeduration = spark.sql("select param_value from "+dynamicparamstab+" where project_name='Profitec' and process_name = 'ManualOrdersEgress' and param_key='DateRangeDuration'").collect()[0][0]
    print("daterangeduration======"+str(daterangeduration))

    if restartflagcount > 0:
        restartflagcount = spark.sql("select count(*) from "+restartconfigtab+" where restartflag = 'Y' and projectname = 'Profitec' and feedname like 'ManualOrders%'").collect()[0][0]
        print("restartflagcount===="+str(restartflagcount))

        if restartflagcount == 1:
            print('calling manualorders restart')
            previousenddate = spark.sql("select enddate from "+restartconfigtab+" where projectname = 'Profitec' and restartflag = 'Y' and feedname like 'ManualOrders%'").collect()[0][0]
            startextractdate = str(spark.sql("select date_add('"+previousenddate+"',1)").collect()[0][0])
            endextractdate = str(spark.sql("select date_add('"+previousenddate+"',"+daterangeduration+")").collect()[0][0])
            print('previousenddate===='+previousenddate)
            print('startextractdate===='+startextractdate)
            print('endextractdate===='+endextractdate)

            spark.sql("update "+restartconfigtab+" set restartflag = 'R' where projectname = 'Profitec' and restartflag = 'Y' and feedname like 'ManualOrders%'")
            spark.sql("update "+restartconfigtab+" set activeflag = 'A' where projectname = 'Profitec' and activeflag = 'Y' and restartflag = 'N'")
            spark.sql("update "+restartconfigtab+" set restartflag = 'A' where projectname = 'Profitec' and activeflag = 'N' and restartflag = 'Y'")	
            spark.sql("insert into "+restartconfigtab+" select 'Profitec','ManualOrders_"+getdateval+"','"+startextractdate+"','"+endextractdate+"','P','N'")

            #call the notebook
            dbutils.notebook.run("./Manual_Order_Profitec_Extract",10800,{'EtlRunlogId':EtlRunlogId,'startextractdate':startextractdate,'endextractdate':endextractdate,'getdateval':getdateval})

            spark.sql("update "+restartconfigtab+" set activeflag = 'N' where projectname = 'Profitec' and activeflag = 'P' and restartflag = 'N' and feedname = 'ManualOrders_"+getdateval+"' and startdate = '"+startextractdate+"' and enddate = '"+endextractdate+"'")
            spark.sql("update "+restartconfigtab+" set activeflag = 'Y' where projectname = 'Profitec' and activeflag = 'A' and restartflag = 'N'")
            spark.sql("update "+restartconfigtab+" set restartflag = 'Y' where projectname = 'Profitec' and activeflag = 'N' and restartflag = 'A'")
            spark.sql("update "+restartconfigtab+" set restartflag = 'Y' where projectname = 'Profitec' and restartflag = 'R' and feedname like 'ManualOrders%'")
        
    else:
        restartflagcount = spark.sql("select count(*) from "+restartconfigtab+" where activeflag = 'Y' and restartflag = 'N' and projectname = 'Profitec' and feedname like 'ManualOrders%'").collect()[0][0]
        print("restartflagcount===="+str(restartflagcount))

        if restartflagcount == 1:
            print('calling manualorders')
            previousenddate = spark.sql("select enddate from "+restartconfigtab+" where projectname = 'Profitec' and activeflag = 'Y' and restartflag = 'N' and feedname like 'ManualOrders%'").collect()[0][0]
            startextractdate = str(spark.sql("select date_add('"+previousenddate+"',1)").collect()[0][0])
            endextractdate = str(spark.sql("select date_add('"+previousenddate+"',"+daterangeduration+")").collect()[0][0])
            print('previousenddate===='+previousenddate)
            print('startextractdate===='+startextractdate)
            print('endextractdate===='+endextractdate)

            spark.sql("update "+restartconfigtab+" set activeflag = 'N' where projectname = 'Profitec' and activeflag = 'Y' and restartflag = 'N' and feedname like 'ManualOrders%'")
            spark.sql("update "+restartconfigtab+" set activeflag = 'A' where projectname = 'Profitec' and activeflag = 'Y' and restartflag = 'N'")
            spark.sql("insert into "+restartconfigtab+" select 'Profitec','ManualOrders_"+getdateval+"','"+startextractdate+"','"+endextractdate+"','P','N'")

            #call the notebook
            dbutils.notebook.run("./Manual_Order_Profitec_Extract",10800,{'EtlRunlogId':EtlRunlogId,'startextractdate':startextractdate,'endextractdate':endextractdate,'getdateval':getdateval})

            spark.sql("update "+restartconfigtab+" set activeflag = 'Y' where projectname = 'Profitec' and activeflag = 'P' and restartflag = 'N' and feedname = 'ManualOrders_"+getdateval+"' and startdate = '"+startextractdate+"' and enddate = '"+endextractdate+"'")
            spark.sql("update "+restartconfigtab+" set activeflag = 'Y' where projectname = 'Profitec' and activeflag = 'A' and restartflag = 'N'")
    msg = {"ProcedureStatus" : "0", "ProcedureMessage" : "OK"}
except Exception as e:
    msg = {"ProcedureStatus" : "-1", "ProcedureMessage" : "{Error}".format(e)}


# COMMAND ----------

dbutils.notebook.exit(msg)