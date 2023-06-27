# Databricks notebook source
# MAGIC %run "./RPI_Logging_Notebook_Table"

# COMMAND ----------

# MAGIC %run "./RPI_Logging_Notebook_Common_Method"

# COMMAND ----------

dbutils.widgets.text("ProcessStage", "","")
process_stage = dbutils.widgets.get("ProcessStage")

dbutils.widgets.text("EntityName", "","")
entity_name = dbutils.widgets.get("EntityName")

dbutils.widgets.text("BatchId", "","")
batch_id = dbutils.widgets.get("BatchId")

dbutils.widgets.text("SourceName", "","")
source_name = dbutils.widgets.get("SourceName")

dbutils.widgets.text("FeedName", "","")
feed_name = dbutils.widgets.get("FeedName")

dbutils.widgets.text("Status", "","")
status = dbutils.widgets.get("Status")

dbutils.widgets.text("Error", "","")
error_details = dbutils.widgets.get("Error")

dbutils.widgets.text("RunTime", "","")
run_time = dbutils.widgets.get("RunTime")

dbutils.widgets.text("TriggerId", "","")
trigger_id = dbutils.widgets.get("TriggerId")

dbutils.widgets.text("SourceCount", "","")
source_count = dbutils.widgets.get("SourceCount")

dbutils.widgets.text("TargetCount", "","")
target_count = dbutils.widgets.get("TargetCount")

dbutils.widgets.text("Remark", "","")
remark = dbutils.widgets.get("Remark")

dbutils.widgets.text("ETLRunLogID", "","")
etlrunlogid = dbutils.widgets.get("ETLRunLogID")

dbutils.widgets.text("DTCreated", "","")
dtcreated = dbutils.widgets.get("DTCreated")

dbutils.widgets.text("UserCreated", "","")
usercreated = dbutils.widgets.get("UserCreated")

dbutils.widgets.text("SystemOfRecordId", "","")
systemofrecordid = dbutils.widgets.get("SystemOfRecordId")

dbutils.widgets.text("LoadType", "","")
loadtype = dbutils.widgets.get("LoadType")


# COMMAND ----------

import re
def rmove_quotes(string_value):
  REGEX = re.compile(r"[" + r'"' + r"'" + r"]")
  string_value = REGEX.sub('', string_value)
  return string_value


# COMMAND ----------

entity = rmove_quotes(entity_name.replace("[" ,"").replace("]" ,""))
process_stage = rmove_quotes(process_stage)
batch_id = rmove_quotes(batch_id)
source_name = rmove_quotes(source_name)
feed_name = rmove_quotes(feed_name)
status = rmove_quotes(status)
error_details = rmove_quotes(error_details)
run_time = rmove_quotes(run_time)
trigger_id = rmove_quotes(trigger_id)
source_count = rmove_quotes(source_count)
target_count = rmove_quotes(target_count)
remark = rmove_quotes(remark.replace("[" ,"").replace("]" ,""))
etlrunlogid = rmove_quotes(etlrunlogid)
dtcreated = rmove_quotes(dtcreated)
usercreated = rmove_quotes(usercreated)
systemofrecordid = rmove_quotes(systemofrecordid)


# COMMAND ----------

if loadtype=='History' and 'curatestaging' in process_stage.replace(" ", "").lower():
    entity_name=eval(entity_name)
    for file in entity_name:
        FileName=eval(file)
        insert_log(process_stage,FileName[0],batch_id,source_name,feed_name,status,error_details,run_time,trigger_id,FileName[1],FileName[2],remark,etlrunlogid,dtcreated,usercreated,systemofrecordid)

elif loadtype=='Incremental' and'curatestaging' in process_stage.replace(" ", "").lower():
    entity_list = entity.split(",")
    for entity in entity_list:
        insert_log(process_stage, entity, batch_id, source_name, feed_name, status, error_details, run_time, trigger_id, source_count, target_count, remark,etlrunlogid,dtcreated,usercreated,systemofrecordid)    
elif ('curatesynapse' in process_stage.replace(" ", "").lower()):
    target_count = target_count.split(",")[-1].split(":")[-1].replace("}","")
    insert_log(process_stage, entity, batch_id, source_name, feed_name, status, error_details, run_time, trigger_id, source_count, target_count, remark,etlrunlogid,dtcreated,usercreated,systemofrecordid)
elif 'curateadls' in process_stage.replace(" ", "").lower():
    target_count = target_count.split(",")[-1].split(":")[-1].replace("}","")
    insert_log(process_stage, entity, batch_id, source_name, feed_name, status, error_details, run_time, trigger_id, source_count, target_count, remark,etlrunlogid,dtcreated,usercreated,systemofrecordid)
else:
    insert_log(process_stage, entity, batch_id, source_name, feed_name, status, error_details, run_time, trigger_id, source_count, target_count, remark,etlrunlogid,dtcreated,usercreated,systemofrecordid)

# COMMAND ----------

