# Databricks notebook source
#importing libraries
#-----------------------
import sys
import os
import json
import re
from datetime import datetime,timedelta
import uuid
import shutil

# COMMAND ----------

# MAGIC %md
# MAGIC ## IDF SDK-Running Required modules

# COMMAND ----------

# MAGIC %run ../commons/CommonBase

# COMMAND ----------

# MAGIC %run ../commons/IDF_Python_Module

# COMMAND ----------

# MAGIC %run ../commons/IDF_Python_DG_Module

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config

# COMMAND ----------

#Loading config json and common setup
#----------------------------------------------
dbutils.widgets.text("config_file_name", '', '')
config_file_name = dbutils.widgets.get('config_file_name')

dbutils.widgets.text("config_file_path", '', '')
config_file_path = dbutils.widgets.get('config_file_path')

dbutils.widgets.text("common_setup_file_path", '', '')
common_setup_file_path = dbutils.widgets.get('common_setup_file_path')

dbutils.widgets.text("common_setup_file_name", '', '')
common_setup_file_name = dbutils.widgets.get('common_setup_file_name')

config = json.loads(open(f"{config_file_path}/{config_file_name}",'r').read())
#config = json.loads(open(f"/dbfs/mnt/wbaidf/configs/collibra/{config_file_path}",'r').read())
collibra_setup = json.loads(open(f"{common_setup_file_path}/{common_setup_file_name}",'r').read())
#collibra_setup = json.loads(open(f"/dbfs/mnt/wbaidf/configs/collibra-setup.json",'r').read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common

# COMMAND ----------

#Fetching variable from widget and storing into variable
#----------------------------------------------------------
dbutils.widgets.text("collibra_username", '', '')
collibra_username = dbutils.widgets.get('collibra_username') 
#print(collibra_username)

dbutils.widgets.text("collibra_password", '', '')
collibra_password = dbutils.widgets.get('collibra_password') 
#print(collibra_password)

#dbutils.widgets.text("domain_id", '', '')
#domain_id = dbutils.widgets.get('domain_id')
#print("domain id:",domain_id)# 97fbbfa3-462c-47f0-a5f9-0e8e583f982c

collibra_hostname = collibra_setup.get("collibra_hostname")

#print(collibra_hostname)

ASSET_REGISTRY = {}

#dbutils.widgets.remove("domain_id")

CURRENT_PARTITION_DATE = generateCurrentDate_fn()  # else /

#Create these assets
database_type = collibra_setup.get("database_type")
#print(database_type)
table_type = collibra_setup.get("table_type")
#print(table_type)
col_type = collibra_setup.get("col_type")
#print(col_type)


# COMMAND ----------

#Creating batch id parameter and getting column names from output for DF Json
#----------------------------------------------------------------
dbutils.widgets.text("batch_id", '', '')
batch_id = dbutils.widgets.get('batch_id')
if config.get("config_type","").lower() == "df":
  v=config.get('data_destination')
  df_path=v[0].get('type_specific_details').get('destination_path')
  #print(df_path)
  if v[0].get('data_type')=='PARQUET':
    try:
      file_existance=len(dbutils.fs.ls(df_path+"/"+batch_id))
    except:
      file_existance=0
    #print(file_existance)
    if file_existance==0:
      dbutils.notebook.exit("Output parquet file is not found")
    else:
      print("Output parquet file found")
      df=spark.read.parquet(df_path+"/"+batch_id)
      df_column=df.columns
    
  elif v[0].get('data_type')=="DELTA":
    try:
      file_existance=len(dbutils.fs.ls(df_path))
    except:
      file_existance=0
    if file_existance==0:
      dbutils.notebook.exit("Output delta file is not found")
    else:
      print("Output delta file found")
      df=spark.read.format('delta').load(df_path)
      df_column=df.columns



# COMMAND ----------

# MAGIC %md
# MAGIC ### Code (df config_type)

# COMMAND ----------

#Creating Assets,Attributes and relations for datafoundation
#------------------------------------------------------------
if config.get("config_type","").lower() == "df":
  schema = config.get("schema",{})
  for source in config.get("data_destination",{}):
    data_source = source.get("type_specific_details",{})
    #master_asset_id = data_source.get("destination_path").replace("/","_").replace("s3://","")
    #database_name = "_".join(data_source.get("destination_path").split("/")[-4:-2])
    #table_name = "_".join(data_source.get("destination_path").split("/")[-2:])
    database_name = "_".join(data_source.get("destination_path").split("/")[2:4])
    table_name = "_".join(data_source.get("destination_path").split("/")[4:])
    domain_id = collibra_setup.get("adls_standard_domain_id")
    master_asset_id=database_name+' > '+table_name
    #=====================================================
    derived_col=config.get("rules_engine",{}).get("derived_mapping",{})
    #=====================================================
    #-----Creating All Aseets---------------
    json_payload = createAssetsInCollibra_fn_new(domain_id, master_asset_id, database_name, table_name, df_column)

    for asset  in json_payload:
      callPOSTAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", asset)
    #for

    if ASSET_REGISTRY.get("{database_name}".format(database_name = database_name)) == None:
      #print(callGETAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", database_name))
      callGETAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", database_name,domain_id)

    if ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name, table_name = table_name)) == None:
      callGETAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", "{database_name} > {table_name}".format(database_name = database_name, table_name = table_name),domain_id)

    #----create relation between DB and table----
    json_payload = {"targetId": ASSET_REGISTRY.get(database_name), "sourceId": ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name, table_name = table_name)), "typeId": collibra_setup.get("relation_type_rule_DB2Table")}
    callPOSTAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/relations", json_payload)
    #----Creating relation between Table and Columns----
    column_list = []
#==============================================
    ASSET_REGISTRY={}
    callGETAPI_ASSET_DETAIL_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", domain_id)
    column_asset=ASSET_REGISTRY
    column_ls=[]
    for col in df_column:
      col_name=master_asset_id+' > '+col
      column_ls+=[col_name]
      asset_dict=dict()
    for k,v in column_asset.items():
      if k in column_ls:
        asset_dict[k]=v
#================================================   
    for k,v in asset_dict.items():
      #if k not in ["{database_name} > {table_name}".format(database_name = database_name, table_name = table_name), database_name]:
      column_list.append(v)
    #for k,v in asset_dict.items():
      #if k not in ["{database_name} > {table_name}".format(database_name = database_name, table_name = table_name), database_name]:
        #column_list.append(v)

    tmp = createRelations4AssetsInCollibra_fn_reverse([ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name, table_name = table_name))], collibra_setup.get("relation_type_rule_Table2Col"))
    

    tmp["relatedAssetIds"] = [col for col in column_list if col != None]
    callPutAPI_fn( ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets/{assetId}/relations".format(assetId=ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name,table_name = table_name))), tmp)    

# COMMAND ----------

if config.get("config_type","").lower() == "etl":
  cards = config.get("cards",[])
  for i in cards:
    if i.get('card_type')=='write_dataset':
      if i.get('mode')=='OVERWRITE':
        datapath=i.get('data_path')
      else:
        datapath=i.get('data_path')+"/"+batch_id
      try:
        file_existance=len(dbutils.fs.ls(datapath))
      except:
        file_existance=0
      #print(file_existance)
      if file_existance==0:
        dbutils.notebook.exit("Output parquet file is not found")
      else:
        print("Output parquet file found")
        df=spark.read.parquet(datapath)
        df_column=df.columns   
        print(df_column)
        print(datapath)
    elif i.get('card_type')=='write_delta':
      datapath2=i.get('data_path')
      try:
        file_existance=len(dbutils.fs.ls(datapath2))
      except:
        file_existance=0
      #print(file_existance)
      if file_existance==0:
        dbutils.notebook.exit("Output delta file is not found")
      else:
        print("Output delta file found")
        df=spark.read.format('delta').load(datapath2)
        df_column=df.columns       
        print(df_column)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Code (etl config_type)

# COMMAND ----------

# Functions to assign database_name, table_name, and master_asset_id for ETL cards
#----------------------------------------------------------------------------------
def assignDatabaseName(card_type):
  if "snowflake" in card_type.lower() or "synapse" in card_type.lower():
    for source in config.get("data_source",{}):
      data_source = source.get("type_specific_details",{})
      return data_source.get("database_name")
  elif "delta" in card_type.lower() or "dataset" in card_type.lower():
    return "_".join(card['data_path'].split("/")[2:4])

def assignTableName(card_type):
  if "snowflake" in card_type.lower():
    return card['table_name']
  elif "synapse" in card_type.lower():
    return card['output_table']
  elif "delta" in card_type.lower() or "dataset" in card_type.lower():
    return "_".join(card['data_path'].split("/")[4:])

def assignMasterAssetId(card_type, db_name, table_name):
  if "snowflake" in card_type.lower() or "synapse" in card_type.lower():
    return (db_name + "_" + table_name)
  elif "delta" in card_type.lower() or "dataset" in card_type.lower():
    return card['data_path'].replace("/","_").replace("s3://","")
  
#Function to determine domain_id for ETL (all df will be the same)
def determineDomainId(card_type):
  if "snowflake" in card_type.lower():
    return collibra_setup.get("snowflake_domain_id")
  elif "synapse" in card_type.lower():
    return collibra_setup.get("synapse_domain_id")
  elif "delta" in card_type.lower() or "dataset" in card_type.lower():
    return collibra_setup.get("adls_curated_domain_id")

# COMMAND ----------

#Craeting Assets,Realtion for ETL Json
#-------------------------------------
if config.get("config_type","").lower() == "etl":
  cards = config.get("cards",[])
  
  for card in cards:
    # ALL writes, could change if else to take 4 types it can accept (or change function to allow all write card in the dataset/delta section)
    if "write" in card['card_type'].lower():
      database_name = assignDatabaseName(card['card_type'])
      table_name = assignTableName(card['card_type'])
      #master_asset_id = assignMasterAssetId(card['card_type'], database_name, table_name)
      master_asset_id=database_name+' > '+table_name
      domain_id = determineDomainId(card['card_type'])
    
      # Create the database and table attributes in collibra
      json_payload = createAssetsInCollibraETL_fn_new(domain_id, master_asset_id, database_name, table_name,df_column)

      for asset in json_payload:
        callPOSTAPI_fn( ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", asset)
    
      if ASSET_REGISTRY.get("{database_name}".format(database_name = database_name)) == None:
        callGETAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", database_name,domain_id)

      if ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name, table_name = table_name)) == None:
        callGETAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", "{database_name} > {table_name}".format(database_name = database_name, table_name = table_name),domain_id)

      json_payload = createAttributes4AssetsInCollibra_fn(ASSET_REGISTRY, master_asset_id, cards)
      for asset  in json_payload:
        assetId = asset.get("assetId")
        del asset['assetId']
        callPutAPI_fn( ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets/{assetId}/attributes".format(assetId=assetId), asset)

      # Create a Column Type "transformation_logic" and add all card information as a note
#========================================================      
#      COLUMN_BASED_TRANSFORMATION_MAP = {}
#      for card in config.get("cards",[]):
#        col_name = "transformation_logic"   
        #col_name ="Test_columns"
#        json_payload = createIndivisualAssetInCollibra_fn(domain_id, master_asset_id, col_name)          
#        callPOSTAPI_fn( ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", json_payload)
#        asset = createIndivisualAttributes4AssetsInCollibra_fn(ASSET_REGISTRY, master_asset_id, col_name, 'attribute_transformation_logic', card)
#        assetId = asset.get("assetId")
#        del asset['assetId']
#        if assetId in COLUMN_BASED_TRANSFORMATION_MAP:
#          COLUMN_BASED_TRANSFORMATION_MAP.get(assetId).append(asset)
#        else:
#          COLUMN_BASED_TRANSFORMATION_MAP[assetId] = [asset]         
      
      #==================
#      json_payload = {"sourceId": assetId, "targetId": ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name, table_name = table_name)), "typeId": collibra_setup.get("relation_type_rule_Table2Col")}
#      callPOSTAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/relations", json_payload)
      #=================
      # Table Attributes
#      card.update({"tags": config.get("dataset_tags","NA")})
#      callPutAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets/{assetId}/attributes".format(assetId=ASSET_REGISTRY.get("#{database_name} > {table_name}".format(database_name = database_name, table_name = table_name))), convertJSON2HTML_fn(card))
#      for assetId, card in COLUMN_BASED_TRANSFORMATION_MAP.items():
#        rule_value = """<table border="5" width ="100%">"""
#        payload = {'typeId':card[0].get('typeId')}
#        for x in card:
#          rule_value = rule_value + """<tr style="background-color:#FFFFE0"><td>""" + formatJSON2HTMLTable_fn(x.get('value'))+"""</td></tr>"""
#          payload['value'] = rule_value
#          callPutAPI_fn( ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets/{assetId}/attributes".format(assetId=assetId), payload)
#        rule_value = rule_value + """</table>"""
#========================================================================          
      # For any card_type = read, get the information from Collibra if it exists and create lineage
      # If it does not exist, add it based on config file and create lineage to write card table
      """"for card in config.get("cards",[]):
        if "read" in card['card_type']:
          database_name2 = assignDatabaseName(card['card_type'])
          table_name2 = assignTableName(card['card_type'])
          master_asset_id2 = assignMasterAssetId(card['card_type'], database_name, table_name)

          if ASSET_REGISTRY.get("{database_name}".format(database_name = database_name2)) == None:
            try:
              callGETAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", database_name2,domain_id)
              print("Hello")
            #except: IndexError
            except:
              json_payload = createAssetsInCollibraETL_fn(domain_id, master_asset_id2, database_name2, table_name2)
              #print("33333333333333333333333333333")

              for asset  in json_payload:
                callPOSTAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", asset)

          if ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name2, table_name = table_name2)) == None:
            try:
              callGETAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", "{database_name} > {table_name}".format(database_name = database_name2, table_name = table_name2),domain_id)
            #except: IndexError
            except:
              json_payload = createAssetsInCollibraETL_fn(domain_id, master_asset_id2, database_name2, table_name2)

              for asset  in json_payload:
                callPOSTAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", asset)
          
          # Create relationships in Collibra for the read card types that do not exist in Collibra domain
          #----------------------------------------------------------------------------------------------
          json_payload = {"targetId": ASSET_REGISTRY.get(database_name2), "sourceId": ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name2, table_name = table_name2)), "typeId": collibra_setup.get("relation_type_rule_DB2Table") }
          callPOSTAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/relations", json_payload)

          if "delta" in card['card_type'].lower() or "dataset" in card['card_type'].lower():
            ASSET_REGISTRY = {key:val for key, val in ASSET_REGISTRY.items() if key != (database_name2)}"""
      
      # Create relationships in Collibra   
      #---------------------------------
      json_payload = {"targetId": ASSET_REGISTRY.get(database_name), "sourceId": ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name, table_name = table_name)), "typeId": collibra_setup.get("relation_type_rule_DB2Table")}
      callPOSTAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/relations", json_payload)        
      column_list = []
#=====================================
      ASSET_REGISTRY={}
      callGETAPI_ASSET_DETAIL_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", domain_id)
      column_asset=ASSET_REGISTRY
      column_ls=[]
      for col in df_column:
        col_name=master_asset_id+' > '+col
        column_ls+=[col_name]
        asset_dict=dict()
      for k,v in column_asset.items():
        if k in column_ls:
          asset_dict[k]=v
  #================================================   
      for k,v in asset_dict.items():
        #if k not in ["{database_name} > {table_name}".format(database_name = database_name, table_name = table_name), database_name]:
        column_list.append(v)
      #for k,v in asset_dict.items():
        #if k not in ["{database_name} > {table_name}".format(database_name = database_name, table_name = table_name), database_name]:
          #column_list.append(v)

      tmp = createRelations4AssetsInCollibra_fn_reverse([ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name, table_name = table_name))], collibra_setup.get("relation_type_rule_Table2Col"))


      tmp["relatedAssetIds"] = [col for col in column_list if col != None]
      callPutAPI_fn( ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets/{assetId}/relations".format(assetId=ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name,table_name = table_name))), tmp) 
      

# COMMAND ----------

#Creating relation between Standard and curated table
#-----------------------------------------------------
if config.get("config_type","").lower() == "etl":
  etl_table_id=ASSET_REGISTRY.get("{database_name} > {table_name}".format(database_name = database_name, table_name = table_name))
  #print(etl_table_id)
  for card in config.get("cards",[]):
    if "read" in card['card_type'] and card['card_type'].lower()!="read_table":
      gg=card.get("data_path")
      database_name = "_".join(gg.split("/")[2:4])
      table_name = "_".join(gg.split("/")[4:])
      #table_name = "_".join(gg.split("/")[2:])
      table_name=database_name+' > '+table_name
      #print(table_name)
      ASSET_REGISTRY={}
      domain_id =collibra_setup.get("adls_standard_domain_id")
      callGETAPI_ASSET_DETAIL_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/assets", domain_id)
      df_table_id=ASSET_REGISTRY.get(table_name) 
      #print(df_table_id)
      json_payload2 = {"sourceId": df_table_id, "targetId": etl_table_id, "typeId": collibra_setup.get("relation_type_rule_ETL_Table2Tab")}
      #print(json_payload2)
      callPOSTAPI_fn(ASSET_REGISTRY, collibra_hostname, collibra_username, collibra_password, "/relations", json_payload2)