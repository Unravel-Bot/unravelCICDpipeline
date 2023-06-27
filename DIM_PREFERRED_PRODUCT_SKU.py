# Databricks notebook source
import datetime


# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

--%sql
/*SELECT 
Sourcekey ID_PREFERRED_PRODUCT_SKU, 
EndDate Preferred_End_Date,
StartDate Preferred_Start_Date,
Region as REGION,
PharmacyProductSKUID  AS ID_PRODUCT_SKU, 
PharmacyProductID  AS ID_PRODUCT, 
PreferredType AS Preferred_Type,
current_timestamp as CREATE_DATETIME, 
cast(null as timestamp) as UPDATE_DATETIME 
FROM 
columbus_curation.curateadls_preferredproductsku */

# COMMAND ----------

OutputDF=spark.sql("SELECT distinct  \
nvl(cast(Sourcekey as int),0) as ID_PREFERRED_PRODUCT_SKU,  \
nvl(PharmacyProductID,0)  AS ID_PRODUCT, \
PharmacyProductSKUID  AS ID_PRODUCT_SKU, \
PreferredType AS Preferred_Type, \
EndDate Preferred_End_Date, \
StartDate Preferred_Start_Date, \
Region as REGION, \
current_timestamp as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_preferredproductsku \
")
#OutputDF.count()

# COMMAND ----------

truncate_and_load_synapse (OutputDF,'con_columbus.DIM_PREFERRED_PRODUCT_SKU')

# COMMAND ----------

