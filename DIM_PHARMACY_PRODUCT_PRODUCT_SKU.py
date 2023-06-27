# Databricks notebook source
import datetime


# COMMAND ----------

# MAGIC %run "../../Common/Reading_Writing_Synapse"

# COMMAND ----------

OutputDF=spark.sql("SELECT distinct \
cast(Sourcekey as int) ID_PRODUCT_PRODUCT_SKU, \
PharmacyProductID  AS ID_PRODUCT, \
PharmacyProductSKUID  AS ID_PRODUCT_SKU, \
IsPrimarySKUIndicator AS Primary_sku_indicator, \
current_timestamp as CREATE_DATETIME, \
cast(null as timestamp) as UPDATE_DATETIME \
FROM columbus_curation.curateadls_productproductsku \
")

# COMMAND ----------

truncate_and_load_synapse (OutputDF,'con_columbus.DIM_PHARMACY_PRODUCT_PRODUCT_SKU')