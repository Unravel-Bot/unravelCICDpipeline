// Databricks notebook source
// DBTITLE 1,Import FileInfo
import com.databricks.backend.daemon.dbutils.FileInfo
val debug = false

// COMMAND ----------

// DBTITLE 1,Define asset type variables
val CSV = "csv"
val TXT = "text"
val PSV = "psv"

// COMMAND ----------

dbutils.widgets.removeAll
dbutils.widgets.text("AssetPath", "dbfs:/mnt/...", "Asset Path")
dbutils.widgets.text("AssetName", "asset.csv", "Source Asset Name")
dbutils.widgets.dropdown("SourceAssetType", "csv", List(CSV, TXT, PSV), "Source Asset type")
dbutils.widgets.dropdown("outputFileType", "csv", List(CSV, TXT, PSV), "Output Asset Type")
dbutils.widgets.text("desiredFileSizeOrRows", "100000", "Desired output files Rows")
dbutils.widgets.text("ContainerName", "egresshopper", "ContainerName")
dbutils.widgets.text("StorageAccountEndPoint", "aznednadevdl01.dfs.core.windows.net", "Storage Account EndPoint")

// COMMAND ----------

val AssetPath = dbutils.widgets.get("AssetPath")
val AssetName = dbutils.widgets.get("AssetName")
val SourceAssetType = dbutils.widgets.get("SourceAssetType")
val outputFileType = dbutils.widgets.get("outputFileType")
val ContainerName = dbutils.widgets.get("ContainerName")
val StorageAccountEndPoint = dbutils.widgets.get("StorageAccountEndPoint")
val desiredFileSizeOrRows = dbutils.widgets.get("desiredFileSizeOrRows").toLong
val AssetTempPath = AssetName + "_temp"
final val rowSizeIncreasePercentage = 0.1

// COMMAND ----------

val SourceAssetPath = "abfs://" + ContainerName + "@" + StorageAccountEndPoint + AssetPath
val destinationPath = "abfs://" + ContainerName + "@" + StorageAccountEndPoint + AssetPath + AssetName

// COMMAND ----------

val deltaTableDF = spark.read.format(SourceAssetType).load(SourceAssetPath+AssetName)
val countDF = deltaTableDF.count()

// COMMAND ----------

if (countDF == 0){
  val Chunk_asset = AssetName.replace("SeqNum", "00000")
  dbutils.fs.mv(SourceAssetPath+AssetName, SourceAssetPath+Chunk_asset)
  dbutils.notebook.exit("[{\"name\":\""+Chunk_asset+"\", \"rows\":0}]")
} 

// COMMAND ----------

//deltaTableDF_P.repartition(repartitionTo).write.format(outputFileType).save(s"${destinationPath}/csvFiles")
outputFileType match {
 case CSV => deltaTableDF.repartition(1).write.option("maxRecordsPerFile", desiredFileSizeOrRows).mode("overwrite").format(outputFileType).save(s"${destinationPath}_temp")
 case TXT => deltaTableDF.repartition(1).write.option("maxRecordsPerFile", desiredFileSizeOrRows).mode("overwrite").format(outputFileType).save(s"${destinationPath}_temp")
 case PSV => deltaTableDF.repartition(1).write.option("maxRecordsPerFile", desiredFileSizeOrRows).mode("overwrite").format(outputFileType).save(s"${destinationPath}_temp")
 case _ => println("Output not supported")
}

// COMMAND ----------

val chunk_asset = dbutils.notebook.run("./rename_files_rows", 300 ,Map("AssetName" -> AssetName, "SourceAssetPath" -> SourceAssetPath, "AssetTempPath" -> s"${destinationPath}_temp", "SourceAssetType" ->SourceAssetType))
dbutils.notebook.exit(chunk_asset)