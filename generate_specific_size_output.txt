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
dbutils.widgets.dropdown("SourceAssetType", "csv", List(CSV, TXT, PSV), "Source type")
dbutils.widgets.dropdown("outputFileType", "csv", List(CSV, TXT, PSV), "Desired Output File Type")
dbutils.widgets.text("desiredFileSizeOrRows", "1073741824", "Desired output files Bytes")
dbutils.widgets.text("ContainerName", "egresshopper", "ContainerName")
dbutils.widgets.text("StorageAccountEndPoint", "aznednadevdl01.dfs.core.windows.net", "Storage Account EndPoint")

// COMMAND ----------

val ONE = 1
val ONE_HUNDRED = 100
val AssetPath = dbutils.widgets.get("AssetPath")
val AssetName = dbutils.widgets.get("AssetName")
val SourceAssetType = dbutils.widgets.get("SourceAssetType")
val outputFileType = dbutils.widgets.get("outputFileType")
val ContainerName = dbutils.widgets.get("ContainerName")
val StorageAccountEndPoint = dbutils.widgets.get("StorageAccountEndPoint")
val desiredFileSizeOrRows = dbutils.widgets.get("desiredFileSizeOrRows").toLong
val AssetTempPath = AssetName + "_temp"
println(s"desiredFileSizeInMB: ${desiredFileSizeOrRows/1024/1024}")
val rowSizeIncreasePercentage = 0.1

// COMMAND ----------

final val SourceAssetPath = "abfs://" + ContainerName + "@" + StorageAccountEndPoint + AssetPath
final val destinationPath = "abfs://" + ContainerName + "@" + StorageAccountEndPoint + AssetPath + AssetName

// COMMAND ----------

val deltaTableDF = SourceAssetType match {
 case PSV => spark.read.option("delimiter", "|").format(CSV).load(SourceAssetPath+AssetName)
 case _ => spark.read.format(SourceAssetType).load(SourceAssetPath+AssetName)
}

// COMMAND ----------

val countDF = deltaTableDF.count()

if (countDF == 0){
  val Chunk_asset = AssetName.replace("SeqNum", "00000")
  dbutils.fs.mv(SourceAssetPath+AssetName, SourceAssetPath+Chunk_asset)
  println(SourceAssetPath+Chunk_asset)
  dbutils.notebook.exit("[{\"name\":\""+Chunk_asset+"\", \"rows\":0}]")

}


// COMMAND ----------


val deltaTableDF_P = deltaTableDF.persist()
val totalCount = deltaTableDF_P.count()

// COMMAND ----------

//Comuting average size of CSV row
val averageRowSizesInBytes = dbutils.fs.ls(s"${destinationPath}").map{
  case fi: FileInfo if fi.path.contains(s".csv") => {
    val path = fi.path
    val count = spark.read.csv(path).count()
    val sizeInMB = fi.size/(1024*1024)
    println(s"path: $path - count: $count - size: $sizeInMB MB")
    print(s"rowSizeInByte = ${sizeInMB/count}")
    val rowSizeInByte = fi.size/count
    Some(rowSizeInByte + rowSizeInByte*rowSizeIncreasePercentage)
  }
  case fi: FileInfo if fi.path.contains(s".txt") => {
    val path = fi.path
    val count = spark.read.text(path).count()
    val sizeInMB = fi.size/(1024*1024)
    println(s"path: $path - count: $count - size: $sizeInMB MB")
    print(s"rowSizeInByte = ${sizeInMB/count}")
    val rowSizeInByte = fi.size/count
    Some(rowSizeInByte + rowSizeInByte*rowSizeIncreasePercentage)
  }
  case fi: FileInfo if fi.path.contains(s".psv") => {
    val path = fi.path
    val count = spark.read.csv(path).count()
    val sizeInMB = fi.size/(1024*1024)
    println(s"path: $path - count: $count - size: $sizeInMB MB")
    print(s"rowSizeInByte = ${sizeInMB/count}")
    val rowSizeInByte = fi.size/count
    Some(rowSizeInByte + rowSizeInByte*rowSizeIncreasePercentage)
  }
  case _ => Option.empty
}.flatten

// COMMAND ----------

val avgRowSizeInBytes = averageRowSizesInBytes(0)

// COMMAND ----------

//Based on average row size in CSV the total record per file to match size limit is computed
val desiredRecordNumber = (desiredFileSizeOrRows/(avgRowSizeInBytes)).toLong //computing the number of records needed to have a file of the specified dimension

// COMMAND ----------

//To be sure to remain below the size limit an additional partiotion is added
val repartitionTo = (totalCount/desiredRecordNumber).toInt + ONE

// COMMAND ----------

//deltaTableDF_P.repartition(repartitionTo).write.format(outputFileType).save(s"${destinationPath}/csvFiles")
outputFileType match {
 case CSV => deltaTableDF_P.repartition(repartitionTo).write.format(outputFileType).mode("overwrite").save(s"${destinationPath}_temp")
 case TXT => deltaTableDF_P.repartition(repartitionTo).write.format(outputFileType).mode("overwrite").save(s"${destinationPath}_temp")
 case PSV => deltaTableDF_P.repartition(repartitionTo).write.format(CSV).option("sep", "|").mode("overwrite").save(s"${destinationPath}_temp")
 case _ => println("Output not supported")
}

// COMMAND ----------

deltaTableDF_P.unpersist()

// COMMAND ----------

for( fi <- dbutils.fs.ls(destinationPath) if fi.path.contains(s".$outputFileType") && debug) {
  val path = fi.path
  val count = spark.read.csv(path).count()
  val size = fi.size/(1024*1024)
  println(s"path: $path - count: $count - size: $size MB - row size BYTE: ${fi.size/count}")
}

// COMMAND ----------


val chunk_asset = dbutils.notebook.run("./rename_files_size", 300 ,Map("AssetName" -> AssetName, "SourceAssetPath" -> SourceAssetPath, "AssetTempPath" -> s"${destinationPath}_temp", "SourceAssetType" ->SourceAssetType))
dbutils.notebook.exit(chunk_asset)