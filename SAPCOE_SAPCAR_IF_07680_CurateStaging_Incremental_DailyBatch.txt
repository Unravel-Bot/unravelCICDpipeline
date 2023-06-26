# Databricks notebook source
#import required libraries

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

#Get pipeline parameters
dbutils.widgets.text("RunDate", '', '')
RunDate = dbutils.widgets.get('RunDate') 

dbutils.widgets.text("RunTime", '', '')
RunTime = dbutils.widgets.get('RunTime') 

dbutils.widgets.text("SourcePath", '', '')
SourcePath = dbutils.widgets.get('SourcePath') 

dbutils.widgets.text("DestinationPath", '', '')
DestinationPath = dbutils.widgets.get('DestinationPath')

dbutils.widgets.text("FileList", "","")
FileList = dbutils.widgets.get("FileList")

# COMMAND ----------

#dynamic delta table variable
deltaTable='SAPCAR_Incremental.SourceDF_'+RunDate+RunTime

#source file location - this will be wrangled location
source_path="/mnt/idf-cleansed/"+SourcePath+"/"
FileList=eval(FileList)

SourceFileCount=len(FileList)
#create list of file path
SourceList=[]
for file in FileList:
  SourceList.append(source_path+file) #source path + filename 
  
SourceList=str(SourceList)[1:-1].replace("'","").replace(" ","")  #replacing string character and space
#print(SourceList)

# COMMAND ----------

#define destination parametrs - this will be curatestaging location
destinationDetails=[
        {
			"dataframeName":"df_Transaction",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/Transaction/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_TransactionLineItem",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/TransactionLineItem/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_TransactionAdjustment",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/TransactionAdjustment/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_TransactionLineItemAdjustment",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/TransactionLineItemAdjustment/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_TransactionLoyaltyAccount",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/TransactionLoyaltyAccount/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_LoyaltyAccountEarning",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/LoyaltyAccountEarning/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_GiftCardTransaction",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/GiftCardTransaction/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_TransactionPromotion",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/TransactionPromotion/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_TransactionPayment",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/TransactionPayment/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_Payment",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/Payment/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_TransactionCoupon",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/TransactionCoupon/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_ETopupEVoucher",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/ETopupEVoucher/Incremental/",
            "mode": "append"
        },
		{
			"dataframeName":"df_TransactionCreditClaim",
            "destination_path": "/SAPCOE/SAPCAR/IF_07680/TransactionCreditClaim/Incremental/",
            "mode": "append"
        }
    ]

# COMMAND ----------

# MAGIC %sql
# MAGIC create Database if not exists SAPCAR_Incremental

# COMMAND ----------

#schema defenition

sourceSchema=StructType([
  StructField("Attribute0",StringType(),True),
  StructField("recordtype",StringType(),True),
  StructField("Attribute1",StringType(),True),
  StructField("Attribute2",StringType(),True),
  StructField("Attribute3",StringType(),True),
  StructField("Attribute4",StringType(),True),
  StructField("Attribute5",StringType(),True),
  StructField("Attribute6",StringType(),True),
  StructField("Attribute7",StringType(),True),
  StructField("Attribute8",StringType(),True),
  StructField("Attribute9",StringType(),True),
  StructField("Attribute10",StringType(),True),
  StructField("Attribute11",StringType(),True),
  StructField("Attribute12",StringType(),True),
  StructField("Attribute13",StringType(),True),
  StructField("Attribute14",StringType(),True),
  StructField("Attribute15",StringType(),True),
  StructField("Attribute16",StringType(),True),
  StructField("Attribute17",StringType(),True),
  StructField("Attribute18",StringType(),True),
  StructField("Attribute19",StringType(),True),
  StructField("Attribute20",StringType(),True),
  StructField("Attribute21",StringType(),True),
  StructField("Attribute22",StringType(),True),
  StructField("Attribute23",StringType(),True),
  StructField("Attribute24",StringType(),True),
  StructField("Attribute25",StringType(),True),
  StructField("Attribute26",StringType(),True),
  StructField("Attribute27",StringType(),True),
  StructField("Attribute28",StringType(),True),
  StructField("Attribute29",StringType(),True)
])

# COMMAND ----------

#creating source datafrmae by reading list of source path
sourceDF=spark.read.format('csv').option("delimiter","|").schema(sourceSchema).load(SourceList.split(','))

sourceDF=sourceDF.drop("Attribute0")
#display(sourceDF)

# COMMAND ----------

#adding unique id column
sourceDF=sourceDF.withColumn("sequence_nbr",monotonically_increasing_id())

# COMMAND ----------

#add surrogate keys

sourceDF=sourceDF.withColumn("TRANSACTIONID",when(col('recordtype')=='H',concat_ws('',col("Attribute1"),col("Attribute4"),col("Attribute6"),col("Attribute5"))))

sourceDF=sourceDF.withColumn("TRANSACTIONID",last(col("TRANSACTIONID"),ignorenulls=True).over(Window.orderBy("sequence_nbr")))

sourceDF=sourceDF.withColumn("TRANSACTIONLINEITEMID",when(col('recordtype')=='L',concat_ws('',col("TRANSACTIONID"),col("recordtype"),col("Attribute1"))))

sourceDF=sourceDF.withColumn("TRANSACTIONLINEITEMID",last(col("TRANSACTIONLINEITEMID"),ignorenulls=True).over(Window.orderBy("sequence_nbr")))

sourceDF=sourceDF.withColumn("TRANSLINEADJUSTMENTID",when(col('recordtype')=='LD',concat_ws('',col("TRANSACTIONLINEITEMID"),col("recordtype"),col("Attribute1"))))

sourceDF=sourceDF.withColumn("TRANSLINEADJUSTMENTID",last(col("TRANSLINEADJUSTMENTID"),ignorenulls=True).over(Window.orderBy("sequence_nbr")))

sourceDF=sourceDF.withColumn("TRANADJUSTMENTID",when(col('recordtype')=='HD',concat_ws('',col("TRANSACTIONID"),col("recordtype"),col("Attribute6"))))

sourceDF=sourceDF.withColumn("TRANADJUSTMENTID",last(col("TRANADJUSTMENTID"),ignorenulls=True).over(Window.orderBy("sequence_nbr")))

sourceDF=sourceDF.withColumn("PAYMENTID",when(col('recordtype')=='TR',concat_ws('',col("TRANSACTIONID"),col("recordtype"),col("Attribute1"))))

sourceDF=sourceDF.withColumn("PAYMENTID",last(col("PAYMENTID"),ignorenulls=True).over(Window.orderBy("sequence_nbr")))

sourceDF=sourceDF.withColumn("TRANSACTIONDATE",when(col('recordtype')=='H',col("Attribute2")))

sourceDF=sourceDF.withColumn("TRANSACTIONDATE",last(col("TRANSACTIONDATE"),ignorenulls=True).over(Window.orderBy("sequence_nbr")))

#display(sourceDF)

# COMMAND ----------

#create temp delta table for better performance
sourceDF.write.mode("overwrite").format("delta").saveAsTable(deltaTable)

# COMMAND ----------

#create 39 entity dataframes from delta table.

df_H=spark.sql("select RECORDTYPE, Attribute1 as RETAILSTOREID, Attribute2 as BUSINESSDAYDATE, Attribute3 as TRANSTYPECODE, Attribute4 as WORKSTATIONID, Attribute5 as TRANSNUMBER, Attribute6 as BEGINTIMESTAMP, Attribute7 as ENDDATETIMESTAMP, Attribute8 as OPERATORQUAL, Attribute9 as OPERATORID, Attribute10 as TRANSCURRENCY, Attribute11 as PARTNERQUALIFIER, Attribute12 as PARTNERID, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='H'")

df_0=spark.sql("select RECORDTYPE, Attribute1 as MGRKEYUSD, Attribute2 as TRNNOSETFG, Attribute3 as RINGTIME, Attribute4 as TNDRTIME, Attribute5 as NONSLSTIME, Attribute6 as INACTIVTIM, Attribute7 as BASECURR, Attribute8 as OPEMPID, Attribute9 as OPEMPFLG, Attribute10 as OPEMPGRPCD, Attribute11 as SUPRID, Attribute12 as SUPREMPID, Attribute13 as SUPREMPFLG, Attribute14 as SUPREMPGRP, Attribute15 as GRPLUS, Attribute16 as GRMINUS, Attribute17 as NETCASH, Attribute18 as NETNONCASH, Attribute19 as NONCSHNEGF, Attribute20 as NETTDRTOR, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='0'")

df_94=spark.sql("select RECORDTYPE, Attribute1 as TRANSUSPND, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='94'")

df_93=spark.sql("select RECORDTYPE, Attribute1 as TRANRETSUS, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='93'")

df_81=spark.sql("select RECORDTYPE, Attribute1 as COUNTTILL, Attribute2 as COUNTAMNT, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='81'")

df_36=spark.sql("select RECORDTYPE, Attribute1 as STFDICCARD, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='36'")

df_60=spark.sql("select RECORDTYPE, Attribute1 as ORGRECTPRS, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='60'")

df_1=spark.sql("select RECORDTYPE, Attribute1 as ORGTRANNO, Attribute2 as ORGTILLNO, Attribute3 as ORGOPID, Attribute4 as ORDDATETRN, Attribute5 as ORGTIMETRN, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='1'")

df_50=spark.sql("select RECORDTYPE, Attribute1 as COMORDNUM, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='50'")

df_FT=spark.sql("select RECORDTYPE, Attribute1 as FITYPECODE, Attribute2 as AMOUNT, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='FT'")

df_33=spark.sql("select RECORDTYPE, Attribute1 as CHISSTILL, Attribute2 as SUNDREASN, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='33'")

df_41=spark.sql("select RECORDTYPE, Attribute1 as PROMOID, Attribute2 as PRDESC, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='41'")

df_LC=spark.sql("select RECORDTYPE, Attribute1 as LOYNUMBER, Attribute2 as CUSTCARDNUMBER, Attribute3 as LOYPROGRAMID, Attribute4 as LOYPNTSAWARDED, Attribute5 as ELIGIBLEAMOUNT, Attribute6 as ELIGIBLEQUANTITY, Attribute7 as ELIGIBLEQUANUOM, Attribute8 as LOYPNTSREDEEMED, Attribute9 as LOYPNTSTOTAL, Attribute10 as CUSTCARDHOLDNAME, Attribute11 as CUSTCARDTYPE, Attribute12 as CUSTCARDVALIDFR, Attribute13 as CUSTCARDVALIDTO, TRANSACTIONID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='LC'")

df_38=spark.sql("select RECORDTYPE, Attribute1 as COUNTCRDUS, Attribute2 as LREASNCODE, Attribute3 as LPVALIDCOD, Attribute4 as ADCARD_PRESENTATION_METHOD, Attribute5 as LVALIDBARC, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='38'")

df_HD=spark.sql("select RECORDTYPE, Attribute1 as DISCNUMBER, Attribute2 as DISCTYPECODE, Attribute3 as DISCREASONCODE, Attribute4 as REDUCTIONAMOUNT, Attribute5 as DISCOUNTID, Attribute6 as BONUSBUYID, TRANSACTIONID, TRANADJUSTMENTID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='HD'")

df_20=spark.sql("select RECORDTYPE, Attribute1 as QUALREASN, Attribute2 as QUALAMNT, Attribute3 as QUALLISTX, Attribute4 as QUALLISTA, Attribute5 as QUALLISTB, Attribute6 as QUALLISTC, Attribute7 as QUALBRAND, Attribute8 as QUALREDEEM, Attribute9 as QUALEXEMPT, Attribute10 as QUALNUMBER, Attribute11 as EXTRAADPNT, TRANSACTIONID, TRANADJUSTMENTID from "+deltaTable+" where RECORDTYPE='20'")

df_34=spark.sql("select RECORDTYPE, Attribute1 as REWARDAMNT, TRANADJUSTMENTID from "+deltaTable+" where RECORDTYPE='34'")

df_39 = spark.sql("select RECORDTYPE, Attribute1 as CPONBARCOD, Attribute2 as CPONAMT, Attribute3 as CPONSCNKEY, Attribute4 as CPONRETFLG, Attribute5 as CPONVODFLG, Attribute6 as CPONQTYFLG, Attribute7 as CPONPNTFLG, Attribute8 as CPONOVRRID, TRANSACTIONID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='39'")

df_L=spark.sql("select RECORDTYPE, Attribute1 as RETAILNUMBER, Attribute2 as RETAILTYPECODE, Attribute3 as RETAILREASONCODE, Attribute4 as ITEMIDQUALIFIER, Attribute5 as ITEMID, Attribute6 as RETAILQUANTITY, Attribute7 as SALESUOM, Attribute8 as SALESAMOUNT, Attribute9 as NORMALSALESAMOUNT, Attribute10 as ENTRYMETHODCODE, TRANSACTIONID, TRANSACTIONLINEITEMID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='L'")

df_5=spark.sql("select RECORDTYPE, Attribute1 as DUMMYART, TRANSACTIONLINEITEMID from "+deltaTable+" where RECORDTYPE='5'")

df_6=spark.sql("select RECORDTYPE, Attribute1 as ITMBASEPNT, Attribute2 as BOOTSBRAND, Attribute3 as ELGREDEMP, Attribute4 as EXPTDISC, Attribute5 as EXPTPTAQST, Attribute6 as EXTPRICE, Attribute7 as ITEMVOID, Attribute8 as ITMMVNTFLG, Attribute9 as ITMREFUND, Attribute10 as ITMSOLDOFL, Attribute11 as LOCPRCFLG, Attribute12 as QTYENTRFLG, Attribute13 as NONPLUFLG, Attribute14 as PRCOVRRDN, Attribute15 as RFNDFL_COM, TRANSACTIONLINEITEMID from "+deltaTable+" where RECORDTYPE='6'")

df_55=spark.sql("select RECORDTYPE, Attribute1 as ORGPRICE, TRANSACTIONLINEITEMID from "+deltaTable+" where RECORDTYPE='55'")

df_LD=spark.sql("select RECORDTYPE, Attribute1 as DISCNUMBER, Attribute2 as DISCTYPECODE, Attribute3 as DISCREASONCODE, Attribute4 as REDUCTIONAMOUNT, Attribute5 as DISCOUNTID, Attribute6 as BONUSBUYID, TRANSACTIONID, TRANSACTIONLINEITEMID, TRANSLINEADJUSTMENTID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='LD'")

df_10 = spark.sql("select RECORDTYPE, Attribute1 as ITMDISPERC, Attribute2 as ITMDISRFND, Attribute3 as ITMDISVOID, Attribute4 as ITMTRNDISC, Attribute5 as DISCOVRRDN, Attribute6 as STFDISOVRN, Attribute7 as VATEXMTFLG, TRANSACTIONID, TRANSACTIONLINEITEMID, TRANSLINEADJUSTMENTID from "+deltaTable+" where RECORDTYPE='10'")

df_32 = spark.sql("select RECORDTYPE, Attribute1 as DLBOOTSITM, Attribute2 as DLREDMITM, Attribute3 as DLEXMITM, Attribute4 as DLRWDPNTS, TRANSACTIONID, TRANSACTIONLINEITEMID, TRANSLINEADJUSTMENTID from "+deltaTable+" where RECORDTYPE='32'")

df_45=spark.sql("select RECORDTYPE, Attribute1 as CARDNUM, Attribute2 as ITMCODE, Attribute3 as TOPUPAMT, Attribute4 as NETWRKNAME, Attribute5 as NETWRKPRDT, Attribute6 as SWIPEKEYED, Attribute7 as VOIDFLAG, Attribute8 as RESPONCODE, Attribute9 as MOBPHNNUM, TRANSACTIONID, TRANSACTIONLINEITEMID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='45'")

df_57=spark.sql("select RECORDTYPE, Attribute1 as CARDNUM, Attribute2 as ITMCODE, Attribute3 as TOPUPAMT, Attribute4 as NETWRKNAME, Attribute5 as NETWRKPRDT, Attribute6 as RESPONCODE, TRANSACTIONID, TRANSACTIONLINEITEMID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='57'")

df_64=spark.sql("select RECORDTYPE, Attribute1 as PROMOID, Attribute2 as PRDESC, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='64'")

df_24=spark.sql("select RECORDTYPE, Attribute1 as TRANSNUMBER, Attribute2 as BUSINESSDAYDATE, Attribute3 as BEGINTIMESTAMP, Attribute4 as CREDITCLAIMNO, Attribute5 as BCLETTER, Attribute6 as CLAIMREASON, Attribute7 as INVNUMBER, Attribute8 as FOLIONO, Attribute9 as BATCHREF, Attribute10 as CONSFLAG, Attribute11 as REPCATG, Attribute12 as REPNO, Attribute13 as POLCYNO, Attribute14 as DDDANO, Attribute15 as DELVNO, Attribute16 as DELDATE, Attribute17 as CARTONRCD, Attribute18 as REFDOCID, Attribute19 as COMMENT, Attribute20 as ITMCOUNT, Attribute21 as ITEMID, Attribute22 as BASEUOM, Attribute23 as BASEQUANTITY, Attribute24 as UNITPRICE, TRANSACTIONID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='24'")

df_25=spark.sql("select RECORDTYPE, Attribute1 as TRANSNUMBER, Attribute2 as BUSINESSDAYDATE, Attribute3 as BEGINTIMESTAMP, Attribute4 as CREDITCLAIMNO, Attribute5 as AUTHORISATION, Attribute6 as STOCKFL, Attribute7 as CLAIMREASON, Attribute8 as ITMCOUNT, Attribute9 as ITEMID, Attribute10 as BASEUOM, Attribute11 as BASEQUANTITY, Attribute12 as UNITPRICE, TRANSACTIONID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='25'")

df_26=spark.sql("select RECORDTYPE, Attribute1 as TRANSNUMBER, Attribute2 as BUSINESSDAYDATE, Attribute3 as BEGINTIMESTAMP, Attribute4 as UODNO, Attribute5 as STATUS, Attribute6 as CREDITCLAIMNO, Attribute7 as UODQTY, Attribute8 as STOCKFL, Attribute9 as SPROUTE, Attribute10 as DISPLOC, Attribute11 as BCLETTER, Attribute12 as RECALNO, Attribute13 as AUTHORISATION, Attribute14 as RETMETHD, Attribute15 as CARRIER, Attribute16 as CLAIMREASON, Attribute17 as RESCAN, Attribute18 as DAMREAS, Attribute19 as RECSTORE, Attribute20 as ITEMID, Attribute21 as BASEUOM, Attribute22 as BASEQUANTITY, Attribute23 as UNITPRICE, TRANSACTIONID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='26'")

df_28 = spark.sql("select RECORDTYPE, Attribute1 as TRANSNUMBER, Attribute2 as BUSINESSDAYDATE, Attribute3 as BEGINTIMESTAMP, Attribute4 as UODNO, Attribute5 as STATUS, Attribute6 as CREDITCLAIMNO, Attribute7 as ITMCOUNT, Attribute8 as ITEMID, Attribute9 as BASEUOM, Attribute10 as BASEQUANTITY, Attribute11 as UNITPRICE, TRANSACTIONID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='28'")

df_46 = spark.sql("select RECORDTYPE, Attribute1 as GCRDNUM, Attribute2 as GCRDTRNID, Attribute3 as GCRD1SALFL, Attribute4 as GCRD2SALFL, Attribute5 as GCRDRETFLG, Attribute6 as GCRDADDFLG, Attribute7 as GCRDREMFLG, Attribute8 as GCRDAMTADJ, Attribute9 as GCRDPREVBL, TRANSACTIONID , TRANSACTIONLINEITEMID from "+deltaTable+" where RECORDTYPE='46'")

df_86=spark.sql("select RECORDTYPE, Attribute1 as GCRDNUM, Attribute2 as GCRDTRNID, Attribute3 as GCRD1SALFL, Attribute4 as GCRD2SALFL, Attribute5 as GCRDRETFLG, Attribute6 as GCRDADDFLG, Attribute7 as GCRDREMFLG, Attribute8 as GCRDAMTADJ, Attribute9 as GCRDPREVBL, TRANSACTIONID from "+deltaTable+" where RECORDTYPE='86'")

df_TR = spark.sql("select RECORDTYPE, Attribute1 as TENDERNUMBER, Attribute2 as TENDERTYPECODE, Attribute3 as TENDERAMOUNT, Attribute4 as TENDERID, Attribute5 as ACCOUNTNUMBER, TRANSACTIONID, PAYMENTID, TRANSACTIONDATE from "+deltaTable+" where RECORDTYPE='TR'")

df_23 = spark.sql("select RECORDTYPE, Attribute1 as EPSCRDNUM, Attribute2 as EPSCRDAMT, Attribute3 as EPSTYPE, Attribute4 as EPSCRDTYPE, Attribute5 as EPSENTRYTY, Attribute6 as EPSTERVERF, PAYMENTID from "+deltaTable+" where RECORDTYPE='23'")

df_61 = spark.sql("select RECORDTYPE, Attribute1 as DATE, Attribute2 as TIME, Attribute3 as CARDTRAMT, Attribute4 as FLAG, Attribute5 as CARDTRMASK, Attribute6 as CARDTRNUM, Attribute7 as ID, Attribute8 as CARDENTRYTY, Attribute9 as CARDTRTY, Attribute10 as CARDCVM, Attribute11 as AUTH_TYPE, Attribute12 as CARDATHCD, Attribute13 as RESPONSE_CODE, Attribute14 as CARDNAME, Attribute15 as CARDMID, Attribute16 as ZTOKEN, PAYMENTID from "+deltaTable+" where RECORDTYPE='61'")

df_13 = spark.sql("select RECORDTYPE, Attribute1 as NEGTNDRFLG, PAYMENTID from "+deltaTable+" where RECORDTYPE='13'")

df_11 = spark.sql("select RECORDTYPE, Attribute1 as TENDRAMNT, PAYMENTID from "+deltaTable+" where RECORDTYPE='11'")


# COMMAND ----------

#create temp view for entity dataframes

df_H.createOrReplaceTempView('df_H')
df_0.createOrReplaceTempView('df_0')
df_60.createOrReplaceTempView('df_60')
df_50.createOrReplaceTempView('df_50')
df_LC.createOrReplaceTempView('df_LC')
df_36.createOrReplaceTempView('df_36')
df_38.createOrReplaceTempView('df_38')
df_L.createOrReplaceTempView('df_L')
df_6.createOrReplaceTempView('df_6')
df_45.createOrReplaceTempView('df_45')
df_57.createOrReplaceTempView('df_57')
df_46.createOrReplaceTempView('df_46')
df_32.createOrReplaceTempView('df_32')
df_LD.createOrReplaceTempView('df_LD')
df_41.createOrReplaceTempView('df_41')
df_10.createOrReplaceTempView('df_10')
df_55.createOrReplaceTempView('df_55')
df_20.createOrReplaceTempView('df_20')
df_HD.createOrReplaceTempView('df_HD')
df_34.createOrReplaceTempView('df_34')
df_93.createOrReplaceTempView('df_93')
df_1.createOrReplaceTempView('df_1')
df_TR.createOrReplaceTempView('df_TR')
df_13.createOrReplaceTempView('df_13')
df_39.createOrReplaceTempView('df_39')
df_86.createOrReplaceTempView('df_86')
df_61.createOrReplaceTempView('df_61')
df_25.createOrReplaceTempView('df_25')
df_11.createOrReplaceTempView('df_11')
df_24.createOrReplaceTempView('df_24')
df_26.createOrReplaceTempView('df_26')
df_28.createOrReplaceTempView('df_28')
df_81.createOrReplaceTempView('df_81')
df_FT.createOrReplaceTempView('df_FT')
df_5.createOrReplaceTempView('df_5')
df_33.createOrReplaceTempView('df_33')
df_23.createOrReplaceTempView('df_23')
df_64.createOrReplaceTempView('df_64')
df_94.createOrReplaceTempView('df_94')

# COMMAND ----------

#merging entity tables for creating ADRM model tables

df_Transaction=spark.sql("select df_H.TRANSACTIONID as TransactionId, df_H.RETAILSTOREID as SiteSourceKey, df_H.BUSINESSDAYDATE as TransactionDate, df_H.TRANSTYPECODE as TransactionTypeId, df_H.WORKSTATIONID as PointOfSaleId, df_H.TRANSNUMBER as SourceTransactionNumber, df_H.BEGINTIMESTAMP as TransactionInitiatedTimestamp, df_H.ENDDATETIMESTAMP as TransactionCompletedTimestamp, df_H.OPERATORQUAL as OperatorIndicator, df_H.OPERATORID as OperatorId, df_H.TRANSCURRENCY as IsoCurrencyCode, df_H.PARTNERQUALIFIER as AccountSaleIndicator, df_H.PARTNERID as AccountSaleAccountNumber, df_0.MGRKEYUSD as ManagerKeyIndicator, df_0.TRNNOSETFG as TransactionNumberSetIndicator, df_0.RINGTIME as RingDuration, df_0.TNDRTIME as PaymentDuration, df_0.NONSLSTIME as TransactionNonSalesDuration, df_0.INACTIVTIM as InactiveDuration, df_0.BASECURR as TransactionBaseCurrencyType, df_0.OPEMPID as OperatorEmployeeId, df_0.OPEMPFLG as OperatorEmployeeIndicator, df_0.OPEMPGRPCD as OperatorEmployeeGroupCode, df_0.SUPRID as SupervisorId, df_0.SUPREMPID as SupervisorEmployeeId, df_0.SUPREMPFLG as SupervisorEmployeeIndicator, df_0.SUPREMPGRP as SupervisorEmployeeGroupCode, df_0.GRPLUS as TransactionGrossPlusAmount, df_0.GRMINUS as TransactionGrossMinusAmount, df_0.NETCASH as NetCashAmount, df_0.NETNONCASH as NetNonCashAmount, df_0.NONCSHNEGF as NegativeTenderIndicator, df_0.NETTDRTOR as NetTotalAmount, df_94.TRANSUSPND as TransactionSuspendedIndicator, df_93.TRANRETSUS as TransactionRetrievedIndicator, df_81.COUNTTILL as AccountSalePointOfSaleId, df_81.COUNTAMNT as AccountSaleTransactionAmount, df_36.STFDICCARD as StaffDiscountCardNumber, df_60.ORGRECTPRS as OriginalReceiptPresentIndicator, df_1.ORGTRANNO as OriginalTransactionId, df_1.ORGTILLNO as OriginalPointOfSaleId, df_1.ORGOPID as OriginalOperatorId, df_1.ORDDATETRN as OriginalTransactionDate, df_1.ORGTIMETRN as OriginalTransactionTime, df_50.COMORDNUM as DotcomOrderNumber, CASE WHEN df_FT.FITYPECODE IS NOT NULL THEN df_FT.FITYPECODE WHEN df_33.SUNDREASN IS NOT NULL THEN df_33.SUNDREASN ELSE NULL END as FinancialMovementTypeCode, df_FT.AMOUNT as FinancialMovementAmount, df_33.CHISSTILL as ChangeIssuedPointOfSaleId, df_41.PROMOID as PromotionId, df_41.PRDESC as PromotionName, df_LC.LOYNUMBER as LoyaltyAccountNumber, df_LC.CUSTCARDNUMBER as LoyaltyCardId, df_LC.LOYPROGRAMID as LoyaltyProgramId, df_LC.ELIGIBLEAMOUNT as LoyaltyEligibleAmount, df_LC.ELIGIBLEQUANTITY as LoyaltyEligibleQuantity, df_LC.ELIGIBLEQUANUOM as LoyaltyEligibleUOM, df_LC.CUSTCARDHOLDNAME as LoyaltyCardHolderName, df_LC.CUSTCARDTYPE as LoyaltyCardCategory, df_LC.CUSTCARDVALIDFR as LoyaltyCardValidFromDate, df_LC.CUSTCARDVALIDTO as LoyaltyCardValidToDate, df_38.ADCARD_PRESENTATION_METHOD as LoyaltyCardAcquisitionMethodId, df_38.LVALIDBARC as LoyaltyValidationBarcode from df_H left join df_0 on df_H.TRANSACTIONID =df_0.TRANSACTIONID left join df_94 on df_H.TRANSACTIONID =df_94.TRANSACTIONID left join df_93 on df_H.TRANSACTIONID =df_93.TRANSACTIONID left join df_81 on df_H.TRANSACTIONID =df_81.TRANSACTIONID left join df_36 on df_H.TRANSACTIONID =df_36.TRANSACTIONID left join df_60 on df_H.TRANSACTIONID =df_60.TRANSACTIONID left join df_1 on df_H.TRANSACTIONID =df_1.TRANSACTIONID left join df_50 on df_H.TRANSACTIONID =df_50.TRANSACTIONID left join df_FT on df_H.TRANSACTIONID =df_FT.TRANSACTIONID left join df_33 on df_H.TRANSACTIONID =df_33.TRANSACTIONID left join df_41 on df_H.TRANSACTIONID =df_41.TRANSACTIONID left join df_LC on df_H.TRANSACTIONID =df_LC.TRANSACTIONID left join df_38 on df_H.TRANSACTIONID =df_38.TRANSACTIONID")

#df_Transaction.cache()

df_TransactionLineItem=spark.sql("Select df_L.TRANSACTIONID as TransactionId, df_L.TRANSACTIONLINEITEMID as TransactionLineItemId, df_L.RETAILTYPECODE as TransactionLineItemTypeId, df_L.RETAILREASONCODE as TransactionReasonCode, df_L.ITEMIDQUALIFIER as ArticleIdentifierType, df_L.ITEMID as ProductSourceKey, df_L.RETAILQUANTITY as Quantity, df_L.SALESUOM as UnitOfMeasure, df_L.SALESAMOUNT as TransactionProductPriceAmount, CASE when df_L.NORMALSALESAMOUNT is not null THEN df_L.NORMALSALESAMOUNT ELSE df_55.ORGPRICE END as ProductListPriceAmount, df_L.ENTRYMETHODCODE as EntryMethodType, df_6.ITMBASEPNT as TransactionBaseLoyaltyUnits, df_6.BOOTSBRAND as BootsOwnBrandIndicator, df_6.ELGREDEMP as EligibleForRedemptionIndicator, df_6.EXPTDISC as DiscountExemptIndicator, df_6.EXPTPTAQST as LoyaltyUnitsAcquisitionExemptIndicator, df_6.EXTPRICE as TotalTransactionLineItemAmount, df_6.ITEMVOID as ItemVoidedIndicator, df_6.ITMMVNTFLG as ItemMovementKeptIndicator, df_6.ITMREFUND as ItemRefundedIndicator, df_6.ITMSOLDOFL as ItemSoldOfflineIndicator, df_6.LOCPRCFLG as ItemLocallyPricedIndicator, df_6.QTYENTRFLG as QuantityEnteredIndicator, df_6.NONPLUFLG as ItemPriceManuallyEnteredIndicator, df_6.PRCOVRRDN as ProductPriceOverrideIndicator, df_5.DUMMYART as DummyArticleNumber, df_6.RFNDFL_COM as DotcomOrderRefundIndicator, df_L.TRANSACTIONDATE as TransactionDate from df_L left join df_55 on df_L.TRANSACTIONLINEITEMID =df_55.TRANSACTIONLINEITEMID left join df_5 on df_L.TRANSACTIONLINEITEMID =df_5.TRANSACTIONLINEITEMID left join df_6 on df_L.TRANSACTIONLINEITEMID =df_6.TRANSACTIONLINEITEMID")

#df_TransactionLineItem.cache()

df_TransactionAdjustment=spark.sql("select df_HD.TRANSACTIONID as TransactionId, df_HD.DISCOUNTID as DiscountIdentifier, df_HD.DISCNUMBER as TransactionNumber, df_HD.DISCTYPECODE as DiscountTypeCode, df_HD.DISCREASONCODE as DiscountReasonCode, df_HD.REDUCTIONAMOUNT as AdjustmentAmount, df_HD.BONUSBUYID as AdjustmentId, df_20.QUALREASN as QualificationReason, df_20.QUALAMNT as QualificationAmount, df_20.QUALLISTX as QualificationListXIndicator, df_20.QUALLISTA as QualificationListAIndicator, df_20.QUALLISTB as QualificationListBIndicator, df_20.QUALLISTC as QualificationListCIndicator, df_20.QUALBRAND as QualificationBrandStatus, df_20.QUALREDEEM as QualificationRedeemableStatus, df_20.QUALEXEMPT as QualificationExemptStatus, df_20.QUALNUMBER as QualificationCount, df_20.EXTRAADPNT as ExtraLoyaltyUnitsCount, df_34.REWARDAMNT as RewardAmount, df_HD.TRANSACTIONDATE as TransactionDate from df_HD left join df_20 ON df_HD.TRANADJUSTMENTID = df_20.TRANADJUSTMENTID left join df_34 ON df_HD.TRANADJUSTMENTID = df_34.TRANADJUSTMENTID where df_HD.BONUSBUYID is not null")

#df_TransactionAdjustment.cache()

df_TransactionLineItemAdjustment=spark.sql("select df_LD.TRANSACTIONID as TransactionId, df_LD.TRANSACTIONLINEITEMID as TransactionLineItemId, df_LD.DISCOUNTID as DiscountIdentifier, df_LD.DISCNUMBER as TransactionLineItemNumber, df_LD.DISCTYPECODE as DiscountTypeCode, df_LD.DISCREASONCODE as DiscountReasonCode, df_LD.REDUCTIONAMOUNT as AdjustmentAmount, df_LD.BONUSBUYID as AdjustmentId, df_10.ITMDISPERC as DiscountPercentage, df_10.ITMDISRFND as DiscountRefundedIndicator, df_10.ITMDISVOID as DiscountVoidedIndicator, df_10.ITMTRNDISC as TransactionLevelDiscountIndicator, df_10.DISCOVRRDN as DiscountOverriddenIndicator, df_10.STFDISOVRN as StaffDiscountOverriddenIndicator, df_10.VATEXMTFLG as VATExemptDiscountIndicator, df_32.DLBOOTSITM as BootsItemDealListId, df_32.DLREDMITM as DealListRedeemableIndicator, df_32.DLEXMITM as DealListExemptItemIndicator, df_32.DLRWDPNTS as DealLoyaltyRewardUnits, df_LD.TRANSACTIONDATE as TransactionDate from df_LD left join df_10 ON df_LD.TRANSLINEADJUSTMENTID = df_10.TRANSLINEADJUSTMENTID left join df_32 ON df_LD.TRANSLINEADJUSTMENTID = df_32.TRANSLINEADJUSTMENTID where df_LD.BONUSBUYID is not null")

##df_TransactionLineItemAdjustment.cache()

df_TransactionLoyaltyAccount=spark.sql("select df_LC.TRANSACTIONID as TransactionId, df_LC.CUSTCARDNUMBER as LoyaltyCardId, '' as LoyaltyProgramId, '' as LoyaltyAccountId, df_LC.LOYPNTSREDEEMED as LoyaltyUnitsUsed, df_LC.LOYPNTSTOTAL as LoyaltyAccountBalanceAmount, df_38.COUNTCRDUS as LoyaltyCardUsageCount, df_LC.TRANSACTIONDATE as TransactionDate from df_LC left join df_38 on df_LC.TRANSACTIONID = df_38.TRANSACTIONID where df_LC.CUSTCARDNUMBER is not null")

##df_TransactionLoyaltyAccount.cache()

df_LoyaltyAccountEarning=spark.sql("select df_LC.TRANSACTIONID as TransactionId, df_LC.CUSTCARDNUMBER as LoyaltyCardId, '' as LoyaltyProgramId, '' as LoyaltyAccountId, df_H.ENDDATETIMESTAMP as Timestamp, df_LC.LOYPNTSAWARDED as EarnedLoyaltyUnits, df_38.LREASNCODE as LoyaltyEarningTypeId, df_38.LPVALIDCOD as LoyaltyAdditionValidationCode, df_LC.TRANSACTIONDATE as TransactionDate from df_LC left join df_38 on df_LC.TRANSACTIONID = df_38.TRANSACTIONID left join df_H on df_LC.TRANSACTIONID = df_H.TRANSACTIONID where df_LC.CUSTCARDNUMBER is not null")

#df_LoyaltyAccountEarning.cache()

df_GiftCardTransaction=spark.sql("select df_46.TRANSACTIONID as TransactionId, df_46.TRANSACTIONLINEITEMID as TransactionLineItemId, df_46.GCRDNUM as GiftCardId, df_H.ENDDATETIMESTAMP as Timestamp, df_46.GCRDTRNID as GiftCardTransactionNumber, df_46.GCRD1SALFL as FirstSaleIndicator, df_46.GCRD2SALFL as SubsequentSaleIndicator, case when df_46.GCRDRETFLG is NOT NULL Then 'Funds Returned' when df_46.GCRDADDFLG is NOT NULL Then 'Funds Added' when df_46.GCRDREMFLG is NOT NULL Then 'Funds Removed' ELSE NULL END as GiftCardTransactionType, df_46.GCRDAMTADJ as Amount, df_46.GCRDPREVBL as GiftCardOpeningBalanceAmount, df_H.BUSINESSDAYDATE as TransactionDate from df_46 left join df_H ON df_46.TRANSACTIONID = df_H.TRANSACTIONID where df_46.GCRDNUM is not null union select df_86.TRANSACTIONID as TransactionId, NULL as TransactionLineItemId, df_86.GCRDNUM as GiftCardId, df_H.ENDDATETIMESTAMP as Timestamp, df_86.GCRDTRNID as GiftCardTransactionNumber, df_86.GCRD1SALFL as FirstSaleIndicator, df_86.GCRD2SALFL as SubsequentSaleIndicator, case when df_86.GCRDRETFLG is NOT NULL Then 'Funds Returned' when df_86.GCRDADDFLG is NOT NULL Then 'Funds Added' when df_86.GCRDREMFLG is NOT NULL Then 'Funds Removed' ELSE NULL END as GiftCardTransactionType, df_86.GCRDAMTADJ as Amount, df_86.GCRDPREVBL as GiftCardOpeningBalanceAmount, df_H.BUSINESSDAYDATE as TransactionDate from df_86 left join df_H ON df_86.TRANSACTIONID = df_H.TRANSACTIONID where df_86.GCRDNUM is not null")

#df_GiftCardTransaction.cache()

df_TransactionPromotion=spark.sql("select df_41.TRANSACTIONID as TransactionId, df_41.PROMOID as PromotionId, df_41.PRDESC as PromotionDescription, df_H.BUSINESSDAYDATE as TransactionDate from df_41 left join df_H ON df_41.TRANSACTIONID = df_H.TRANSACTIONID where df_41.PROMOID is not null Union select df_64.TRANSACTIONID as TransactionId, df_64.PROMOID as PromotionId, df_64.PRDESC as PromotionDescription, df_H.BUSINESSDAYDATE as TransactionDate from df_64 left join df_H ON df_64.TRANSACTIONID = df_H.TRANSACTIONID where df_64.PROMOID is not null")

#df_TransactionPromotion.cache()

df_TransactionPayment=spark.sql("select distinct df_H.TRANSACTIONID as TransactionId, df_TR.PAYMENTID as PaymentId, df_H.BUSINESSDAYDATE as TransactionDate from df_TR left join df_H ON df_TR.TRANSACTIONID = df_H.TRANSACTIONID")

#df_TransactionPayment.cache()

df_Payment=spark.sql("select df_TR.PAYMENTID as PaymentId, df_TR.TENDERNUMBER as PaymentTenderNumber, df_TR.TENDERTYPECODE as PaymentMethodTypeCode, df_TR.TENDERAMOUNT as PaymentAmount, df_TR.TENDERID as TenderId, df_TR.ACCOUNTNUMBER as AccountSaleAccountNumber, df_13.NEGTNDRFLG as NegativeTenderIndicator, df_23.EPSCRDNUM as PaymentCardPAN, df_23.EPSCRDTYPE as PaymentCardTypeId, CASE WHEN df_23.EPSENTRYTY is not null THEN df_23.EPSENTRYTY ELSE df_61.CARDENTRYTY END as PaymentCardAcquisitionMethodId, df_23.EPSTERVERF as TerminalVerificationResults, CONCAT(df_61.DATE,df_61.TIME) as PaymentTimestamp, df_61.FLAG as TokenizedIndicator, df_61.CARDTRMASK as PaymentCardNumberMasked, df_61.CARDTRNUM as PaymentCardTenderNumber, df_61.ID as PaymentCardNumber, df_61.CARDTRTY as CardTenderType, df_61.CARDCVM as CustomerVerificationMethod, df_61.AUTH_TYPE as PaymentAuthorizationType, df_61.CARDATHCD as PaymentCardAuthorizationCode, df_61.RESPONSE_CODE as PaymentResponseCode, df_61.CARDNAME as PaymentCardName, df_61.CARDMID as PaymentCardMerchantId, df_61.ZTOKEN as PaymentTNSTokenNumber, df_TR.TRANSACTIONDATE as TransactionDate from df_TR left join df_13 on df_TR.PAYMENTID = df_13.PAYMENTID left join df_23 on df_TR.PAYMENTID = df_23.PAYMENTID left join df_61 on df_TR.PAYMENTID = df_61.PAYMENTID  left join df_11 on df_TR.PAYMENTID = df_11.PAYMENTID")

#df_Payment.cache()

df_TransactionCoupon=spark.sql("select df_39.TRANSACTIONID as TransactionId, df_39.CPONBARCOD as CouponId, df_39.CPONAMT as TransactionCouponDiscountAmount, df_39.CPONSCNKEY as CouponKeyedIndicator, df_39.CPONRETFLG as CouponRefundIndicator, df_39.CPONVODFLG as CouponVoidedIndicator, df_39.CPONQTYFLG as CouponQuantityEnteredIndicator, df_39.CPONPNTFLG as LoyaltyUnitsCouponIndicator, df_39.CPONOVRRID as CouponPriceOverrideIndicator, df_39.TRANSACTIONDATE as TransactionDate from df_39 where df_39.CPONBARCOD is not null")

#df_TransactionCoupon.cache()

df_ETopupEVoucher=spark.sql("select df_45.TRANSACTIONID as TransactionId, df_45.TRANSACTIONLINEITEMID as TransactionLineItemId, df_45.CARDNUM as CardNumber, df_45.ITMCODE as ItemCode, df_45.TOPUPAMT as TopUpAmount, df_45.NETWRKNAME as NetworkName, df_45.NETWRKPRDT as NetworkProductName, df_45.SWIPEKEYED as CardNumberAcquisitionMethodId, df_45.VOIDFLAG as ItemVoidedIndicator, df_45.RESPONCODE as ResponseCode, df_45.MOBPHNNUM as TopupMobileNumber, df_45.TRANSACTIONDATE as TransactionDate from df_45 where df_45.CARDNUM is not null union select df_57.TRANSACTIONID as TransactionId, df_57.TRANSACTIONLINEITEMID as TransactionLineItemId, df_57.CARDNUM as CardNumber, df_57.ITMCODE as ItemCode, df_57.TOPUPAMT as TopUpAmount, df_57.NETWRKNAME as NetworkName, df_57.NETWRKPRDT as NetworkProductName, NULL as CardNumberAcquisitionMethodId, NULL as ItemVoidedIndicator, df_57.RESPONCODE as ResponseCode, NULL as TopupMobileNumber, df_57.TRANSACTIONDATE as TransactionDate from df_57 where df_57.CARDNUM is not null")

#df_ETopupEVoucher.cache()

df_TransactionCreditClaim=spark.sql("select df_24.TRANSACTIONID as TransactionId, df_24.CREDITCLAIMNO as CreditClaimNumber, df_24.ITEMID as ProductSourceKey, df_24.TRANSNUMBER as TransactionNumber, df_24.BUSINESSDAYDATE as TransactionDate, df_24.BEGINTIMESTAMP as TransactionInitiatedTimestamp, df_24.BCLETTER as BusinessCentreLetter, df_24.CLAIMREASON as CreditClaimReasonCode, df_24.INVNUMBER as InvoiceNumber, df_24.FOLIONO as FolioNumber, df_24.BATCHREF as BatchReference, df_24.CONSFLAG as ConsignmentType, df_24.REPCATG as RepairCategoryReasonCode, df_24.REPNO as RepairNumber, df_24.POLCYNO as Plan4PolicyNumber, df_24.DDDANO as DDDADCDRNumber, df_24.DELVNO as DeliveryNoteNumber, df_24.DELDATE as DeliveryDate, df_24.CARTONRCD as CartonsReceivedCount, df_24.REFDOCID as OrderNumber, df_24.COMMENT as CreditClaimComment, df_24.ITMCOUNT as CreditClaimTotalItemCount, df_24.BASEUOM as UnitOfMeasureId, df_24.BASEQUANTITY as Quantity, df_24.UNITPRICE as ItemUnitPrice, NULL as CreditClaimAuthorisation, NULL as StockAdjustmentIndicator, NULL as UnitOfDeliveryNumber, NULL as UnitOfDeliveryStatus, NULL as UnitOfDeliveryQuantity, NULL as SupplyRouteType, NULL as DispensaryLocationType, NULL as RecallNumber, NULL as ReturnMethodId, NULL as CarrierId, NULL as UnitOfDeliveryType, NULL as DamageReasonCode, NULL as SiteSourceKey from df_24 where df_24.CREDITCLAIMNO is not null and df_24.ITEMID is not null UNION select df_25.TRANSACTIONID as TransactionId, df_25.CREDITCLAIMNO as CreditClaimNumber, df_25.ITEMID as ProductSourceKey, df_25.TRANSNUMBER as TransactionNumber, df_25.BUSINESSDAYDATE as TransactionDate, df_25.BEGINTIMESTAMP as TransactionInitiatedTimestamp, NULL as BusinessCentreLetter, df_25.CLAIMREASON as CreditClaimReasonCode, NULL as InvoiceNumber, NULL as FolioNumber, NULL as BatchReference, NULL as ConsignmentType, NULL as RepairCategoryReasonCode, NULL as RepairNumber, NULL as Plan4PolicyNumber, NULL as DDDADCDRNumber, NULL as DeliveryNoteNumber, NULL as DeliveryDate, NULL as CartonsReceivedCount, NULL as OrderNumber, NULL as CreditClaimComment, df_25.ITMCOUNT as CreditClaimTotalItemCount, df_25.BASEUOM as UnitOfMeasureId, df_25.BASEQUANTITY as Quantity, df_25.UNITPRICE as ItemUnitPrice, df_25.AUTHORISATION as CreditClaimAuthorisation, df_25.STOCKFL as StockAdjustmentIndicator, NULL as UnitOfDeliveryNumber, NULL as UnitOfDeliveryStatus, NULL as UnitOfDeliveryQuantity, NULL as SupplyRouteType, NULL as DispensaryLocationType, NULL as RecallNumber, NULL as ReturnMethodId, NULL as CarrierId, NULL as UnitOfDeliveryType, NULL as DamageReasonCode, NULL as SiteSourceKey from df_25 where df_25.CREDITCLAIMNO is not null and df_25.ITEMID is not null union select df_26.TRANSACTIONID as TransactionId, df_26.CREDITCLAIMNO as CreditClaimNumber, df_26.ITEMID as ProductSourceKey, df_26.TRANSNUMBER as TransactionNumber, df_26.BUSINESSDAYDATE as TransactionDate, df_26.BEGINTIMESTAMP as TransactionInitiatedTimestamp, df_26.BCLETTER as BusinessCentreLetter, df_26.CLAIMREASON as CreditClaimReasonCode, NULL as InvoiceNumber, NULL as FolioNumber, NULL as BatchReference, NULL as ConsignmentType, NULL as RepairCategoryReasonCode, NULL as RepairNumber, NULL as Plan4PolicyNumber, NULL as DDDADCDRNumber, NULL as DeliveryNoteNumber, NULL as DeliveryDate, NULL as CartonsReceivedCount, NULL as OrderNumber, NULL as CreditClaimComment, NULL as CreditClaimTotalItemCount, df_26.BASEUOM as UnitOfMeasureId, df_26.BASEQUANTITY as Quantity, df_26.UNITPRICE as ItemUnitPrice, df_26.AUTHORISATION as CreditClaimAuthorisation, df_26.STOCKFL as StockAdjustmentIndicator, df_26.UODNO as UnitOfDeliveryNumber, df_26.STATUS as UnitOfDeliveryStatus, df_26.UODQTY as UnitOfDeliveryQuantity, df_26.SPROUTE as SupplyRouteType, df_26.DISPLOC as DispensaryLocationType, df_26.RECALNO as RecallNumber, df_26.RETMETHD as ReturnMethodId, df_26.CARRIER as CarrierId, df_26.RESCAN as UnitOfDeliveryType, df_26.DAMREAS as DamageReasonCode, df_26.RECSTORE as SiteSourceKey from df_26 where df_26.ITEMID is not null and df_26.CREDITCLAIMNO is not null union select df_28.TRANSACTIONID as TransactionId, df_28.CREDITCLAIMNO as CreditClaimNumber, df_28.ITEMID as ProductSourceKey, df_28.TRANSNUMBER as TransactionNumber, df_28.BUSINESSDAYDATE as TransactionDate, df_28.BEGINTIMESTAMP as TransactionInitiatedTimestamp, NULL as BusinessCentreLetter, NULL as CreditClaimReasonCode, NULL as InvoiceNumber, NULL as FolioNumber, NULL as BatchReference, NULL as ConsignmentType, NULL as RepairCategoryReasonCode, NULL as RepairNumber, NULL as Plan4PolicyNumber, NULL as DDDADCDRNumber, NULL as DeliveryNoteNumber, NULL as DeliveryDate, NULL as CartonsReceivedCount, NULL as OrderNumber, NULL as CreditClaimComment, df_28.ITMCOUNT as CreditClaimTotalItemCount, df_28.BASEUOM as UnitOfMeasureId, df_28.BASEQUANTITY as Quantity, df_28.UNITPRICE as ItemUnitPrice, NULL as CreditClaimAuthorisation, NULL as StockAdjustmentIndicator, df_28.UODNO as UnitOfDeliveryNumber, df_28.STATUS as UnitOfDeliveryStatus, NULL as UnitOfDeliveryQuantity, NULL as SupplyRouteType, NULL as DispensaryLocationType, NULL as RecallNumber, NULL as ReturnMethodId, NULL as CarrierId, NULL as UnitOfDeliveryType, NULL as DamageReasonCode, NULL as SiteSourceKey from df_28 where df_28.CREDITCLAIMNO is not null and df_28.ITEMID is not null")

#df_TransactionCreditClaim.cache()

# COMMAND ----------

OutputTableList=[]
for destPath in destinationDetails:
  if len(eval(destPath["dataframeName"]).head(1))>0:
    OutputTableList.append(destPath["dataframeName"][3:])
    dest_path="/mnt/idf-curatestage"+destPath["destination_path"]+RunDate+RunTime+'/'
    eval(destPath["dataframeName"]).write.mode(destPath["mode"]).format("csv").option("header",True).save(dest_path)

# COMMAND ----------

#Get count of output table
DestinationTableCount=len(OutputTableList)

#deleting temp delta table
spark.sql("drop table {}".format(deltaTable))

# COMMAND ----------

#exit notebook with table names
dbutils.notebook.exit({"SourceCount":SourceFileCount,"DestinationCount":DestinationTableCount,"DestinationTableList":OutputTableList})