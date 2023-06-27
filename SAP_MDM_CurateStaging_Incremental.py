# Databricks notebook source
#import required libraries

from pyspark.sql.functions import *

# COMMAND ----------

#Get pipeline parameters
dbutils.widgets.text("RunDate", '', '')
RunDate = dbutils.widgets.get('RunDate') 
print(RunDate)
dbutils.widgets.text("RunTime", '', '')
RunTime = dbutils.widgets.get('RunTime') 
print(RunTime)
dbutils.widgets.text("SourcePath", '', '')
SourcePath = dbutils.widgets.get('SourcePath') 
print(SourcePath)
dbutils.widgets.text("DestinationPath", '', '')
DestinationPath = dbutils.widgets.get('DestinationPath')
print(DestinationPath)
dbutils.widgets.text("FileList", "","")
FileList = dbutils.widgets.get("FileList")
print(FileList)

# COMMAND ----------

#source file location - this will be wrangled location
source_path="/mnt/idf-cleansed/"+SourcePath+"/"
print(source_path)

#destination path 
dest_path="/mnt/idf-curatestage/"+DestinationPath+'/'+RunDate+RunTime+'/'
print(dest_path)

FileList=eval(FileList)
print(FileList)
print(type(FileList))

SourceFileCount=len(FileList)
#create list of file path
SourceList=[]
for file in FileList:
    SourceList.append(source_path+file) #source path + filename 
print(SourceList)

#SourceList=str(SourceList)[1:-1].replace("'","").replace(" ","")  #replacing string character and space
#print(SourceList)

# COMMAND ----------

#Reading source files
df = spark.read.format("com.databricks.spark.xml").option("rootTag", "Sites").option("rowTag", "Site").load(','.join(SourceList))

# COMMAND ----------

for coln in df.columns:
    newcoln='PSites_'+coln
    df=df.withColumnRenamed(coln,newcoln)

# COMMAND ----------

try:
    Floor_Dept_DF=df.withColumn("Floor_Dept",explode_outer(col("PSites_Area_By_Floor_For_Dept.Floor_Dept")),).drop('PSites_Area_By_Floor_For_Dept').selectExpr('*','Floor_Dept.*').drop('Floor_Dept')
    FD_cols=list(set(Floor_Dept_DF.columns)-set(df.columns))
    for coln in FD_cols:
        newcoln='PFloorDept_'+coln
        Floor_Dept_DF=Floor_Dept_DF.withColumnRenamed(coln,newcoln)
except:
    Floor_Dept_DF=df

# COMMAND ----------

try:
    Escalators_DF=Floor_Dept_DF.selectExpr('*','PSites_Escalators.Escalators as Escalators_Escalators').drop('PSites_Escalators').withColumn("Escalators",explode_outer(col("Escalators_Escalators")),).drop('Escalators_Escalators').selectExpr('*','Escalators.*').drop('Escalators')
    Escalators_cols=list(set(Escalators_DF.columns)-set(Floor_Dept_DF.columns))
    for coln in Escalators_cols:
        newcoln='PEscalators_'+coln
        Escalators_DF=Escalators_DF.withColumnRenamed(coln,newcoln)
except:
    Escalators_DF=Floor_Dept_DF

# COMMAND ----------

try:
    ExtOrg_DF=Escalators_DF.withColumn("Ext_OrgRef_Nos",explode_outer(col("PSites_Ext_Orgs_By_Names.Ext_OrgRef_Nos")),).drop('PSites_Ext_Orgs_By_Names').selectExpr('*','Ext_OrgRef_Nos.*').drop('Ext_OrgRef_Nos')
    ExtOrg_cols=list(set(ExtOrg_DF.columns)-set(Escalators_DF.columns))
    for coln in ExtOrg_cols:
        newcoln='PExtOrg_'+coln
        ExtOrg_DF=ExtOrg_DF.withColumnRenamed(coln,newcoln)
except:
    ExtOrg_DF=Escalators_DF

# COMMAND ----------

try:
    FaxNum_DF=ExtOrg_DF.withColumn("FaxNum",explode_outer(col("PSites_Fax_Numbers.Fax_numbers")),).drop('PSites_Fax_Numbers').selectExpr('*','FaxNum.*').drop('FaxNum')
    FaxNum_cols=list(set(FaxNum_DF.columns)-set(ExtOrg_DF.columns))
    for coln in FaxNum_cols:
        newcoln='PFax_'+coln
        FaxNum_DF=FaxNum_DF.withColumnRenamed(coln,newcoln)
except:
    FaxNum_DF=ExtOrg_DF

# COMMAND ----------

try:
    FloorDetrail_DF=FaxNum_DF.withColumn("Floor_detrail",explode_outer(col("PSites_Floor_Details.Floor_detrail")),).drop('PSites_Floor_Details').selectExpr('*','Floor_detrail.*').drop('Floor_detrail')
    FloorDetrail_cols=list(set(FloorDetrail_DF.columns)-set(FaxNum_DF.columns))
    for coln in FloorDetrail_cols:
        newcoln='PFloorDetail_'+coln
        FloorDetrail_DF=FloorDetrail_DF.withColumnRenamed(coln,newcoln)
except:
    FloorDetrail_DF=FaxNum_DF

# COMMAND ----------

try:
    Lift_DF=FloorDetrail_DF.withColumn("Lifts",explode_outer(col("PSites_Lift.Lift")),).drop('PSites_Lift').selectExpr('*','Lifts.*').drop('Lifts')
    Lift_cols=list(set(Lift_DF.columns)-set(FloorDetrail_DF.columns))
    for coln in Lift_cols:
        newcoln='PLift_'+coln
        Lift_DF=Lift_DF.withColumnRenamed(coln,newcoln)
except:
    Lift_DF=FloorDetrail_DF

# COMMAND ----------

try:
    Merchants_DF=Lift_DF.withColumn("Merchants",explode_outer(col("PSites_Merchant.Merchant_Id")),).drop('PSites_Merchant').selectExpr('*','Merchants.*').drop('Merchants')
    Merchants_cols=list(set(Merchants_DF.columns)-set(Lift_DF.columns))
    for coln in Merchants_cols:
        newcoln='PMerchant_'+coln
        Merchants_DF=Merchants_DF.withColumnRenamed(coln,newcoln)
except:
    Merchants_DF=Lift_DF

# COMMAND ----------

try:
    PhoneNumber_DF=Merchants_DF.withColumn("PhoneNumber",explode_outer(col("PSites_Phone_List.PhoneNumber")),).drop('PSites_Phone_List').selectExpr('*','PhoneNumber.*').drop('PhoneNumber')
    PhoneNumber_cols=list(set(PhoneNumber_DF.columns)-set(Merchants_DF.columns))
    for coln in PhoneNumber_cols:
        newcoln='PPhone_'+coln
        PhoneNumber_DF=PhoneNumber_DF.withColumnRenamed(coln,newcoln)
except:
    PhoneNumber_DF=Merchants_DF

# COMMAND ----------

try:
    SecClad_DF=PhoneNumber_DF.withColumn("SecurityCladdings",explode_outer(col("PSites_Security_Claddings.Security_Claddings")),).drop('PSites_Security_Claddings').selectExpr('*','SecurityCladdings.*').drop('SecurityCladdings')
    SecClad_cols=list(set(SecClad_DF.columns)-set(PhoneNumber_DF.columns))
    for coln in SecClad_cols:
        newcoln='PSecurity_'+coln
        SecClad_DF=SecClad_DF.withColumnRenamed(coln,newcoln)
except:
    SecClad_DF=PhoneNumber_DF

# COMMAND ----------

try:
    Van_Route_DF=SecClad_DF.withColumn("Van_Route",explode_outer(col("PSites_VanRoutes_List.Van_Route")),).drop('PSites_VanRoutes_List').selectExpr('*','Van_Route.*').drop('Van_Route')
    Van_Route_cols=list(set(Van_Route_DF.columns)-set(SecClad_DF.columns))
    for coln in Van_Route_cols:
        newcoln='PVanRoute_'+coln
        Van_Route_DF=Van_Route_DF.withColumnRenamed(coln,newcoln)
except:
    Van_Route_DF=SecClad_DF

# COMMAND ----------

try:
    Tax_class_DF=Van_Route_DF.selectExpr('*','PSites_Tax_classification.Tax_class as Tax_class').drop('PSites_Tax_classification').selectExpr('*','Tax_class.*').drop('Tax_class')
    Tax_class_cols=list(set(Tax_class_DF.columns)-set(Van_Route_DF.columns))
    for coln in Tax_class_cols:
        newcoln='PTax_'+coln
        Tax_class_DF=Tax_class_DF.withColumnRenamed(coln,newcoln)
except:
    Tax_class_DF=Van_Route_DF

# COMMAND ----------

try:
    Window_DF=Tax_class_DF.selectExpr('*','PSites_Windows.Windows as Window').drop('PSites_Windows').selectExpr('*','Window.*').drop('Window')
    Window_cols=list(set(Window_DF.columns)-set(Tax_class_DF.columns))
    for coln in Window_cols:
        newcoln='PWindow_'+coln
        Window_DF=Window_DF.withColumnRenamed(coln,newcoln)
except:
    Window_DF=Tax_class_DF

# COMMAND ----------

#Function for adding missing column and converting all columns to string
def detect_data(column, DF, data_type):
    if not column in DF.columns:
        ret = lit(None).cast(data_type)
    else:
        ret = col(column).cast(data_type)

    return ret

# COMMAND ----------

ReqColumnlist=['PSites_Site_Number','PSites_Site_Name','PSites_Site_Extended_Name','PSites_Site_Short_Name','PSites_SiteType_Code','PSites_Format_Code','PSites_Stores_Type_Code','PSites_Confidentiality_Indicator','PSites_Special_Dummy','PSites_Instore_Flag','PSites_CACI_Classification_code','PSites_Franchise','PSites_Price_Band_Code','PSites_Delivery_Chain_code','PSites_Alliance_Healthcare_DC_Code','PSites_CDC_Number','PSites_Chain_Letter_Code','PSites_Supply_Region_Code','PSites_Hazardous_Goods_Shipping_Reg_Flag','PSites_Acquisition_Date','PSites_Rebranding_Date','PSites_Clearance_Date','PSites_Management_Origin_Code','PSites_Customer_Stairs','PSites_Public_Address_System','PSites_Language','PSites_Profit_Centre','PSites_Cost_Centre','PSites_POS_Outbound_Profile_Code','PSites_POS_Inbound_Profile_Code','PSites_POS_Currency_Code','PSites_Calendar_Code','PSites_SAP_Company_Code','PSites_Purchasing__Organisation_Code','PSites_Sales_Organisation_Code','PSites_Distribution_Channel_Code','PSites_SAP_Site_Profile_Code','PSites_Reference_Site_Code','PSites_SAG','PSites_Last_Updated_SAG','PSites_SAG_Pro_flag','PSites_Mini_SAG','PSites_Last_Update_Mini_SAG','PSites_MiniSAG_Pro_flag','PSites_Non-LFL_Code','PSites_Intervention_Start_Date','PSites_Intervention_End_Date','PSites_Intervention_Comparative_To_Date','PSites_Chilled_Food_Depot_Code','PSites_Property_Reference_ID','PSites_Store_Facilities_Code','PSites_Store_Facilities_Desc','PSites_Workflow_Status','PSites_Comments','PSites_Create_Date','PSites_Update_Date','PSites_Created_By','PSites_Updated_By','PSites_TimeStampChange','PSites_Pharmacy_Registration_Status_Code','PSites_PAG','PSites_NHS_Contract_Flag','PSites_NHS_Contract_Code','PSites_Primary_Care_Organisation_Code','PSites_Dispensary_Type_code','PSites_NHS_Market_Description','PSites_Midnight_Pharmacy','PSites_MDS_Room_Flag','PSites_Benchmark_Group_Item_Code','PSites_Benchmark_Group_Sales_Code','PSites_Successor_Store_Number','PSites_Previous_Store_Number','PSites_Child_Store_Number','PSites_Parent_Store_Number','PSites_Site_Status_Code','PSites_Opening_Date','PSites_Closing_Date','PSites_Temporary_Closure_Flag','PPhone_Phone_Number','PPhone_Phone_Extension','PPhone_Standard_Number','PPhone_Country_Code','PPhone_SMS_Enabled','PFax_Fax_Number','PFax_Phone_Extension','PFax_Standard_No','PFax_Country_Code','PFax_SMS_Enabled','PFax_Not_Used','PSites_House_Number','PSites_Street','PSites_Street_2','PSites_District','PSites_TownCity','PSites_County_Code','PSites_Division_Code','PSites_Region_Code','PSites_Area_Number','PSites_Postal_Code','PSites_SD_Country_Code','PSites_Country_Code','PSites_Grid_Reference_Easting','PSites_Grid_Reference_Northing','PSites_Grid_Reference_Latitude','PSites_Grid_Reference_Longitude','PVanRoute_Van_Route_Link_Point','PVanRoute_VanRoute_LinkPointNumber','PVanRoute_StartDate','PExtOrg_Ext_Org_Code','PExtOrg_Ext_Org_Ref_No','PMerchant_Payment_card_code','PMerchant_Merchant_Id','PTax_Tax_categories','PTax_Country_code','PTax_Tax_classification','PSites_Number_of_Opticians_Consulting_Rooms','PSites_Number_of_Gondola_Backs','PSites_Number_of_Transigns','PSites_Number_of_Trading_Floors','PSites_Number_of_Floors','PEscalators_Escalator_Type_Code','PEscalators_Number_of_Escalators','PSecurity_Security_Cladding_Type_Code','PSecurity_Number_of_Security_Claddings','PWindow_Window_Type_Code','PWindow_Number_of_Window','PLift_Lift_types','PLift_Number_of_Lift','PFloorDetail_Floor_Code','PFloorDetail_Noof_Entrance','PFloorDetail_Ceiling_Type','PFloorDetail_Ceiling_Hieght','PFloorDept_Dept_Code','PFloorDept_Floor_Code','PFloorDept_Area','PFloorDept_Provisional_Data_Flag','PFloorDept_Primary_Floor_Flag','PFloorDept_Department_Start_Date','PSites_Grd_tot_Gross_Area','PSites_Last_Upt_Grd_tot_Gross_Area','PSites_Grd_tot_Gross_Area_Pro_flg','PSites_Grd_tot_Net_Sal_area','PSites_Last_Upt_Grd_tot_Net_Sal_area','PSites_Grd_tot_Net_Sales_area_Pro_flg','PSites_Grd_tot_NDSA','PSites_Last_Upt_Grd_tot_Non_Dispensing_Sal_Area_NDSA','PSites_Grd_tot_NDSA_Pro_flg','PSites_Grd_tot_tot_Sal_area','PSites_Last_Upt_Grd_tot_tot_Sal_area','PSites_Grd_tot_Total_Sales_area_Pro_flg','PSites_Grd_tot_Dispensary_area','PSites_Last_Upt_Grd_tot_Dispensary_area','PSites_Grd_tot_Dispensary_area_Pro_flg','PSites_Grd_tot_MDSCHOM_room_area','PSites_Last_Upt_Grd_tot_MDSCHOM_room_area','PSites_Grd_tot_MDSCHOM_room_area_Pro_flg','PSites_Grd_tot_Health_Clinic_area','PSites_Last_Upt_Grd_tot_Health_Clinic_area','PSites_Grd_tot_Health_Clinic_Area_Pro_flg','PSites_Grd_tot_Consultation_area','PSites_Last_Upt_Grd_tot_Consultation_area','PSites_Grd_tot_Consultation_area_Pro_flg','PSites_Grd_tot_Photolab_Off-Sal_area','PSites_Last_Upt_Grd_tot_Photolab_Off-Sal_area','PSites_Grd_tot_Photolab_Off-Sales_area_Pro_flg','PSites_Grd_tot_Photolab_On-Sal_area','PSites_Last_Upt_Grd_tot_Photolab_On-Sal_area','PSites_Grd_tot_Photolab_On-Sales_area_Pro_flg','PSites_Grd_tot_Opticians_Off-Sal_area','PSites_Last_Upt_Grd_tot_Opticians_Off-Sal_area','PSites_Grd_tot_Opticians_Off-Sales_area_Pro_flg','PSites_Grd_tot_Opticians_On-Sal_area','PSites_Last_Upt_Grd_tot_Opticians_On-Sal_area','PSites_Grd_tot_Opticians_On-Sales_area_Pro_flg','PSites_Grd_tot_Baby_Changing_room_area','PSites_Last_Upt_Grd_tot_Baby_Changing_room_area','PSites_Grd_tot_Baby_Changing_room_area_Pro_flg','PSites_Grd_tot_ATM_room_area','PSites_Last_Upt_Grd_tot_ATM_room_area','PSites_Grd_tot_ATM_room_area_Pro_flg','PSites_Grd_tot_Waitrose_area','PSites_Last_Upt_Grd_tot_Waitrose_area','PSites_Grd_tot_Waitrose_area_Pro_flg','PSites_Grd_tot_David_Omerod_area','PSites_Last_Upt_Grd_tot_David_Omerod_area','PSites_Grd_tot_David_Omerod_area_Pro_flg','PSites_Grd_tot_GP_area','PSites_Last_Upt_Grd_tot_GP_area','PSites_Grd_tot_GP_area_Pro_flg','PSites_Grd_tot_Dentist_area','PSites_Last_Upt_Grd_tot_Dentist_area','PSites_Grd_tot_Dentist_area__Provisonal__flg','PSites_Grd_tot_Other_Health_Services_area','PSites_Last_Upt_Grd_tot_Other_Health_Services_area','PSites_Grd_tot_Other_Health_Services_area_Pro_flg','PSites_Grd_tot_Stock_Area_OnOffSite','PSites_Last_Upt_Grd_tot_Stock_Area_OnandOffSite','PSites_Grd_tot_Stock_Area_OnandoffSite_Pro_flg','PSites_Grd_tot_Stock_Area_OnSite','PSites_Last_Upt_Grd_tot_Stock_Area_OnSite','PSites_Grd_tot_Stock_Area_OnSite_Pro_flg','PSites_Grd_tot_Offsite_Stock_room_area_OSSR','PSites_Last_Upt_Grd_tot_Offsite_Stock_room_area_OSSR','PSites_Grd_tot_OSSR_Pro_flg','PSites_Grd_tot_Void_Space_not_easily_rec_to_Sal_area','PSites_Last_Upt_Grd_tot_Void_Space_not_easily_rec_to_Sal_area','PSites_Grd_tot_Void_SpnoteasilyrecotoSales_area_Pro_flg','PSites_Grd_tot_Void_Space_easily_rec_to_Sal_area','PSites_Last_Upt_Grd_tot_Void_Space_easily_rec_to_Sal_area','PSites_Grd_tot_Void_Spaceeasilyrecoto_Sales_area_Pro_flg','PSites_Grd_tot_Staff_Area','PSites_Last_Upt_Grd_tot_Staff_Area','PSites_Grd_tot_Staff_Area_Pro_flg','PSites_Grd_tot_Non_store_OfficeTraining_area','PSites_Last_Upt_Grd_tot_Non_store_OffiTraing_area','PSites_Grd_tot_Non_store_OfficeTraining_area_Pro_flg','PSites_Grd_tot_Plant_Room_area','PSites_Last_Upt_Grd_tot_Plant_Room_area','PSites_Grd_tot_Plant_Room_area_Pro_flg','PSites_Grd_tot_Customer_Toilets_area','PSites_Last_Upt_Grd_tot_Cust_Toilets_area','PSites_Grd_tot_Customer_Toilets_area_Pro_flg','PSites_Grd_tot_Third_Party1_area','PSites_Last_Upt_Grd_tot_Third_Party1_area','PSites_Grd_tot_Third_Party1_area_Provisonal_flg','PSites_Grd_tot_Third_Party2_area','PSites_Last_Upt_Grd_tot_Third_Party2_area','PSites_Grd_tot_Third_Party2_area_Provisonal_flg','PSites_Grd_tot_Third_Party3_area','PSites_Last_Upt_Grd_tot_Third_Party3_area','PSites_Grd_tot_Third_Party3_area_Provisonal_flg','PSites_Grd_tot_Third_Party4_area','PSites_Last_Upt_Grd_tot_Third_Party4_area','PSites_Grd_tot_Third_Party4_area_Provisonal_flg','PSites_Grd_tot_Third_Party5_area','PSites_Last_Upt_Grd_tot_Third_Party5_area','PSites_Grd_tot_Third_Party5_area_Provisonal_flg','PSites_Grd_tot_Third_Party6_area','PSites_Last_Upt_Grd_tot_Third_Party6_area','PSites_Grd_tot_Third_Party6_area_Provisonal_flg','PSites_Grd_tot_Third_Party7_area','PSites_Last_Upt_Grd_tot_Third_Party7_area','PSites_Grd_tot_Third_Party7_area_Provisonal_flg','PSites_Grd_tot_Third_Party8_area','PSites_Last_Upt_Grd_tot_Third_Party8_area','PSites_Grd_tot_Third_Party8_area_Provisonal_flg','PSites_Grd_tot_Third_Party9_area','PSites_Last_Upt_Grd_tot_Third_Party9_area','PSites_Grd_tot_Third_Party9_area_Provisonal_flg','PSites_Grd_tot_Third_Party10_area','PSites_Last_Upt_Grd_tot_Third_Party10_area','PSites_Grd_tot_Third_Party10_area_Provisonal_flg','PSites_Grd_tot_Third_Party11_area','PSites_Last_Upt_Grd_tot_Third_Party11_area','PSites_Grd_tot_Third_Party11_area_Provisonal_flg','PSites_Grd_tot_Third_Party12_area','PSites_Last_Upt_Grd_tot_Third_Party12_area','PSites_Grd_tot_Third_Party12_area_Provisonal_flg','PSites_Grd_tot_Third_Party13_area','PSites_Last_Upt_Grd_tot_Third_Party13_area','PSites_Grd_tot_Third_Party13_area_Provisonal_flg','PSites_Grd_tot_Third_Party14_area','PSites_Last_Upt_Grd_tot_Third_Party14_area','PSites_Grd_tot_Third_Party14_area_Provisonal_flg','PSites_Grd_tot_Third_Party15_area','PSites_Last_Upt_Grd_tot_Third_Party15_area','PSites_Grd_tot_Third_Party15_area_Provisonal_flg','PSites_Grd_tot_Third_Party16_area','PSites_Last_Upt_Grd_tot_Third_Party16_area','PSites_Grd_tot_Third_Party16_area_Provisonal_flg','PSites_Grd_tot_Third_Party17_area','PSites_Last_Upt_Grd_tot_Third_Party17_area','PSites_Grd_tot_Third_Party17_area_Provisonal_flg','PSites_Grd_tot_Third_Party18_area','PSites_Last_Upt_Grd_tot_Third_Party18_area','PSites_Grd_tot_Third_Party18_area_Provisonal_flg','PSites_Grd_tot_Third_Party19_area','PSites_Last_Upt_Grd_tot_Third_Party19_area','PSites_Grd_tot_Third_Party19_area_Provisonal_flg','PSites_Grd_tot_Third_Party20_area','PSites_Last_Upt_Grd_tot_Third_Party20_area','PSites_Grd_tot_Third_Party20_area_Provisonal_flg','PSites_Grd_tot_Third_Party21_area','PSites_Last_Upt_Grd_tot_Third_Party21_area','PSites_Grd_tot_Third_Party21_area_Provisonal_flg','PSites_Grd_tot_Third_Party22_area','PSites_Last_Upt_Grd_tot_Third_Party22_area','PSites_Grd_tot_Third_Party22_area_Provisonal_flg','PSites_Non_Dispensing_Sales_Area_NDSA_Sub-basement','PSites_NDSA_Sub-basement_Pro_flg','PSites_Non_Dispensing_Sales_Area_NDSA_Basement','PSites_NDSA_Basement_Pro_flg','PSites_Non_Dispensing_Sales_Area_NDSA_Ground','PSites_NDSA_Ground_Pro_flg','PSites_Non_Dispensing_Sales_Area_NDSA_First','PSites_NDSA_First_Pro_flg','PSites_Non_Dispensing_Sales_Area_NDSA_Second','PSites_NDSA_Second_Pro_flg','PSites_Non_Dispensing_Sales_Area_NDSA_Third','PSites_NDSA_Third_Pro_flg','PSites_Non_Dispensing_Sales_Area_NDSA_Fourth','PSites_NDSA_Fourth_Pro_flg','PSites_Alliance_Healthcare_DC_Desc','PSites_Area_Name','PSites_Benchmark_Group_Item_Description','PSites_Benchmark_Group_Sales_Desc','PSites_CACI_Classification_Name','PSites_Calendar_Desc','PSites_CDC_Long_Name','PFloorDetail_Ceiling_Type_Description','PSites_Chain_Letter_Description','PSites_Chilled_Food_Depot_Desc','PTax_Country_Name','PSites_POS_Currency_Desc','PSites_Delivery_Chain_Desc','PFloorDept_Dept_Name','PSites_Dispensary_Type_Desc','PSites_Distribution_Channel_Desc','PSites_Division_Name','PEscalators_Escalator_Type_Desc','PExtOrg_Ext_Org_Name','PFloorDept_Floor_Desc','PSites_Language_Desc','PSites_Management_Origin_Description','PSites_NHS_Contract_Desc','PSites_Non-LFL_Description','PMerchant_Payment_card_Desc','PSites_Pharmacy_Registration_Status','PSites_POS_Inbound_Profile_Desc','PSites_Price_Band_Desc','PSites_Primary_Care_Organisation_Name','PSites_Reference_Site_Name','PSites_Region_Name','PSites_Sales_Organisation_Desc','PSites_SAP_Company_Name','PSites_SAP_Site_Profile_Description','PSites_SD_Country_Name','PSecurity_Security_Cladding_Type_Desc','PSites_Format_Desc','PSites_Site_Status_Description','PSites_SiteType_Description','PSites_Stores_Type_Desc','PSites_Supply_Region_Desc','PTax_Tax_category_Description','PTax_Tax_classification_Description','PWindow_Window_Type_Desc','PSites_County_Name','PSites_Purchasing_Organisation_Desc','PSites_POS_Outbound_Profile_decs']

# COMMAND ----------

for ReqCol in ReqColumnlist:
    Window_DF = Window_DF.withColumn(ReqCol, detect_data(ReqCol, Window_DF, StringType()))

# COMMAND ----------

CurateStageDF=Window_DF.select([col(c) for c in ReqColumnlist])

# COMMAND ----------

#Write final data to curatestage layer as parquet
CurateStageDF.write.mode('append').format("parquet").save(dest_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Execute Reference update

# COMMAND ----------

dbutils.notebook.run("RPI_SAPMDM_REF_UPDATE_INCREMENTAL", 12000, {'RefSourcePath':dest_path})

# COMMAND ----------

#exit notebook with table names
DestinationCount=CurateStageDF.count()
dbutils.notebook.exit({"SourceCount":DestinationCount,"DestinationCount":DestinationCount,"DestinationTableList":""})