# Databricks notebook source
# DBTITLE 0,Libraries
#Import Libraries
from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd

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

DF=spark.read.format("xml").option("rootTag","soapenv:Envelope").option("rowTag","soapenv:Body").option("rowtTag","hr:QuerycaseResponse").option("rowTag","hr:result").load(','.join(SourceList))
DF.printSchema()

# COMMAND ----------

#removing hr: 
DF1 = DF.toDF(*[x.lstrip("hr:") for x in DF.columns])

# COMMAND ----------

if DF1.rdd.isEmpty():
    dbutils.notebook.exit({"SourceCount":0,"DestinationCount":0,"DestinationTableList":""})
    dbutils.notebook.exit ('stop')
else:
    print('Source data is available')

# COMMAND ----------

ReqColumnlist=['CaseNumber','RecordTypeId','RecordTypeName','Origin','CreatedDate','ClosedDate','Case_Age_Days__c','Priority','Status','Transferred_to_HR_Admin__c','Result_Code__c','Fit_to_work_questions__c','Stores_Administration_Change__c','Empowerment_Check__c','Legacy_Data_Flag__c','Reason_level_1__c','Reason_level_2__c','Reason_level_3__c','Reason_level_4__c','Last_flagged_to_ERM__c','Last_Viewed_by_ERM__c','Employee_Number__c','Contacted_by__c','Related_Employee_Staff_Number__c','ContactByEmpNumber__c','Insight__c','PI_Claims__c','Call_Back_No__c','Calling_As__c','Self_Served__c','Service_Info__c','Ranking__c','ER_Manager__c','Case_to_ERM_for__c','Re_flag_to_ERM__c','ERM_Case_Action__c','Outcome__c','Right_or_Rep_present__c','Appeal__c','Appeal_Outcome__c','Tribunal__c','Tribunal_Outcome__c','Reflaged_h__c','DSP_call__c','Potential_Dismissal_Date__c','Date_informed__c','Line_Managers_Email_Address__c','RHRM_Email_Address__c','Pre_2008__c','Current_Leave_to_Remain_Category__c','Leave_to_Remain_Status__c','Current_Leave_to_Remain_Expiry_Date__c','Valid_from_date__c','ILTR_granted_date__c','Name_in_Full__c','Nationality__c','Passport_Number__c','Passport_Issue_Date__c','Passport_Expiry_Date__c','Passport_Place_of_Issue__c','BID_Number__c','Proof_of_Postage_Date__c','UKBA_holding_letter_date__c','ECS_can_confirm_date__c','CoS_Number__c','COS_issue_date__c','Professional_Membership_Number__c','Complete_File__c','Awaiting_info_Expiry__c','Postpone_Expiry__c','Awaiting_Info_ECS_date__c','Postpone_ECS__c','Related_Employee_Contracted_Hours__c','AP_Loyalty__c','Amount_Paid__c','Car_Registration__c','Start_Date_of_Rental__c','End_Date_of_Rental__c','Issuing_Authority__c','Date_of_Offence__c','Notification_Reference_No__c','Boots_or_AP_Pension_Member__c','Trade_Union_Member__c','X12_weeks_from_mat_RTW_h__c','Actual_or_Expected_Date_of_Conf_h__c','Stakeholder_Pension_or_AU_Contrib_Scheme__c','Receiving_Childcare_Vouchers__c','Tier_2__c','Multiple_Births__c','X100_Voucher_sent__c','Voucher_Tracking_Number__c','Status_Last_Updated__c','DataEase_Import__c','Advised_of_Pregnancy__c','MATB1_Form_Received__c','Expected_Date_of_Confinement__c','SMP_Form_Required__c','Qualifying_Date__c','Earliest_Leave_Date__c','Qualifying_Week__c','Maternity_start_date__c','Actual_Date_of_Confinement__c','SMP_End_Date__c','Maternity_Latest_RTW__c','Maternity_RTW_date__c','Maternity_resignation__c','Adoption_Notification_Form_Received__c','Matching_Certificate_Received__c','Expected_Date_of_Child_Placement__c','Actual_Date_of_Child_Placement__c','Adoption_Resignation__c','Adoptive_Leave_Start_Date__c','Adoptive_Leave_End_Date__c','Adoption_Latest_RTW__c','Adoption_return_to_work_date__c','Paternity_Leave_Request_Form_Received__c','Copy_of_MATB1_Form_Received__c','Expected_Date_of_Confinement_Paternity__c','Paternity_Start_Date__c','Paternity_Requested__c','APL_SDF_Form_Received__c','APL_Expected_Date_of_Confinement__c','APL_Expected_End_Date__c','APL_Actual_Date_of_Confinement__c','APL_Start_Date__c','APL_RTW_Date__c','APL_Resignation_Date__c','Mat_Ret_Award_12_Months__c','Mat_Ret_Award_18_Months__c','Adopt_Ret_Award_12_Months__c','Adopt_Ret_Award_18_Months__c','Outside_of_Policy__c','Outside_of_Policy_Detail__c','Signature_1_Line_Manager__c','Signature_2_Director__c','Signature_3_HR_Director__c','Property_Address__c','Inclusion_Date__c','Auth_Form_Emailed_to_Line_Manager__c','Auth_Form_Returned_to_HR__c','Relocation_Pack_Issued__c','H_Number__c','Cost_centre__c','Signed_Acceptance_Form_Rcvd_frm_Employee__c','Guaranteed_Sale_Price_GSP__c','GSP_Offered__c','GSP_Accepted__c','MES_Approved__c','Estate_Agents_Name_and_Address__c','Estate_Agents_Contact_Number__c','Marketing_Price__c','Estate_Agents_Selling_or_Fee__c','Company_Solicitor__c','Employee_Solicitor_Name_Address__c','Date_on_Market__c','Scottish_Property__c','Fixed_Price_or_Over__c','Buy_in_Completed__c','Sold_Subject_to_Contract__c','Sold_Value__c','GSP__c','Profit_or_Loss__c','Exchange_Contracts__c','Completion__c','Sell_on_Agreement__c','TR1_Form__c','SDLT_Form__c','Name_of_1st_Offerer__c','X1st_Offer__c','Date_of_1st_Offer__c','Offer_1_Accepted__c','Name_of_2nd_Offerer__c','X2nd_Offer__c','Date_of_2nd_Offer__c','Offer_2_Accepted__c','Name_of_3rd_Offerer__c','X3rd_Offer__c','Date_of_3rd_Offer__c','Offer_3_Accepted__c','Line_Manager_Supported__c','Line_Manager_Not_Supported__c','BGBF_Approved__c','BGBF_Declined__c','Grant__c','Loan__c','Cheque_Issued__c','Cheque_Number__c','Amount__c','NOK_Entitled_to_2_weeks_Monies__c','NOK_2_Weeks_Monies_Issued__c','NOK_2_weeks_Monies_Amount__c','Final_Monies_Issued__c','Final_Monies_Amount__c','Ill_Health_Retirement_Approved__c','Ill_Health_Declined__c','Ill_Health_Deferred__c','Related_Employee_Title__c','Parent','Subject','Escalated_Team__c','Contact_Phone__c','Contact_Email__c','Issue_Query__c','Cause_Description__c','Action__c','Lesson_Title__c','Device__c','Operating_System__c','Browser__c','Connection__c','Student_Permitted_Hours__c','Related_Case__r_CaseNumber','Contact_Employee_Number__c']

# COMMAND ----------

#Function for adding missing column and converting all columns to string
def detect_data(column, DF, data_type):
    if not column in DF.columns:
        ret = lit(None).cast(data_type)
    else:
        ret = col(column).cast(data_type)

    return ret

for ReqCol in ReqColumnlist:
    DF1 = DF1.withColumn(ReqCol, detect_data(ReqCol, DF1, StringType()))

# COMMAND ----------

#final DF
DF2=DF1.selectExpr('CaseNumber','RecordTypeId','RecordTypeName','Origin','CreatedDate','ClosedDate','Case_Age_Days__c','Priority','Status','Transferred_to_HR_Admin__c','Result_Code__c','Fit_to_work_questions__c','Stores_Administration_Change__c','Empowerment_Check__c','Legacy_Data_Flag__c','Reason_level_1__c','Reason_level_2__c','Reason_level_3__c','Reason_level_4__c','Last_flagged_to_ERM__c','Last_Viewed_by_ERM__c','Employee_Number__c','Contacted_by__c','Related_Employee_Staff_Number__c','ContactByEmpNumber__c','Insight__c','PI_Claims__c','Call_Back_No__c','Calling_As__c','Self_Served__c','Service_Info__c','Ranking__c','ER_Manager__c','Case_to_ERM_for__c','Re_flag_to_ERM__c','ERM_Case_Action__c','Outcome__c','Right_or_Rep_present__c','Appeal__c','Appeal_Outcome__c','Tribunal__c','Tribunal_Outcome__c','Reflaged_h__c','DSP_call__c','Potential_Dismissal_Date__c','Date_informed__c','Line_Managers_Email_Address__c','RHRM_Email_Address__c','Pre_2008__c','Current_Leave_to_Remain_Category__c','Leave_to_Remain_Status__c','Current_Leave_to_Remain_Expiry_Date__c','Valid_from_date__c','ILTR_granted_date__c','Name_in_Full__c','Nationality__c','Passport_Number__c','Passport_Issue_Date__c','Passport_Expiry_Date__c','Passport_Place_of_Issue__c','BID_Number__c','Proof_of_Postage_Date__c','UKBA_holding_letter_date__c','ECS_can_confirm_date__c','CoS_Number__c','COS_issue_date__c','Professional_Membership_Number__c','Complete_File__c','Awaiting_info_Expiry__c','Postpone_Expiry__c','Awaiting_Info_ECS_date__c','Postpone_ECS__c','Related_Employee_Contracted_Hours__c','AP_Loyalty__c','Amount_Paid__c','Car_Registration__c','Start_Date_of_Rental__c','End_Date_of_Rental__c','Issuing_Authority__c','Date_of_Offence__c','Notification_Reference_No__c','Boots_or_AP_Pension_Member__c','Trade_Union_Member__c','X12_weeks_from_mat_RTW_h__c','Actual_or_Expected_Date_of_Conf_h__c','Stakeholder_Pension_or_AU_Contrib_Scheme__c','Receiving_Childcare_Vouchers__c','Tier_2__c','Multiple_Births__c','X100_Voucher_sent__c','Voucher_Tracking_Number__c','Status_Last_Updated__c','DataEase_Import__c','Advised_of_Pregnancy__c','MATB1_Form_Received__c','Expected_Date_of_Confinement__c','SMP_Form_Required__c','Qualifying_Date__c','Earliest_Leave_Date__c','Qualifying_Week__c','Maternity_start_date__c','Actual_Date_of_Confinement__c','SMP_End_Date__c','Maternity_Latest_RTW__c','Maternity_RTW_date__c','Maternity_resignation__c','Adoption_Notification_Form_Received__c','Matching_Certificate_Received__c','Expected_Date_of_Child_Placement__c','Actual_Date_of_Child_Placement__c','Adoption_Resignation__c','Adoptive_Leave_Start_Date__c','Adoptive_Leave_End_Date__c','Adoption_Latest_RTW__c','Adoption_return_to_work_date__c','Paternity_Leave_Request_Form_Received__c','Copy_of_MATB1_Form_Received__c','Expected_Date_of_Confinement_Paternity__c','Paternity_Start_Date__c','Paternity_Requested__c','APL_SDF_Form_Received__c','APL_Expected_Date_of_Confinement__c','APL_Expected_End_Date__c','APL_Actual_Date_of_Confinement__c','APL_Start_Date__c','APL_RTW_Date__c','APL_Resignation_Date__c','Mat_Ret_Award_12_Months__c','Mat_Ret_Award_18_Months__c','Adopt_Ret_Award_12_Months__c','Adopt_Ret_Award_18_Months__c','Outside_of_Policy__c','Outside_of_Policy_Detail__c','Signature_1_Line_Manager__c','Signature_2_Director__c','Signature_3_HR_Director__c','Property_Address__c','Inclusion_Date__c','Auth_Form_Emailed_to_Line_Manager__c','Auth_Form_Returned_to_HR__c','Relocation_Pack_Issued__c','H_Number__c','Cost_centre__c','Signed_Acceptance_Form_Rcvd_frm_Employee__c','Guaranteed_Sale_Price_GSP__c','GSP_Offered__c','GSP_Accepted__c','MES_Approved__c','Estate_Agents_Name_and_Address__c','Estate_Agents_Contact_Number__c','Marketing_Price__c','Estate_Agents_Selling_or_Fee__c','Company_Solicitor__c','Employee_Solicitor_Name_Address__c','Date_on_Market__c','Scottish_Property__c','Fixed_Price_or_Over__c','Buy_in_Completed__c','Sold_Subject_to_Contract__c','Sold_Value__c','GSP__c','Profit_or_Loss__c','Exchange_Contracts__c','Completion__c','Sell_on_Agreement__c','TR1_Form__c','SDLT_Form__c','Name_of_1st_Offerer__c','X1st_Offer__c','Date_of_1st_Offer__c','Offer_1_Accepted__c','Name_of_2nd_Offerer__c','X2nd_Offer__c','Date_of_2nd_Offer__c','Offer_2_Accepted__c','Name_of_3rd_Offerer__c','X3rd_Offer__c','Date_of_3rd_Offer__c','Offer_3_Accepted__c','Line_Manager_Supported__c','Line_Manager_Not_Supported__c','BGBF_Approved__c','BGBF_Declined__c','Grant__c','Loan__c','Cheque_Issued__c','Cheque_Number__c','Amount__c','NOK_Entitled_to_2_weeks_Monies__c','NOK_2_Weeks_Monies_Issued__c','NOK_2_weeks_Monies_Amount__c','Final_Monies_Issued__c','Final_Monies_Amount__c','Ill_Health_Retirement_Approved__c','Ill_Health_Declined__c','Ill_Health_Deferred__c','Related_Employee_Title__c','Parent','Subject','Escalated_Team__c','Contact_Phone__c','Contact_Email__c','Issue_Query__c','Cause_Description__c','Action__c','Lesson_Title__c','Device__c','Operating_System__c','Browser__c','Connection__c').withColumnRenamed('CaseNumber','CASENUMBER').withColumnRenamed('RecordTypeId','RECORDTYPE_ID').withColumnRenamed('Origin','ORIGIN').withColumnRenamed('CreatedDate','CREATEDDATE').withColumnRenamed('ClosedDate','CLOSEDDATE').withColumnRenamed('Case_Age_Days__c','CASE_AGE_DAYS').withColumnRenamed('Priority','PRIORITY').withColumnRenamed('Status','STATUS').withColumnRenamed('Transferred_to_HR_Admin__c','TRANSFERRED_TO_HR_ADMIN').withColumnRenamed('Result_Code__c','RESULT_CODE').withColumnRenamed('Fit_to_work_questions__c','FIT_TO_WORK_QUESTIONS').withColumnRenamed('Stores_Administration_Change__c','STORES_ADMINISTRATION_CHANGE').withColumnRenamed('Empowerment_Check__c','EMPOWERMENT_CHECK').withColumnRenamed('Legacy_Data_Flag__c','LEGACY_DATA_FLAG').withColumnRenamed('Reason_level_1__c','REASON_LEVEL_1').withColumnRenamed('Reason_level_2__c','REASON_LEVEL_2').withColumnRenamed('Reason_level_3__c','REASON_LEVEL_3').withColumnRenamed('Reason_level_4__c','REASON_LEVEL_4').withColumnRenamed('Last_flagged_to_ERM__c','LAST_FLAGGED_TO_ERM').withColumnRenamed('Last_Viewed_by_ERM__c','LAST_VIEWED_BY_ERM').withColumnRenamed('Employee_Number__c','EMPLOYEE_NUMBER').withColumnRenamed('Contacted_by__c','CONTACTED_BY').withColumnRenamed('Related_Employee_Staff_Number__c','RELATED_EMPLOYEE_STAFF_NUMBER').withColumnRenamed('ContactByEmpNumber__c','CONTACTBYEMPNUMBER').withColumnRenamed('Insight__c','INSIGHT').withColumnRenamed('PI_Claims__c','PI_CLAIMS').withColumnRenamed('Call_Back_No__c','CALL_BACK_NO').withColumnRenamed('Calling_As__c','CALLING_AS').withColumnRenamed('Self_Served__c','SELF_SERVED').withColumnRenamed('Service_Info__c','SERVICE_INFO').withColumnRenamed('Ranking__c','RANKING').withColumnRenamed('ER_Manager__c','ER_MANAGER').withColumnRenamed('Case_to_ERM_for__c','CASE_TO_ERM_FOR').withColumnRenamed('Re_flag_to_ERM__c','RE_FLAG_TO_ERM').withColumnRenamed('ERM_Case_Action__c','ERM_CASE_ACTION').withColumnRenamed('Outcome__c','OUTCOME').withColumnRenamed('Right_or_Rep_present__c','RIGHT_OR_REP_PRESENT').withColumnRenamed('Appeal__c','APPEAL').withColumnRenamed('Appeal_Outcome__c','APPEAL_OUTCOME').withColumnRenamed('Tribunal__c','TRIBUNAL').withColumnRenamed('Tribunal_Outcome__c','TRIBUNAL_OUTCOME').withColumnRenamed('Reflaged_h__c','REFLAGGED_H').withColumnRenamed('DSP_call__c','DSP_CALL').withColumnRenamed('Potential_Dismissal_Date__c','POTENTIAL_DISMISSAL_DATE').withColumnRenamed('Date_informed__c','DATE_INFORMED').withColumnRenamed('Line_Managers_Email_Address__c','LINE_MANAGERS_EMAIL_ADDRESS').withColumnRenamed('RHRM_Email_Address__c','RHRM_EMAIL_ADDRESS').withColumnRenamed('Pre_2008__c','PRE_2008').withColumnRenamed('Current_Leave_to_Remain_Category__c','CURRENT_LEAVE_TO_REMAIN_CATEGORY').withColumnRenamed('Leave_to_Remain_Status__c','LEAVE_TO_REMAIN_STATUS').withColumnRenamed('Current_Leave_to_Remain_Expiry_Date__c','CURRENT_LEAVE_TO_REMAIN_EXPIRY_DATE').withColumnRenamed('Valid_from_date__c','VALID_FROM_DATE').withColumnRenamed('ILTR_granted_date__c','ILTR_GRANTED_DATE').withColumnRenamed('Name_in_Full__c','NAME_IN_FULL').withColumnRenamed('Nationality__c','NATIONALITY').withColumnRenamed('Passport_Number__c','PASSPORT_NUMBER').withColumnRenamed('Passport_Issue_Date__c','PASSPORT_ISSUE_DATE').withColumnRenamed('Passport_Expiry_Date__c','PASSPORT_EXPIRY_DATE').withColumnRenamed('Passport_Place_of_Issue__c','PASSPORT_PLACE_OF_ISSUE').withColumnRenamed('BID_Number__c','BID_NUMBER').withColumnRenamed('Proof_of_Postage_Date__c','PROOF_OF_POSTAGE_DATE').withColumnRenamed('UKBA_holding_letter_date__c','UKBA_HOLDING_LETTER_DATE').withColumnRenamed('ECS_can_confirm_date__c','ECS_CAN_CONFIRM_DATE').withColumnRenamed('CoS_Number__c','COS_NUMBER').withColumnRenamed('COS_issue_date__c','COS_ISSUE_DATE').withColumnRenamed('Professional_Membership_Number__c','PROFESSIONAL_MEMBERSHIP_NUMBER').withColumnRenamed('Complete_File__c','COMPLETE_FILE').withColumnRenamed('Awaiting_info_Expiry__c','AWAITING_INFO_EXPIRY').withColumnRenamed('Postpone_Expiry__c','POSTPONE_EXPIRY').withColumnRenamed('Awaiting_Info_ECS_date__c','AWAITING_INFO_ECS_DATE').withColumnRenamed('Postpone_ECS__c','POSTPONE_ECS').withColumnRenamed('Related_Employee_Contracted_Hours__c','RELATED_EMPLOYEE_CONTRACTED_HOURS').withColumnRenamed('AP_Loyalty__c','AP_LOYALTY').withColumnRenamed('Amount_Paid__c','AMOUNT_PAID').withColumnRenamed('Car_Registration__c','CAR_REGISTRATION').withColumnRenamed('Start_Date_of_Rental__c','START_DATE_OF_RENTAL').withColumnRenamed('End_Date_of_Rental__c','END_DATE_OF_RENTAL').withColumnRenamed('Issuing_Authority__c','ISSUING_AUTHORITY').withColumnRenamed('Date_of_Offence__c','DATE_OF_OFFENCE').withColumnRenamed('Notification_Reference_No__c','NOTIFICATION_REFERENCE_NO').withColumnRenamed('Boots_or_AP_Pension_Member__c','BOOTS_OR_AP_PENSION_MEMBER').withColumnRenamed('Trade_Union_Member__c','TRADE_UNION_MEMBER').withColumnRenamed('X12_weeks_from_mat_RTW_h__c','X12_WEEKS_FROM_MAT_RTW_H').withColumnRenamed('Actual_or_Expected_Date_of_Conf_h__c','ACTUAL_OR_EXPECTED_DATE_OF_CONF_H').withColumnRenamed('Stakeholder_Pension_or_AU_Contrib_Scheme__c','STAKEHOLDER_PENSION_OR_AU_CONTRIB_SCHEME').withColumnRenamed('Receiving_Childcare_Vouchers__c','RECEIVING_CHILDCARE_VOUCHERS').withColumnRenamed('Tier_2__c','TIER_2').withColumnRenamed('Multiple_Births__c','MULTIPLE_BIRTHS').withColumnRenamed('X100_Voucher_sent__c','X100_VOUCHER_SENT').withColumnRenamed('Voucher_Tracking_Number__c','VOUCHER_TRACKING_NUMBER').withColumnRenamed('Status_Last_Updated__c','STATUS_LAST_UPDATED').withColumnRenamed('DataEase_Import__c','DATAEASE_IMPORT').withColumnRenamed('Advised_of_Pregnancy__c','ADVISED_OF_PREGNANCY').withColumnRenamed('MATB1_Form_Received__c','MATB1_FORM_RECEIVED').withColumnRenamed('Expected_Date_of_Confinement__c','EXPECTED_DATE_OF_CONFINEMENT').withColumnRenamed('SMP_Form_Required__c','SMP_FORM_REQUIRED').withColumnRenamed('Qualifying_Date__c','QUALIFYING_DATE').withColumnRenamed('Earliest_Leave_Date__c','EARLIEST_LEAVE_DATE').withColumnRenamed('Qualifying_Week__c','QUALIFYING_WEEK').withColumnRenamed('Maternity_start_date__c','MATERNITY_START_DATE').withColumnRenamed('Actual_Date_of_Confinement__c','ACTUAL_DATE_OF_CONFINEMENT').withColumnRenamed('SMP_End_Date__c','SMP_END_DATE').withColumnRenamed('Maternity_Latest_RTW__c','MATERNITY_LATEST_RTW').withColumnRenamed('Maternity_RTW_date__c','MATERNITY_RTW_DATE').withColumnRenamed('Maternity_resignation__c','MATERNITY_RESIGNATION').withColumnRenamed('Adoption_Notification_Form_Received__c','ADOPTION_NOTIFICATION_FORM_RECEIVED').withColumnRenamed('Matching_Certificate_Received__c','MATCHING_CERTIFICATE_RECEIVED').withColumnRenamed('Expected_Date_of_Child_Placement__c','EXPECTED_DATE_OF_CHILD_PLACEMENT').withColumnRenamed('Actual_Date_of_Child_Placement__c','ACTUAL_DATE_OF_CHILD_PLACEMENT').withColumnRenamed('Adoption_Resignation__c','ADOPTION_RESIGNATION').withColumnRenamed('Adoptive_Leave_Start_Date__c','ADOPTIVE_LEAVE_START_DATE').withColumnRenamed('Adoptive_Leave_End_Date__c','ADOPTIVE_LEAVE_END_DATE').withColumnRenamed('Adoption_Latest_RTW__c','ADOPTION_LATEST_RTW').withColumnRenamed('Adoption_return_to_work_date__c','ADOPTION_RETURN_TO_WORK_DATE').withColumnRenamed('Paternity_Leave_Request_Form_Received__c','PATERNITY_LEAVE_REQUEST_FORM_RECEIVED').withColumnRenamed('Copy_of_MATB1_Form_Received__c','COPY_OF_MATB1_FORM_RECEIVED').withColumnRenamed('Expected_Date_of_Confinement_Paternity__c','EXPECTED_DATE_OF_CONFINEMENT_PATERNITY').withColumnRenamed('Paternity_Start_Date__c','PATERNITY_START_DATE').withColumnRenamed('Paternity_Requested__c','PATERNITY_REQUESTED').withColumnRenamed('APL_SDF_Form_Received__c','APL_SDF_FORM_RECEIVED').withColumnRenamed('APL_Expected_Date_of_Confinement__c','APL_EXPECTED_DATE_OF_CONFINEMENT').withColumnRenamed('APL_Expected_End_Date__c','APL_EXPECTED_END_DATE').withColumnRenamed('APL_Actual_Date_of_Confinement__c','APL_ACTUAL_DATE_OF_CONFINEMENT').withColumnRenamed('APL_Start_Date__c','APL_START_DATE').withColumnRenamed('APL_RTW_Date__c','APL_RTW_DATE').withColumnRenamed('APL_Resignation_Date__c','APL_RESIGNATION_DATE').withColumnRenamed('Mat_Ret_Award_12_Months__c','MAT_RET_AWARD_12_MONTHS').withColumnRenamed('Mat_Ret_Award_18_Months__c','MAT_RET_AWARD_18_MONTHS').withColumnRenamed('Adopt_Ret_Award_12_Months__c','ADOPT_RET_AWARD_12_MONTHS').withColumnRenamed('Adopt_Ret_Award_18_Months__c','ADOPT_RET_AWARD_18_MONTHS').withColumnRenamed('Outside_of_Policy__c','OUTSIDE_OF_POLICY').withColumnRenamed('Outside_of_Policy_Detail__c','OUTSIDE_OF_POLICY_DETAIL').withColumnRenamed('Signature_1_Line_Manager__c','SIGNATURE_1_LINE_MANAGER').withColumnRenamed('Signature_2_Director__c','SIGNATURE_2_DIRECTOR').withColumnRenamed('Signature_3_HR_Director__c','SIGNATURE_3_HR_DIRECTOR').withColumnRenamed('Property_Address__c','PROPERTY_ADDRESS').withColumnRenamed('Inclusion_Date__c','INCLUSION_DATE').withColumnRenamed('Auth_Form_Emailed_to_Line_Manager__c','AUTH_FORM_EMAILED_TO_LINE_MANAGER').withColumnRenamed('Auth_Form_Returned_to_HR__c','AUTH_FORM_RETURNED_TO_HR').withColumnRenamed('Relocation_Pack_Issued__c','RELOCATION_PACK_ISSUED').withColumnRenamed('H_Number__c','H_NUMBER').withColumnRenamed('Cost_centre__c','COST_CENTRE').withColumnRenamed('Signed_Acceptance_Form_Rcvd_frm_Employee__c','SIGNED_ACCEPTANCE_FORM_RCVD_FRM_EMPLOYEE').withColumnRenamed('Guaranteed_Sale_Price_GSP__c','GUARANTEED_SALE_PRICE_GSP').withColumnRenamed('GSP_Offered__c','GSP_OFFERED').withColumnRenamed('GSP_Accepted__c','GSP_ACCEPTED').withColumnRenamed('MES_Approved__c','MES_APPROVED').withColumnRenamed('Estate_Agents_Name_and_Address__c','ESTATE_AGENTS_NAME_AND_ADDRESS').withColumnRenamed('Estate_Agents_Contact_Number__c','ESTATE_AGENTS_CONTACT_NUMBER').withColumnRenamed('Marketing_Price__c','MARKETING_PRICE').withColumnRenamed('Estate_Agents_Selling_or_Fee__c','ESTATE_AGENTS_SELLING_OR_FEE').withColumnRenamed('Company_Solicitor__c','COMPANY_SOLICITOR').withColumnRenamed('Employee_Solicitor_Name_Address__c','EMPLOYEE_SOLICITOR_NAME_ADDRESS').withColumnRenamed('Date_on_Market__c','DATE_ON_MARKET').withColumnRenamed('Scottish_Property__c','SCOTTISH_PROPERTY').withColumnRenamed('Fixed_Price_or_Over__c','FIXED_PRICE_OR_OVER').withColumnRenamed('Buy_in_Completed__c','BUY_IN_COMPLETED').withColumnRenamed('Sold_Subject_to_Contract__c','SOLD_SUBJECT_TO_CONTRACT').withColumnRenamed('Sold_Value__c','SOLD_VALUE').withColumnRenamed('GSP__c','GSP').withColumnRenamed('Profit_or_Loss__c','PROFIT_OR_LOSS').withColumnRenamed('Exchange_Contracts__c','EXCHANGE_CONTRACTS').withColumnRenamed('Completion__c','COMPLETION').withColumnRenamed('Sell_on_Agreement__c','SELL_ON_AGREEMENT').withColumnRenamed('TR1_Form__c','TR1_FORM').withColumnRenamed('SDLT_Form__c','SDLT_FORM').withColumnRenamed('Name_of_1st_Offerer__c','NAME_OF_1ST_OFFERER').withColumnRenamed('X1st_Offer__c','X1ST_OFFER').withColumnRenamed('Date_of_1st_Offer__c','DATE_OF_1ST_OFFER').withColumnRenamed('Offer_1_Accepted__c','OFFER_1_ACCEPTED').withColumnRenamed('Name_of_2nd_Offerer__c','NAME_OF_2ND_OFFERER').withColumnRenamed('X2nd_Offer__c','X2ND_OFFER').withColumnRenamed('Date_of_2nd_Offer__c','DATE_OF_2ND_OFFER').withColumnRenamed('Offer_2_Accepted__c','OFFER_2_ACCEPTED').withColumnRenamed('Name_of_3rd_Offerer__c','NAME_OF_3RD_OFFERER').withColumnRenamed('X3rd_Offer__c','X3RD_OFFER').withColumnRenamed('Date_of_3rd_Offer__c','DATE_OF_3RD_OFFER').withColumnRenamed('Offer_3_Accepted__c','OFFER_3_ACCEPTED').withColumnRenamed('Line_Manager_Supported__c','LINE_MANAGER_SUPPORTED').withColumnRenamed('Line_Manager_Not_Supported__c','LINE_MANAGER_NOT_SUPPORTED').withColumnRenamed('BGBF_Approved__c','BGBF_APPROVED').withColumnRenamed('BGBF_Declined__c','BGBF_DECLINED').withColumnRenamed('Grant__c','GRANT_C').withColumnRenamed('Loan__c','LOAN').withColumnRenamed('Cheque_Issued__c','CHEQUE_ISSUED').withColumnRenamed('Cheque_Number__c','CHEQUE_NUMBER').withColumnRenamed('Amount__c','AMOUNT').withColumnRenamed('NOK_Entitled_to_2_weeks_Monies__c','NOK_ENTITLED_TO_2_WEEKS_MONIES').withColumnRenamed('NOK_2_Weeks_Monies_Issued__c','NOK_2_WEEKS_MONIES_ISSUED').withColumnRenamed('NOK_2_weeks_Monies_Amount__c','NOK_2_WEEKS_MONIES_AMOUNT').withColumnRenamed('Final_Monies_Issued__c','FINAL_MONIES_ISSUED').withColumnRenamed('Final_Monies_Amount__c','FINAL_MONIES_AMOUNT').withColumnRenamed('Ill_Health_Retirement_Approved__c','ILL_HEALTH_RETIREMENT_APPROVED').withColumnRenamed('Ill_Health_Declined__c','ILL_HEALTH_DECLINED').withColumnRenamed('Ill_Health_Deferred__c','ILL_HEALTH_DEFERRED').withColumnRenamed('Related_Employee_Title__c','RELATED_EMPLOYEE_TITLE').withColumnRenamed('Parent','PARENT').withColumnRenamed('Subject','SUBJECT').withColumnRenamed('Escalated_Team__c','ESCALATED_TEAM').withColumnRenamed('Contact_Phone__c','CONTACTPHONE').withColumnRenamed('Contact_Email__c','CONTACT_EMAIL').withColumnRenamed('Issue_Query__c','ISSUE_QUERY').withColumnRenamed('Cause_Description__c','CAUSE_DESCRIPTION').withColumnRenamed('Action__c','ACTION').withColumnRenamed('Lesson_Title__c','LESSON_TITLE').withColumnRenamed('Device__c','DEVICE').withColumnRenamed('Operating_System__c','OPERATING_SYSTEM').withColumnRenamed('Browser__c','BROWSER').withColumnRenamed('Connection__c','CONNECTION')

# COMMAND ----------

#contact_employee_number
try:
    DF3=DF1.selectExpr('Contact.*')
    DF3 = DF3.toDF(*[x.lstrip("hr:") for x in DF3.columns])
    class_cols=list(set(DF3.columns))
    for coln in class_cols:
        newcoln='Contact_'+coln
        DF3=DF3.withColumnRenamed(coln,newcoln)
    DF3=DF3.selectExpr('Contact_Employee_Number__c').withColumnRenamed('Contact_Employee_Number__c','CONTACT_EMPLOYEE_NUMBER') 
except:
    DF3=DF1.selectExpr('Contact_Employee_Number__c').withColumnRenamed('Contact_Employee_Number__c','CONTACT_EMPLOYEE_NUMBER') 

# COMMAND ----------

#RecordType_Name
#try:

   # DF4=DF1.selectExpr('RecordType.*')
   # DF4 = DF4.toDF(*[x.lstrip("hr:") for x in DF4.columns])
   # class_cols=list(set(DF4.columns))
    #for coln in class_cols:
        #newcoln='RecordType_'+coln
        #DF4=DF4.withColumnRenamed(coln,newcoln)
    #DF4=DF4.selectExpr('RecordType_Name').withColumnRenamed('RecordType_Name','RECORDTYPE_NAME')
#except:
    #DF4=DF1.selectExpr('RecordType_Name').withColumnRenamed('RecordType_Name','RECORDTYPE_NAME')

# COMMAND ----------

#Related_Case__r_CaseNumber
try:

    DF5=DF1.selectExpr('Related_Case__r.*')
    DF5 = DF5.toDF(*[x.lstrip("hr:") for x in DF5.columns])
    class_cols=list(set(DF5.columns))
    for coln in class_cols:
        newcoln='Related_Case__r_'+coln
        DF5=DF5.withColumnRenamed(coln,newcoln)
    DF5=DF5.selectExpr('Related_Case__r_CaseNumber').withColumnRenamed('Related_Case__r_CaseNumber','RELATED_CASE_CASENUMBER')
except:
    DF5=DF1.selectExpr('Related_Case__r_CaseNumber').withColumnRenamed('Related_Case__r_CaseNumber','RELATED_CASE_CASENUMBER')

# COMMAND ----------

#ReqColumnlist=['CASENUMBER','RELATED_EMPLOYEE_EMPLOYEE_NUMBER','CONTACT_EMPLOYEE_NUMBER','OWNER','RECORDTYPE_ID','RECORDTYPE_NAME','ORIGIN','CREATEDDATE','CLOSEDDATE','CASE_AGE_DAYS','PRIORITY','STATUS','TRANSFERRED_TO_HR_ADMIN','RESULT_CODE','FIT_TO_WORK_QUESTIONS','STORES_ADMINISTRATION_CHANGE','EMPOWERMENT_CHECK','LEGACY_DATA_FLAG','REASON_LEVEL_1','REASON_LEVEL_2','REASON_LEVEL_3','REASON_LEVEL_4','LAST_FLAGGED_TO_ERM','LAST_VIEWED_BY_ERM','EMPLOYEE_NUMBER','CONTACTED_BY','RELATED_EMPLOYEE_STAFF_NUMBER','CONTACTBYEMPNUMBER','INSIGHT','PI_CLAIMS','CALL_BACK_NO','CALLING_AS','SELF_SERVED','SERVICE_INFO','RANKING','ER_MANAGER','CASE_TO_ERM_FOR','RE_FLAG_TO_ERM','ERM_CASE_ACTION','OUTCOME','RIGHT_OR_REP_PRESENT','APPEAL','APPEAL_OUTCOME','TRIBUNAL','TRIBUNAL_OUTCOME','REFLAGGED_H','DSP_CALL','POTENTIAL_DISMISSAL_DATE','DATE_INFORMED','LINE_MANAGERS_EMAIL_ADDRESS','RHRM_EMAIL_ADDRESS','PRE_2008','CURRENT_LEAVE_TO_REMAIN_CATEGORY','LEAVE_TO_REMAIN_STATUS','CURRENT_LEAVE_TO_REMAIN_EXPIRY_DATE','VALID_FROM_DATE','ILTR_GRANTED_DATE','NAME_IN_FULL','NATIONALITY','PASSPORT_NUMBER','PASSPORT_ISSUE_DATE','PASSPORT_EXPIRY_DATE','PASSPORT_PLACE_OF_ISSUE','STUDENT_PERMITTED_HOURS','BID_NUMBER','PROOF_OF_POSTAGE_DATE','UKBA_HOLDING_LETTER_DATE','ECS_CAN_CONFIRM_DATE','COS_NUMBER','COS_ISSUE_DATE','PROFESSIONAL_MEMBERSHIP_NUMBER','COMPLETE_FILE','AWAITING_INFO_EXPIRY','POSTPONE_EXPIRY','AWAITING_INFO_ECS_DATE','POSTPONE_ECS','RELATED_EMPLOYEE_CONTRACTED_HOURS','AP_LOYALTY','AMOUNT_PAID','CAR_REGISTRATION','START_DATE_OF_RENTAL','END_DATE_OF_RENTAL','ISSUING_AUTHORITY','DATE_OF_OFFENCE','NOTIFICATION_REFERENCE_NO','BOOTS_OR_AP_PENSION_MEMBER','TRADE_UNION_MEMBER','X12_WEEKS_FROM_MAT_RTW_H','ACTUAL_OR_EXPECTED_DATE_OF_CONF_H','STAKEHOLDER_PENSION_OR_AU_CONTRIB_SCHEME','RECEIVING_CHILDCARE_VOUCHERS','TIER_2','MULTIPLE_BIRTHS','X100_VOUCHER_SENT','VOUCHER_TRACKING_NUMBER','STATUS_LAST_UPDATED','DATAEASE_IMPORT','ADVISED_OF_PREGNANCY','MATB1_FORM_RECEIVED','EXPECTED_DATE_OF_CONFINEMENT','SMP_FORM_REQUIRED','QUALIFYING_DATE','EARLIEST_LEAVE_DATE','QUALIFYING_WEEK','MATERNITY_START_DATE','ACTUAL_DATE_OF_CONFINEMENT','SMP_END_DATE','MATERNITY_LATEST_RTW','MATERNITY_RTW_DATE','MATERNITY_RESIGNATION','ADOPTION_NOTIFICATION_FORM_RECEIVED','MATCHING_CERTIFICATE_RECEIVED','EXPECTED_DATE_OF_CHILD_PLACEMENT','ACTUAL_DATE_OF_CHILD_PLACEMENT','ADOPTION_RESIGNATION','ADOPTIVE_LEAVE_START_DATE','ADOPTIVE_LEAVE_END_DATE','ADOPTION_LATEST_RTW','ADOPTION_RETURN_TO_WORK_DATE','PATERNITY_LEAVE_REQUEST_FORM_RECEIVED','COPY_OF_MATB1_FORM_RECEIVED','EXPECTED_DATE_OF_CONFINEMENT_PATERNITY','PATERNITY_START_DATE','PATERNITY_REQUESTED','APL_SDF_FORM_RECEIVED','APL_EXPECTED_DATE_OF_CONFINEMENT','APL_EXPECTED_END_DATE','APL_ACTUAL_DATE_OF_CONFINEMENT','APL_START_DATE','APL_RTW_DATE','APL_RESIGNATION_DATE','MAT_RET_AWARD_12_MONTHS','MAT_RET_AWARD_18_MONTHS','ADOPT_RET_AWARD_12_MONTHS','ADOPT_RET_AWARD_18_MONTHS','OUTSIDE_OF_POLICY','OUTSIDE_OF_POLICY_DETAIL','SIGNATURE_1_LINE_MANAGER','SIGNATURE_2_DIRECTOR','SIGNATURE_3_HR_DIRECTOR','PROPERTY_ADDRESS','INCLUSION_DATE','AUTH_FORM_EMAILED_TO_LINE_MANAGER','AUTH_FORM_RETURNED_TO_HR','RELOCATION_PACK_ISSUED','H_NUMBER','COST_CENTRE','SIGNED_ACCEPTANCE_FORM_RCVD_FRM_EMPLOYEE','GUARANTEED_SALE_PRICE_GSP','GSP_OFFERED','GSP_ACCEPTED','MES_APPROVED','ESTATE_AGENTS_NAME_AND_ADDRESS','ESTATE_AGENTS_CONTACT_NUMBER','MARKETING_PRICE','ESTATE_AGENTS_SELLING_OR_FEE','COMPANY_SOLICITOR','EMPLOYEE_SOLICITOR_NAME_ADDRESS','DATE_ON_MARKET','SCOTTISH_PROPERTY','FIXED_PRICE_OR_OVER','BUY_IN_COMPLETED','SOLD_SUBJECT_TO_CONTRACT','SOLD_VALUE','GSP','PROFIT_OR_LOSS','EXCHANGE_CONTRACTS','COMPLETION','SELL_ON_AGREEMENT','TR1_FORM','SDLT_FORM','NAME_OF_1ST_OFFERER','X1ST_OFFER','DATE_OF_1ST_OFFER','OFFER_1_ACCEPTED','NAME_OF_2ND_OFFERER','X2ND_OFFER','DATE_OF_2ND_OFFER','OFFER_2_ACCEPTED','NAME_OF_3RD_OFFERER','X3RD_OFFER','DATE_OF_3RD_OFFER','OFFER_3_ACCEPTED','LINE_MANAGER_SUPPORTED','LINE_MANAGER_NOT_SUPPORTED','BGBF_APPROVED','BGBF_DECLINED','GRANT_C','LOAN','CHEQUE_ISSUED','CHEQUE_NUMBER','AMOUNT','NOK_ENTITLED_TO_2_WEEKS_MONIES','NOK_2_WEEKS_MONIES_ISSUED','NOK_2_WEEKS_MONIES_AMOUNT','FINAL_MONIES_ISSUED','FINAL_MONIES_AMOUNT','ILL_HEALTH_RETIREMENT_APPROVED','ILL_HEALTH_DECLINED','ILL_HEALTH_DEFERRED','RELATED_EMPLOYEE_TITLE','PARENT','SUBJECT','ESCALATED_TEAM','CONTACTPHONE','CONTACT_EMAIL','ISSUE_QUERY','CAUSE_DESCRIPTION','ACTION','LESSON_TITLE','DEVICE','OPERATING_SYSTEM','BROWSER','CONNECTION','RELATED_CASE_CASENUMBER']

# COMMAND ----------

DF_2 = DF2.toPandas()
DF_3 = DF3.toPandas()
#DF_4 = DF4.toPandas()
DF_5 = DF5.toPandas()

#Final_DF1=pd.concat([DF_2, DF_3 ,DF_4 ,DF_5 ], axis=1)
Final_DF1=pd.concat([DF_2, DF_3 ,DF_5 ], axis=1)


# COMMAND ----------

#converting pandas DF to spark DF
Final_DF2=spark.createDataFrame(Final_DF1)

#Convert columns to string 
Final_DF=Final_DF2.select([col(c).cast("string") for c in Final_DF2.columns])

# COMMAND ----------

#Write final data to curatestage layer as parquet
Final_DF.write.mode('append').format("parquet").save(dest_path)

# COMMAND ----------

df=spark.read.parquet(dest_path)
display(df)

# COMMAND ----------

#exit notebook with table names
DestinationCount=Final_DF.count()
dbutils.notebook.exit({"SourceCount":DestinationCount,"DestinationCount":DestinationCount,"DestinationTableList":""})