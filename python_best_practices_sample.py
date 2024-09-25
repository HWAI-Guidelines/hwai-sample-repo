# All content, trademarks, and data on this document are the property of Healthworks Analytics, LLC and are protected by applicable intellectual property laws. Unauthorized use, reproduction, or distribution of this material is strictly prohibited.
# Module Intro: This Module is for the Data Processing of DCM Tactic.
# Raw Data Source: Google Big Query.
# Clean File Destination: GCS Bucket & Eventually creates a Big Query Table.

# Input File: Data will be Extracted from Big Query Table based on User Input Parameter {"start_date", "End_date", refresh_qtr}.
# Output File: Qc and Final Output will go into the Gcs Bucket and Eventually to Big_Query.

# Add Suffix refresh_qtr_bda_start_date_bda_end_date
# Data will directly go into the BQ instead of External Table.
# Prepare Comparison sheet for RAW Vs Final Output.
# Put Product-code as NA for groupby QC.
# Eventually Comparison report should be compared with Qtr on Qtr.

# Importing Libraries.
import os
import time
import logging
import numpy as np
import pandas as pd
from pandas.io import gbq
from google.cloud import storage
from credentials import send_mail
from google.cloud import bigquery
from credentials import send_email_with_exception

# Initiating Logger.
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] --> %(asctime)s --> %(filename)s  --> %(funcName)s --> %(lineno)d --> %(message)s",
    handlers=[
        logging.FileHandler("dcm.log"),
        logging.StreamHandler()
    ]
)

# Path to json file having credentials.
#(this json file is created when assiging a new key in the IAM-> Service Accounts-> in the gcp platform.
# This Json File is not need in the Cloud run, please comment it before you deploy to cloud run.
PATH = os.path.join(os.getcwd(), 'ai-media-marketplace-0661827a36eb.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH

# Defining Query to retrive data for current quarter.
query =  """ SELECT dma,
                    campaign_id,
                    buy,
                    bda_start_week,
                    bda_end_week,
                    client_code,
                    product_code,
                    fiscal_year,
                    quarter_month,
                    campaign_name,
                    media_channel,
                    media_subchannel,
                    campaign_objective,
                    campaign_target,
                    estimate_description,
                    estimate_number,
                    product_code_brand_name ,
                    sum(impressions) as impressions           
             FROM `ai-media-marketplace.digital_bda_derived.dcm` WHERE bda_start_week >= '{0}' AND bda_end_week <= '{1}'
             AND brand LIKE '%OMD' AND buy NOT LIKE '%BidManager%' AND buy NOT LIKE '%DART%'
             GROUP BY
                    dma,
                    campaign_id,
                    buy,
                    bda_start_week,
                    bda_end_week,
                    client_code,
                    product_code,
                    fiscal_year,
                    quarter_month,
                    campaign_name,
                    media_channel,
                    media_subchannel,
                    campaign_objective,
                    campaign_target,
                    estimate_description,
                    estimate_number,
                    product_code_brand_name
         """
# Defining Query for One to many mapping for Campaign_Id X Buy
query2 = """
            SELECT * from
            (
                SELECT campaign_id, COUNT(distinct (buy)) as count
                FROM
                `ai-media-marketplace.digital_bda_derived.dcm`
                WHERE bda_start_week>= '{0}'
                AND   bda_end_week <= '{1}'
                AND brand LIKE '%OMD' AND buy NOT LIKE '%BidManager%' AND buy NOT LIKE '%DART%'
                GROUP BY campaign_id 
            )
            where count >1
        """

# Defining Query for One to many mapping for Buy X Campaign_Id
query3 = """
            SELECT * from
            (
                SELECT buy, COUNT(distinct (campaign_id)) as count
                FROM
                `ai-media-marketplace.digital_bda_derived.dcm`
                WHERE bda_start_week>= '{0}'
                AND   bda_end_week <= '{1}'
                AND brand LIKE '%OMD' AND buy NOT LIKE '%BidManager%' AND buy NOT LIKE '%DART%'
                GROUP BY buy
            )
            where count >1
        """

# Defining Input Parameter.
param = ['start_date', 'end_date', 'refresh_qtr']

# Defining Path for Intermediate and Final Output File.
mapping_file_path = "gs://media-model-storage/mpz/DCM/Mapping_Files/"
qc_file_path = "gs://media-model-storage/mpz/DCM/Qc_Files/"
final_output_path = "gs://media-model-storage/mpz/DCM/Final_Output_Files/"
final_output_BQ_path = "gs://media-model-storage/mpz/DCM/Final_Output_BQ/"
exceptions_file_path = "gs://media-model-storage/mpz/DCM/Exceptions/"
hierarchy_file_path = "gs://media-model-storage/mpz/Hierarchy_Files/"

# Defining Function to remove Leading & Trailing Spaces.
def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe.
    
    Input: DataFrame to be Trimmed.

    Return: DataFrame with trimmed values.
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)

# Function for checking file is already 
def if_file_exists(name,bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    stats = storage.Blob.from_string(f"gs://{bucket_name}/mpz/DCM/Final_Output_BQ/{name}").exists(storage_client)
    return stats

# Defining data cleaning funcion.
def do_data_cleaning(ipd):
    """
    This Function takes the input from user --> extracts the data --> Process the File.
    
    Input Parameter: {"start_date":"2021-03-29", "end_date":"2021-06-13", "refresh_qtr":"FY21Q4"}.
    
    Output Files : QC Files & Final Output Files.
    
    Returns: Code Status.
    """
    try:
        # Defining QC Status.
        Qc_status = True

        # Fetching Data based on user Input from Big Query Table.
        try:
            logging.info('Trying to extract the data from Big Query Table')
            start = time.time()
            df = gbq.read_gbq(query.format(ipd['start_date'],ipd['end_date']), project_id="ai-media-marketplace")
            end = time.time()
            logging.info('Time taken to read data from Big Query: {} Secs'.format(round(abs(start-end)),2))
            logging.info('Successfully got the data, Data Dimension: {}\n'.format(df.shape))
        except Exception as e:
            logging.error(e)
            raise e
        
        # Reading Donovon Mapping File.
        try:
            logging.info("Reading Donovon Mapping File")
            donovon_mapping = pd.read_csv(mapping_file_path + "Donovon_Mapping_File.csv", encoding="latin-1")
            logging.info("Successfully read Donovon Mapping File")
            logging.info("Donovon Mapping File Dimension: {0}".format(donovon_mapping.shape))
            logging.info("Donovon Mapping File Columns: {0}\n".format(donovon_mapping.columns))
        except Exception as e:
            logging.error(e)
            raise(e)

        # Reading Hierarchy Mapping File.
        try:
            logging.info("Reading Hierarchy Mapping File")
            hierarchy_mapping = pd.read_csv(hierarchy_file_path + "dcm.csv")
            hierarchy_mapping = hierarchy_mapping.loc[:, ~hierarchy_mapping.columns.isin(['product_code_brand_name', 'buy'])]
            logging.info("Successfully read Dcm Hierarchy Mapping File")
            logging.info("Hierarchy Mapping File Dimension: {0}".format(hierarchy_mapping.shape))
            logging.info("Hierarchy Mapping File Columns: {0}\n".format(hierarchy_mapping.columns))
        except Exception as e:
            logging.error(e)
            raise(e)

        # Checking Memory usage in df.
        try:
            logging.info('Checking memory usage by dataframe')
            memStats= df.memory_usage()
            logging.info("Memory consumption in Gegabytes(GB): %2.2f GB\n"%(memStats/1024/1024/1024).sum())
        except Exception as e:
            logging.error(e)
            raise(e)


        # Check- 2 : Checking DataFrame rows, it should be empty.
        try:
            logging.info("Checking Dataframe Rows, It shouldn't be Empty.")
            if df.shape[0]!=0:
                logging.info('DataFrame is Not Empty, Hence Proceeding...\n')
            else:
                logging.info('DatFrame has zero rows, Hence Stopping the process')
                send_mail('''We found zero rows in the table with the user input, please check the user input or BQ table.''','qc_error')
                return 'DataFrame has No Rows, Please Check the Big Query Table'
        except Exception as e:
            logging.error(e)
            raise e

        # ********* General Data Cleaning Block ********* 
        
        # Removing Records where all values are null & where Campaign_Id=='nan'.
        try:
            logging.info('Removing Records where Every Columns are Null')
            df.dropna(how='all', axis=0, inplace=True)
            donovon_mapping.dropna(how='all', axis=0, inplace=True)
            hierarchy_mapping.dropna(how='all', axis=0, inplace=True)
            logging.info('Successfully Removed Null Records Records\n')
        except Exception as e:
            logging.error(e)
            raise e

        # Stripping Column Names to remove Leading & Trailing Spaces.
        try:
            logging.info('Trimming Column Names....')
            df.columns = df.columns.str.strip()
            hierarchy_mapping.columns = hierarchy_mapping.columns.str.strip()
            donovon_mapping.columns = donovon_mapping.columns.str.strip()
            logging.info('Successfully Trimmed Column Names\n')
        except Exception as e:
            logging.error(e)
            raise e

        # Stripping data in the dataframe to remove Leading & Trailing Spaces.
        try:
            logging.info('Trying to remove Leading & Trailing WhiteSpaces')
            df = trim_all_columns(df)
            hierarchy_mapping = trim_all_columns(hierarchy_mapping)
            donovon_mapping = trim_all_columns(donovon_mapping)
            logging.info('Successfully Leading & Trailing WhiteSpaces\n')
        except Exception as e:
            logging.error(e)
            raise e

        # Check-3: Check if Donovon Mapping File has any duplicate primary key.
        try:
            if donovon_mapping.duplicated(subset=['donovon_code']).any():
                logging.info("Found Duplicate in Donovon Mapping File, Please refer the Mapping File")
                send_mail("We have found Duplicate in Donovon Mapping File, Please refer the Mapping File", "qc_error")
                donovon_mapping[donovon_mapping.duplicated(subset=['donovon_code'], keep=False)]['donovon_code'].to_csv(qc_file_path+"QC_Duplicate_Donovon_Key"+ipd["refresh_qtr"]+".csv", index=False)
                return {"called_method":"dcm",
                        "error":"Duplicate Found in Donovon-Mapping-File",
                        "status":"failed"}
            else:
                logging.info("No Duplicates found in Donovon Mapping File, Hence proceeding\n")
        except Exception as e:
            logging.error(e)
            raise e

        # Check-4 : Checking for Empty Product code in Raw DCM Files.
        try:
            logging.info("Checking if Product_Code columns has Nulls in it")
            if df.product_code.isnull().sum()==0:
                logging.info(df.isnull().sum())
                logging.info('Raw Dcm Pull has no nulls in product code column \n')
            else:
                df[pd.isnull(df['product_code'])][['campaign_id','buy','impressions']].groupby(['campaign_id','buy']).sum().reset_index().to_csv(qc_file_path+"QC_Null_Product_Codes"+ipd['refresh_qtr']+".csv",index=False, line_terminator='\n')
                send_mail('''We found Nulls in product_code column in the table for the refresh: {}, please refer the QC Sheet for the same.'''.format(ipd['refresh_qtr']),'qc_error')
                Qc_status = False
        except Exception as e:
            logging.error(e)
            raise e

        # Check-5 : Checking one to many mapping for Campaign_Id X Buy.
        try:
            logging.info('Checking One to Many mapping for campaign_id X Buy')
            qc2 = gbq.read_gbq(query2.format(ipd['start_date'],ipd['end_date']), project_id="ai-media-marketplace")
            logging.info('Rows from Query2: {}'.format(qc2))
            if qc2.shape[0]!=0:
                logging.info('Campaign_Id has more than one Buy in it')
                df[df['campaign_id'].isin(qc2['campaign_id'].unique().tolist())][['campaign_id','product_code','buy','impressions']].groupby(['campaign_id','product_code','buy']).sum().reset_index().to_csv(qc_file_path+"QC_Multiple_Buy_Name"+ipd['refresh_qtr']+".csv",index=False, line_terminator='\n')
                send_mail('''We found one to many mapping of campaign_id X Buy for the refresh: {}, please refer the QC Sheet for the same.'''.format(ipd['refresh_qtr']),'qc_error')
                Qc_status = False
            else:
                logging.info('No Multiple mapping for campaign_id X Buy\n')
        except Exception as e:
            logging.error(e)
            raise e

        # Check-6 : Checking one to many mapping for Buy X Campaign_Id.
        try:
            logging.info('Checking One to Many mapping for Buy X campaign_id')
            qc3 = gbq.read_gbq(query3.format(ipd['start_date'],ipd['end_date']), project_id="ai-media-marketplace")
            logging.info('Rows from Query3: {}'.format(qc3))
            if qc3.shape[0]!=0:
                logging.info('Buy has more than one campaign_Id in it')
                df[df['buy'].isin(qc3['buy'].unique().tolist())][['campaign_id','product_code','buy','impressions']].groupby(['campaign_id','product_code','buy']).sum().reset_index().to_csv(qc_file_path+"QC_Multiple_Campaign_Id"+ipd['refresh_qtr']+".csv",index=False, line_terminator='\n')
                send_mail('''We found one to many mapping of Buy X campaign_id for the refresh: {}, please refer the QC Sheet for the same.'''.format(ipd['refresh_qtr']),'qc_error')
                Qc_status = False
            else:
                logging.info('No Multiple mapping for campaign_id X Buy\n')
        except Exception as e:
            logging.error(e)
            raise e

        # Check-7: Checking Unique Donovon List from dcm raw file & Donovon Mapping File.
        try:
            logging.info("Creating Donovon List & Checking New Donovon codes are Present or Not")
            raw_donovon_lst = df['product_code'].unique().tolist()
            donovon_mapping_list = donovon_mapping['donovon_code'].unique().tolist()
            new_donovon_lst = [i for i in raw_donovon_lst if i not in donovon_mapping_list]
            logging.info("No. of New Account in DCM Raw Extract File: {}".format(len(new_donovon_lst)))
            if len(new_donovon_lst) == 0:
                logging.info('No New Donovon Codes are Present in Raw File, Hence Proceeding...\n')
            else:
                logging.info('New Donvon Codes are  Present in Raw File, Please Update Donovon Mapping File:{}'.format(new_donovon_lst))
                df[df['product_code'].isin(new_donovon_lst)][['product_code', 'buy', 'impressions']].groupby(['product_code', 'buy']).sum().reset_index().to_csv(qc_file_path+"QC_Unmapped_Donovon_Codes"+ipd['refresh_qtr']+".csv",index=False, line_terminator='\n')
                send_mail('''We have found Unmapped Donovon Codes in DCM Raw Exctract for the refresh: {}, please refer the QC Sheet for the same.'''.format(ipd['refresh_qtr']),'qc_error')
                Qc_status == False
        except Exception as e:
            logging.error(e)
            raise e

        # Checking QC_Status and stopping the process if QC Check ==False.
        try:
            logging.info("Checking QC_Status and stopping the process if QC Check ==False else code proceeds, status:{}".format(Qc_status))
            if Qc_status == False:
                logging.info("QC Check is False, Hence stopping the process till the qc check resolves\n\n")
                send_mail('''We found some QC Errors in data, which needs to be resolved for the refresh: {}, please refer the QC Sheet for the same.'''.format(ipd['refresh_qtr']),'qc_error')
                return {"called_method": "dcm_cloud_run",
                        "status": "pending....",
                        "error":"QC Check"}
            else:
                logging.info('ALL QC Checks are cleared, Hence Proceeding....\n')
        except Exception as e:
            logging.error(e)
            raise e

        # Joining Donovon Mapping with Raw DCM File.
        try:
            row_count = df.shape[0]
            logging.info("Raw DCM File Rows before Join with Donovon Mapping: {}".format(df.shape))
            donovon_mapping.drop_duplicates(inplace=True)
            donovon_mapping.rename(columns={'donovon_code':'product_code'}, inplace=True)
            df = pd.merge(df, donovon_mapping, left_on='product_code', right_on = 'product_code', how='left')
            logging.info("Raw DCM File Rows after Join with Donovon Mapping: {}\n".format(df.shape))
        except Exception as e:
            logging.error(e)
            raise e

        # Check-8: Checking count of rows in the dataframe, it should be same after joining.
        try:
            logging.info('Checking count of rows in the dataframe, it should be same after joining')
            if df.shape[0]==row_count:
                logging.info('count of rows in the dataframe is same after joining with donovon mapping file, {0}=={1}\n'.format(df.shape[0], row_count))
            else:
                logging.info('Row count changed after joining with donovon code\n')
                return {"called_method": "dcm_cloud_run",
                        "status": "error",
                        "error":"Row count changed after mapping with donovon code"}
        except Exception as e:
            logging.error(e)
            raise e

        # Check-9: Trying to check if New_Brand_Names has any null.
        try:
            logging.info("Trying to check if New_Brand_Names has any null")
            logging.info(df.columns)
            if df['new_bda_brand_names'].isnull().any():
                logging.info('Please Check the Donovon_Mapping_File, it has nulls in it\n')
                df[pd.isnull(df['new_bda_brand_names'])].to_csv(qc_file_path+"QC_Null_Bda_Brand_Names"+ipd['refresh_qtr']+".csv",index=False, line_terminator='\n')
                return {"called_method": "dcm_cloud_run",
                        "status": "failed",
                        "error":"Found nulls in new_bda_brand_name"}
            else:
                logging.info('No Nulls in New_Brand_Names columns, Hence Proceeding...\n')
        except Exception as e:
            logging.error(e)
            raise e

        # Removing Exceptions in DMA Columns.
        try:
            logging.info("Formatting DMA Column...")
            df['dma'] = df['dma'].str.upper()
            df['dma'] = df['dma'].str.replace("/","-")
            df['dma'] = df['dma'].str.replace(",","-")
            logging.info("Successfully Formatted DMA Column \n")
        except Exception as e:
            logging.error(e)
            raise e

        # Adding columns & arranaging Output schema.
        try:
            logging.info("Formatting Columns as per Output Schema")            
            df = df[['dma','buy','campaign_id','bda_end_week','bda_start_week','impressions','client_code','product_code','fiscal_year','quarter_month',
            'campaign_name','media_channel','media_subchannel','campaign_objective','campaign_target','estimate_description','estimate_number','product_code_brand_name','new_bda_brand_names']]
            logging.info("Formatting Completed, File is ready for QC\n")
        except Exception as e:
            logging.error(e)
            raise e

        # Groupby Impression to convert the data to weekly level.
        try:
            logging.info("Aggregating Impressions at Weekly Level...")
            logging.info('Rows Before Aggregating Impressions: {}'.format(df.shape))
            df_grp_by = df.groupby(df.columns.difference(['impressions']).to_list()).sum().reset_index()
            logging.info('Rows After Aggregating Impressions, {}'.format(df_grp_by.shape))
            logging.info('Columns After Aggregating Impressions, {}\n'.format(df_grp_by.columns))
        except Exception as e:
            logging.error(e)
            raise e

        # Check-10: Check if final file is already present or not.
        # If present abort the process.
        try:
            logging.info('Checking if final file is already present or not...')
            stats = if_file_exists(bucket_name="media-model-storage",name="Final_File_"+ipd['refresh_qtr']+".csv")
            logging.info("File Stats: {}".format(stats))
            if stats == True:
                logging.info('File already present in the Gcs Bucket, Please delete and re-run the process')
                send_mail('Final File for the refresh: {} already present in the Gcs Bucket, Please delete and re-run the process'.format(ipd['refresh_qtr']), 'qc_error')
                return  {"called_method":"DCM Refesh",
                        "error":"final file is already present in the folder",
                        "status":"failed"}
            else:
                logging.info('File not present in the Gcs Bucket,Hence Proceeding')
        except Exception as e:
            logging.error(e)
            raise e

        # Mapping Hierarchy Columns.
        try:
            logging.info('Mapping Hierarchy Columns...Rows: {}'.format(df_grp_by.shape))
            hierarchy_mapping.drop_duplicates(inplace=True)
            df_grp_by = pd.merge(df_grp_by, hierarchy_mapping, how='left', on='campaign_id')
            logging.info(df_grp_by.columns)
            logging.info(df_grp_by.isnull().sum())
            
            # Checking if Hierarchy Columns has any nulls.
            if df_grp_by['Event_Key'].isnull().any():
                logging.info("Some Hierarchy Exceptions are there, please check the mail attachment.")
                df_exceptions = df_grp_by[pd.isnull(df_grp_by['Event_Key'])]
                df_exceptions = df_exceptions[['product_code_brand_name','campaign_id','buy','Event_Key', 'Event_Name', 'Hispanic', 'Campaign','Chapter', 'Brand_chapter', 'MT_brand']]
                df_exceptions.to_csv(exceptions_file_path+'Exceptions_File_'+ipd['refresh_qtr']+'.csv', index=False)
                send_email_with_exception(from_address='ad.eff@teganalytics.com',
                    to_address=['nitesh.kumar@teganalytics.com'],
                    subject='DCM Hierarchy Exceptions',
                    content='<strong>Hi Team, Please find the attached exceptions in DCM for the refresh: {}</strong>'.format(ipd['refresh_qtr']),
                    dataframe=df_exceptions,
                    filename='spreadsheet.csv',
                    filetype='text/csv')
            else:
                logging.info('No Hierarchy Exception Found in DCM')



            # Adding Tactic & Refresh Qtr Column.
            df_grp_by['tactic']= 'DCM'
            df_grp_by['refresh_qtr'] = ipd['refresh_qtr']
            logging.info('Successfully Mapped Hierarchy Columns...Rows: {}'.format(df_grp_by.shape))
        except Exception as e:
            logging.error(e)
            raise e

        # Exporting Final File for QC.s
        try:
            logging.info('Exporting final file for QC')
            df_grp_by.to_csv(final_output_path+'Final_File_'+ipd['refresh_qtr']+'.csv', index=False)
            df['estimate_number'].replace('JAS',999)
            df_grp_by.to_csv(final_output_BQ_path+'Final_File_'+ipd['refresh_qtr']+'.csv', index=False)
            logging.info('Successfully Exported final file for QC')
        except Exception as e:
            logging.error(e)
            raise e


        send_mail('''DCM refresh has been completed successfully for the quarter: {}'''.format(ipd['refresh_qtr']), 'qc_error')

        # Return Cleaning Code Status.
        return {"called_method":"DCM Refesh",
                "refresh_qtr":ipd['refresh_qtr'],
                "status":"success"}

    except Exception as e:
        logging.error(e)
        raise e



# ------------------------------------------------#
# Creating Application End Point and Main Function.
# ------------------------------------------------#

def do_process_dcm(user_input):
    """
    This main Function is to clean files for DCM.

    Input Parameters: start_Date - For Extracting the current Quarter from Big Query Tables (YYYY-MM-DD).
                      end_Date - For Extracting the current Quarter from Big Query Tables (YYYY-MM-DD).
                      refresh_qtr - Refresh Quarter Names (FY21Q4)

    Return : Clean Files for Each Media.
    """
    try:
        # Capturing Json Arguments from API Body.
        try:
            logging.info('Executing Main Function for DCM Refresh')
            args = user_input
            logging.info('Parameter Passed by User:{}\n'.format(args))
        except Exception as e:
            logging.error(e)
            raise e

        # Check-1: Checking if user inputs are correct or not.
        try:
            logging.info('Checking User input parameters are correct or not')
            if set(param) == set(list(args.keys())):
                logging.info('Input Parameters are correct, Hence Proceeding...\n')
            else:
                send_mail('''Please check the parameter you have passed for DCM refresh are incorrect:{}, Please re-run the cloud function with correct parameters.'''.format(args), 'param_error')
                return {"called_method":"dcm_cloud_run",
                        "error":"incorrect parameter passed:{}".format(args),
                        "status":"failed"}
        except Exception as e:
            logging.error(e)
            raise e

        # Calling Data Processing Function.
        try:
            logging.info('Calling Data Processing Function\n')
            status = do_data_cleaning(ipd=args)
            logging.info('\n\nSuccessfully called & executed Data processing function with no errors\n\n')
        except Exception as e:
            logging.error(e)
            raise e

        # Return code Status.
        return status

    except Exception as e:
        logging.error(e)
        send_mail(e, 'code_error')
        raise e
       
def main():
    user_input_dict = {"start_date":"2021-06-14", "end_date":"2021-09-12", "refresh_qtr":"FY22Q1"}
    cleaning_status=do_process_dcm(user_input = user_input_dict)
    logging.info('\ncleaning_status: {}\n'.format(cleaning_status))

if __name__ == '__main__':
    main()
