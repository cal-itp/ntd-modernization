from google.cloud import bigquery, storage
from argparse import ArgumentParser
import pandas as pd
import datetime
import logging
import re


'''Check and load BlackCat 2023 NTD reports into Big Query. 
This script:
- searches for and grabs the most recent raw data file; using their filename suffix (the date downloaded from BlackCat) 
- Lists out the subrecipients in the latest file
- loops over them and adds their data to BigQuery's raw data tables - IF the data is not already there. Checks are included
- before upload into BigQuery, a `date_uploaded` file is added to each dataset

To run:
python check_raw_data.py --form_to_check <form-number>

To run this data for revenue vehicle inventory, type:
python check_raw_data.py --form_to_check "Inventory" --incoming_org_col_name "Organization" --bq_org_col_name "Organization"
 '''

def get_arguments(this_year):
    GCS_FILE_PATH_PARSED = f"gs://calitp-ntd-report-validation/blackcat_ntd_reports_{this_year}_parsed"

    parser = ArgumentParser(description="Filter and grab most recent raw data file, load into BigQuery")
    parser.add_argument('--subrecipients', default=f"{GCS_FILE_PATH_PARSED}/organizations.csv")
    parser.add_argument('--form_to_check')
    parser.add_argument('--incoming_org_col_name', default='Organization Legal Name')
    parser.add_argument('--bq_org_col_name', default='Organization_Legal_Name')

    args = parser.parse_args()
    return args


def write_to_log(logfilename):
    '''
    Creates a logger object that outputs to a log file, to the filename specified,
    and also streams to console.
    '''
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(f'%(asctime)s:%(levelname)s: %(message)s',
                                  datefmt='%y-%m-%d %H:%M:%S')
    file_handler = logging.FileHandler(logfilename)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger


def get_latest_excel(this_year, form_to_check, bucket, subdir):
    # Dict code table to decipher a) forms to files - this lists the BEGINNING of the form name
    form_to_file_dict = {
        "RR-20": f"NTD_Annual_Report_Rural_{this_year}",
        "A-30": f"A_30_Revenue_Vehicle_Report_{this_year}",
        "A-10": f"NTD_Stations_and_Maintenace_Facilities_A10_{this_year}",
        "Inventory": "RevenueVehicles"
    }
    file_prefix = form_to_file_dict.get(form_to_check)
    all_files = []

    for file in list(bucket.list_blobs(prefix=subdir)):
        if file_prefix in file.name:
            f = file.name.split('/')[1]
            fdate = re.search(r'(\d{4}-\d{2}-\d{2})', f).group()
            all_files.append(fdate)
        else:
            pass
    
    all_files.sort(key=lambda date: datetime.datetime.strptime(date, "%Y-%m-%d"), reverse=True)
    latest_date = all_files[0]
    latest_file = f"{subdir}/{file_prefix}_{latest_date}.xlsx"
    return latest_file


def load_excel_data(filepath, sheetname):
    df = pd.read_excel(filepath,
                        sheet_name=sheetname,
                        index_col=None)
    return df


def compare_datasets(form_to_check, form_to_sheets_dict, this_year, latest_file, org, logger, bucket_name, incoming_org_col_name, bq_org_col_name):
    excelsheets = form_to_sheets_dict.get(form_to_check) #get Excel sheetnames depending on form
    bq_form_ref = form_to_check.replace("-","").lower() #this is something like "rr20" for the "RR-20" form_to_check
    
    # Load incoming data, worksheet by worksheet
    for sheet in excelsheets:
        bq_sheet_ref = sheet.replace(" ", "_").replace("/", "_").replace(".", "_").replace("-", "").replace(")", "").replace('(', "").replace('\W+', '').lower()
        logger.info(f"Checking data for {org} from {bq_form_ref}_{bq_sheet_ref}")
        incoming_df = load_excel_data(f"gs://{bucket_name}/{latest_file}",sheetname=sheet)
        incoming_org_data = incoming_df[incoming_df[incoming_org_col_name]== org].copy()
        
        # Now we have only 1 org's data. Remove spaces and slashes from col names - they are illegal in BQ
        incoming_org_data.columns = (incoming_org_data.columns.str.replace(' ', '_', regex=True)
                                    .str.replace('/', '_').str.replace('.', '_', regex=True)
                                     .str.replace('-', '', regex=True)
                                     .str.replace('#', 'num', regex=True)
                                    .str.replace('\W+', '', regex=True) #other things, just strip out
                                    )
        
        # Check what data is already in BQ
        existing_data_query = f"""SELECT * from blackcat_raw.{this_year}_{bq_form_ref}_{bq_sheet_ref}
            WHERE {bq_org_col_name} = '{org}'"""
        client = bigquery.Client()
        bq_data = client.query(existing_data_query).to_dataframe()
        bq_data = bq_data.drop_duplicates()
                
        logger.info(f"Found {len(bq_data)} rows in {bq_form_ref}_{bq_sheet_ref} for {org}")
        
        table_id = f"cal-itp-data-infra.blackcat_raw.{this_year}_{bq_form_ref}_{bq_sheet_ref}"
        table = bigquery.Table(table_id) 
        job_config = bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND"
        )
                
        if (len(bq_data) > 0) and (len(incoming_org_data) > 0 ):
            # Get the data with the latest upload date only - because this table serves as running storage for every report submittal.
            upload_dates = bq_data['date_uploaded'].unique()
            upload_dates.sort()
            bq_data = bq_data[bq_data['date_uploaded']==max(upload_dates)]
            bq_compare = bq_data.drop(['date_uploaded'], axis=1)

            try:
                logger.info("Checking for existing data")
                pd.testing.assert_frame_equal(bq_compare.sort_values(by=bq_compare.columns.tolist())
                                              .reset_index(drop=True), 
                                          incoming_org_data.sort_values(by=incoming_org_data.columns.tolist())
                                              .reset_index(drop=True), 
                                          check_dtype=False)
                logger.info(f"{org} data in {bq_form_ref}_{bq_sheet_ref} is already in BigQuery, not writing.")
                pass
            except Exception as ex:
                logger.info(f"Data tables are not the same, with {type(ex).__name__}: {ex}.")

                incoming_org_data.loc[:, 'date_uploaded'] = pd.to_datetime(datetime.datetime.now().date()) # Add in 'date_uploaded' column 
                job_service = client.load_table_from_dataframe(incoming_org_data, table_id, job_config=job_config)  # API request to load data
                job_service.result()  # Wait for the job to complete.
                table = client.get_table(table_id) 
        elif len(incoming_org_data) == 0:
            logger.info(f"No incoming data for {table_id}, skipping.")
            pass
        else:
            logger.info(f"Did not find existing data in {table_id} for {org}, loading new raw data.")
            incoming_org_data.loc[:, 'date_uploaded'] = pd.to_datetime(datetime.datetime.now().date()) # Add in 'date_uploaded' column 
            job_service = client.load_table_from_dataframe(incoming_org_data, table_id, job_config=job_config)  # API request to load data
            job_service.result()  # Wait for the job to complete.
            table = client.get_table(table_id)     
        
        logger.info(f"Loaded {len(incoming_org_data)} rows and {len(table.schema)} columns to {table_id} for {org}")


def main():
    # Set up the logger object
    logger = write_to_log('load_raw_data_output.log')

    storage_client = storage.Client(project='cal-itp-data-infra')
    bucket_name = "calitp-ntd-report-validation"
    bucket = storage_client.get_bucket(bucket_name)
    this_year=datetime.datetime.now().year 
    args = get_arguments(this_year)
    subdir = f"blackcat_ntd_reports_{this_year}_raw"
    
    #Get incoming raw data -  get latest file that start with the filename for each particular report (e.g., "NTD_Annual_Report_Rural_2023_.xlsx" for the RR-20)
    latest_filename = get_latest_excel(this_year, args.form_to_check, bucket, subdir) 
    logger.info(f"The most recent file found for form {args.form_to_check} is {latest_filename}! Checking it's incoming data.") 

    # Get a worksheet name from the latest filename above - so we can get the subrecipients to load data from.
        # if we're checking the RR-20, pull from the 2nd sheet (skip the 1st contacts info since it doesn't reflect who actually submitted something)
        # all other forms have only 1 worksheet so just grab that one.
    form_to_sheets_dict = {
        "RR-20": ['Basics.Contacts', 'Modes', 'Expenses By Mode', 'Revenues By Mode', 'Financials - 2', 'Service Data', 'Safety', 'Other Resources'],
        "A-30": ['A-30 (Rural) RVI'],
        "A-10": ['PurchaseTranspFacOwnTypes', 'DirectlyOperatedFacOwnTypes'],
        "Inventory": ['Revenue Vehicles']
    }
    if args.form_to_check == "RR-20":
        sheet = form_to_sheets_dict.get(args.form_to_check)[1]
    else:
        sheet = form_to_sheets_dict.get(args.form_to_check)[0]
    
    latest_raw_data = load_excel_data(f"gs://{bucket_name}/{latest_filename}", sheet) #now load the data

    orgs = pd.read_csv(args.subrecipients)
    orgs_submitting = orgs['Organization'].unique() 
    orgs_submitting = [x.strip(' ') for x in orgs_submitting] # I see some whitespaces
    # Get list of orgs in the NTD report submittal 
    orgs_in_file = latest_raw_data[args.incoming_org_col_name].unique()

    # Check and load the data!
    for org in orgs_in_file:
        if org in orgs_submitting:
            compare_datasets(args.form_to_check, form_to_sheets_dict, 
                             this_year, latest_filename, org, logger, bucket_name, 
                             args.incoming_org_col_name, args.bq_org_col_name)
            
    logger.info("Completed loading the most recent NTD reports from BlackCat!")


if __name__ == "__main__":
    main()


   
    

    
