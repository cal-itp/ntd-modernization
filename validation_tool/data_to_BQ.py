from argparse import ArgumentParser
from google.cloud import bigquery, storage
import pandas as pd
import datetime
import logging

'''
One-time load of various files from Google Could storage into Big Query. This will not be automated as we only need to do it once.
 Transfer from GCS bucket to BQ.
 Python takes the excel file from GCS, loads here, copy worksheet by worksheet into BQ
 *Note* Already loaded 2022_rr20_service while testing this script, so it is excluded from the below code.

 Commands used for loading the following data:
  * BlackCat 2022 NTD reports: python data_to_BQ.py --year 2022 --gcs_subdir "blackcat_ntd_reports_2022_raw" --filename "NTD_Annual_Report_Rural_2022.xlsx"
  * 2023 Revenue Vehicle Inventory: python data_to_BQ.py --year 2023 --gcs_subdir "blackcat_ntd_reports_2023_raw" --filename "RevenueVehicles_2023-10-04.xlsx" --worksheet "Revenue Vehicles"
  * list of subrecipients submitting to NTD: python data_to_BQ.py --year 2023 --gcs_subdir "blackcat_ntd_reports_2023_parsed" --filename "organizations.xlsx" --worksheet "organizations"
  * A-30 reports, initial load: python data_to_BQ.py --year 2023 --gcs_subdir "blackcat_ntd_reports_2023_raw" --filename "A_30_Revenue_Vehicle_Report_2023_2023-10-04.xlsx" --worksheet "A-30 (Rural) RVI"
  * A-10 reports, initial load: python data_to_BQ.py --year 2023 --gcs_subdir "blackcat_ntd_reports_2023_raw" --filename "NTD_Stations_and_Maintenace_Facilities_A10_2023_2023-10-17.xlsx"
'''

def get_arguments():
    """Get the data as input arguments (for now)"""
    parser = ArgumentParser(description="Loading data from GCS to BigQuery")
    parser.add_argument('--year', default=2023)
    parser.add_argument('--gcs_subdir')
    parser.add_argument('--filename')
    parser.add_argument('--worksheet', nargs='?',default=None)
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


def load_excel_data(filepath, sheetname):
    df = pd.read_excel(f"{filepath}",
                        sheet_name=sheetname,
                        index_col=None)
    return df



def load_new_table(dfdict, year, client, logger):
    for k,v in dfdict.items():
        table_id = f"cal-itp-data-infra.blackcat_raw.{year}_{k}" # Set table_id 

        # Remove spaces and slashes from col names - - they are illegal in BQ
        v.columns = v.columns.str.replace(' ', '_', regex=True)
        v.columns = v.columns.str.replace('/', '_', regex=True)
        v.columns = v.columns.str.replace('.', '_', regex=True)
        v.columns = v.columns.str.replace('\W+', '', regex=True)
        columns = v.columns.values
        
        # Make dict of colname: BQ type
        schema_dict = {}
        for x in columns:
            if v[x].dtypes == 'float64':
                schema_dict[x] = "FLOAT64"
            elif v[x].dtypes == 'int64':
                schema_dict[x] = "INT64"
            elif v[x].dtypes == 'object':
                schema_dict[x] =  "STRING"
            elif v[x].dtypes == 'datetime64[ns]':
                schema_dict[x] =  "DATETIME"
        
        schema = []
        for k2, v2 in schema_dict.items():
            schema.append(bigquery.SchemaField(k2, v2)) 
        
        table = bigquery.Table(table_id, schema=schema)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE"
        )

        table = client.create_table(table) # API request to create table
        logger.info(f"Created table {table.project}.{table.dataset_id}.{table_id}")

        #https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
        job_service = client.load_table_from_dataframe(
            v, table_id, job_config=job_config)  
        job_service.result()  # Wait for the job to complete.
        table = client.get_table(table_id) # API request to load data

        logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


def main():
    # Set up the logger object
    logger = write_to_log('load_new_data_toBQ_log.log')

    # load in the arguments
    args = get_arguments()
    
    # Construct a BigQuery client object.
    client = bigquery.Client()
    filepath = f"gs://calitp-ntd-report-validation/{args.gcs_subdir}/{args.filename}"
    
    #------------- 2022 RR-20 data. Ran once, then commented out this block.
#     # Get data from GCS - RR020 from 2022
#     rr20_exp_by_mode = load_excel_data(filepath, sheetname="Expenses By Mode")
#     rr20_rev_by_mode = load_excel_data(filepath, sheetname="Revenues By Mode")
#     rr20_fin = load_excel_data(filepath, sheetname="Financials - 2")
#     rr20_safety = load_excel_data(filepath, sheetname="Safety")
#     rr20_other = load_excel_data(filepath, sheetname="Other Resources")
#     rr20_contactinfo = load_excel_data(filepath, sheetname="Basics.Contacts")

#     ## Load into "blackcat_raw" BQ tables - we do *not* modify from the original here - 
#     # ...except for removing illegal symbols from column names
#     # ...When we do alter data more, those are saved into "_parsed" BQ tables
#    # RR-20 2022 data: Tables to create and load - we need the df and a string name for the next loop.
#     dfdict = {"rr20_expenses_by_mode": rr20_exp_by_mode, 
#           "rr20_revenues_by_mode": rr20_rev_by_mode,
#           "rr20_financials__2": rr20_fin, 
#           "rr20_safety": rr20_safety, 
#           "rr20_other_resources": rr20_other, 
#           "rr20_basics_contacts": rr20_contactinfo}
    
#     load_new_table(dfdict, 2022, client, logger) #loading RR-20 tables
    

    #------------- 2023 Vehicle Inventory. Ran once, then commented out this block.
    # Get data from GCS - Revenue Vehicle Inventory from 2023
    rev_veh_inv = load_excel_data(filepath, sheetname=args.worksheet)
    rev_veh_inv.loc[:, 'date_uploaded'] = pd.to_datetime(datetime.datetime.now().date()) # Add in 'date_uploaded' column 
    dfdict_veh_inv = {"inventory_revenue_vehicles": rev_veh_inv}
    
    load_new_table(dfdict_veh_inv, 2023, client, logger)

    #------------- 2023 List of subrecipients submitting to NTD. Ran once, then commented out this block.
    # # Get data from GCS - 
    # orgs = load_excel_data(filepath, sheetname=args.worksheet)
    # orgs.loc[:, 'date_uploaded'] = pd.to_datetime(datetime.datetime.now().date()) # Add in 'date_uploaded' column 
    # dfdict_orgs= {"organizations": orgs}
    
    # load_new_table(dfdict_orgs, 2023, client, logger)

    #------------- 2023 A-30. Ran once, then commented out this block.
    # a30 = load_excel_data(filepath, sheetname=args.worksheet)
    # a30.loc[:, 'date_uploaded'] = pd.to_datetime(datetime.datetime.now().date()) # Add in 'date_uploaded' column 
    # dfdict_a30= {"a30_a30_rural_rvi": a30}
    
    # load_new_table(dfdict_a30, 2023, client, logger)

    #------------- 2023 A-10. Ran once, if new data is added, will comment out this block.
    # a10_PurchaseTranspFacOwnTypes = load_excel_data(filepath, sheetname="PurchaseTranspFacOwnTypes")
    # a10_PurchaseTranspFacOwnTypes.loc[:, 'date_uploaded'] = pd.to_datetime(datetime.datetime.now().date()) # Add in 'date_uploaded' column 
    # a10_DirectlyOperatedFacOwnTypes = load_excel_data(filepath, sheetname="DirectlyOperatedFacOwnTypes")
    # a10_DirectlyOperatedFacOwnTypes.loc[:, 'date_uploaded'] = pd.to_datetime(datetime.datetime.now().date()) # Add in 'date_uploaded' column 
    # dfdict_a10= {"a10_purchasetranspfacowntypes": a10_PurchaseTranspFacOwnTypes,
    #              "a10_directlyoperatedfacowntypes": a10_DirectlyOperatedFacOwnTypes}
    
    # load_new_table(dfdict_a10, 2023, client, logger)


if __name__ == "__main__":
    main()



