from google.cloud import bigquery
import pandas as pd

'''
One-time load of BlackCat 2022 NTD reports into Big Query. This will not be automated as we only need to do it once.
 Transfer from GCS bucket to BQ.
 Python takes the excel file from GCS, loads here, copy worksheet by worksheet into BQ
 *Note* Already loaded 2022_rr20_service while testing this script, so it is excluded from the below code.
'''

GCS_FILE_PATH_RAW = "gs://calitp-ntd-report-validation/blackcat_ntd_reports_2022_raw"

def load_excel_data(sheetname):
    df = pd.read_excel(f"{GCS_FILE_PATH_RAW}/NTD_Annual_Report_Rural_2022.xlsx",
                        sheet_name=sheetname,
                        index_col=None)
    return df

# Get data from GCS - RR020 from 2022
rr20_exp_by_mode = load_excel_data(sheetname="Expenses By Mode")
rr20_rev_by_mode = load_excel_data(sheetname="Revenues By Mode")
rr20_fin = load_excel_data(sheetname="Financials - 2")
rr20_safety = load_excel_data(sheetname="Safety")
rr20_other = load_excel_data(sheetname="Other Resources")
rr20_contactinfo = load_excel_data(sheetname="Basics.Contacts")

## Load into "blackcat_raw" BQ tables - we do *not* modify from the original here - 
# ...except for removing illegal symbols from column names
# ...When we do alter data more, those are saved into "_parsed" BQ tables
# Construct a BigQuery client object.
client = bigquery.Client()

# Tables to create and load - we need the df and a string name for the next loop.
dfdict = {"rr20_exp_by_mode": rr20_exp_by_mode, 
          "rr20_rev_by_mode": rr20_rev_by_mode,
          "rr20_fin": rr20_fin, 
          "rr20_safety": rr20_safety, 
          "rr20_other": rr20_other, 
          "rr20_contactinfo": rr20_contactinfo}
          
for k,v in dfdict.items():
    table_id = f"cal-itp-data-infra.blackcat_raw.2022_{k}" # Set table_id 
    
    # Remove spaces and slashes from col names - - they are illegal in BQ
    v.columns = v.columns.str.replace(' ', '')
    v.columns = v.columns.str.replace('/', '_')
    v.columns = v.columns.str.replace('\W+', '')
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
    
    schema = []
    for k2, v2 in schema_dict.items():
        schema.append(bigquery.SchemaField(k2, v2)) 
    
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table) # API request to create table
    print(f"Created table {table.project}.{table.dataset_id}.{table_id}")

    #https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
    job_service = client.load_table_from_dataframe(
        v, table_id)  
    job_service.result()  # Wait for the job to complete.
    table = client.get_table(table_id) # API request to load data

    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")



