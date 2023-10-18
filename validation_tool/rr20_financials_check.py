from argparse import ArgumentParser
from google.cloud import bigquery
import pandas as pd
import datetime
import logging

'''Script for checking RR-20 NTD report for Financial Data. 
Grabs data from GCS buckets for "this year" and "last year". 
Writes validated data into:
- a folder called "gs://calitp-ntd-report-validation/validation_reports_2023"

To run from command line with the default datasources, navigate to folder and type: 
python rr20_financials_check.py'''

def get_arguments(this_year):
    """Get the data as input arguments (for now)"""
    parser = ArgumentParser(description="RR-20 service data checks")
    parser.add_argument('--this_year', default=(datetime.datetime.now().year))
    parser.add_argument('--last_year', default=(this_year-1))
    parser.add_argument('--form_to_check', default="RR-20")
    parser.add_argument('--worksheet', default = "Financials - 2")
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


def financial_checks(df, variable, this_year, last_year, logger):
    agencies = df[df['Fiscal_Year']==this_year]['Organization_Legal_Name'].unique()
    output = []

    for agency in agencies:
        if (len(df[(df['Organization_Legal_Name']==agency) & (df['Fiscal_Year']==this_year)]) == 0):
            logger.info(f"There is no data for {agency}")
            continue
        
        ### combine operating/capital rows into sums
        value_thisyr = (round(df[(df['Organization_Legal_Name'] == agency) 
                              & (df['Fiscal_Year'] == this_year)]
                      [variable].unique().sum()))

        if (value_thisyr == 0) and (variable == "FTA_Formula_Grants_for_Rural_Areas_5311"):
            result = "fail"
            check_name = "RR20F-070: no funds"
            description = (f"The §5311 program is not listed as a revenue source in your report in {this_year}, please provide a narrative justification.")
        elif (round(value_thisyr) % 1000 == 0) and (value_thisyr !=0):
            result = "fail"
            check_name = f"Rounded to thousand: {variable}"
            description = (f"{variable} is rounded to the nearest thousand, but should be reported as exact values. Please provide a narrative justification.")

            
        # Check whether data for last year is also present, if so perform prior yr comparisons.
        if len(df[(df['Organization_Legal_Name']==agency) & (df['Fiscal_Year']==last_year)]) == 0:
            value_lastyr = None
            continue

        value_lastyr = (round(df[(df['Organization_Legal_Name'] == agency) 
                              & (df['Fiscal_Year'] == last_year)]
                      [variable].unique().sum()))

        if ((round(value_thisyr)==0 and round(value_lastyr) != 0) | (round(value_thisyr)!=0 and round(value_lastyr) == 0)) and (variable != 'Other_Directly_Generated_Funds'):
            result = "fail"
            check_name = f"Change from 0: {variable}"
            description = f"{variable} funding changed either from or to zero compared to last year. Please provide a narrative justification."
        elif (abs(round(value_lastyr)) == abs(round(value_thisyr))) and (value_thisyr !=0) and (value_lastyr !=0):
            result = "fail"
            check_name = f"Same value: {variable}"
            description = (f"You have identical values for {variable} reported in {this_year} and {last_year}, which is unusual. Please provide a narrative justification.")
        else:
            result = "pass"
            check_name = f"{variable}"
            description = ""
            
        output_line = {"Organization": agency,
               "name_of_check" : check_name,
                "value_checked": f"{this_year} = {value_thisyr}, {last_year} = {value_lastyr}",
                "check_status": result,
                "Description": description}

        output.append(output_line)
    checks = pd.DataFrame(output).sort_values(by="Organization")
    logger.info(f"Ran financial checks on {variable}.")
    return checks


def equal_totals(this_year, df, logger):
    agencies = df['Organization_Legal_Name'].unique()
    output = []

    for agency in agencies:
        agency_df = df[(df['Organization_Legal_Name']==agency) & (df['Fiscal_Year']==this_year)].drop_duplicates()
        
        if len(agency_df) > 0:
            Total_Annual_Revenues_Expended = agency_df[agency_df['Operating_Capital']=='Operating']['Total_Annual_Revenues_Expended'].values[0]
            Total_Annual_Expenses_by_Mode = agency_df[agency_df['Operating_Capital']=='Operating']['Total_Annual_Expenses_by_Mode'].values[0]

            if round(Total_Annual_Revenues_Expended) != round(Total_Annual_Expenses_by_Mode):
                result = "fail"
                check_name = "RR20F-001OA: equal totals"
                description = f"Total_Annual_Revenues_Expended (${Total_Annual_Revenues_Expended}) should, but does not, equal Total_Annual_Expenses_by_Mode (${Total_Annual_Expenses_by_Mode}). Please provide a narrative justification."
            else:
                result = "pass"
                check_name = "RR20F-001OA: equal totals"
                description = ""
            
            output_line = {"Organization": agency,
                       "name_of_check" : check_name,
                        "value_checked": f"Total_Annual_Revenues_Expended = ${Total_Annual_Revenues_Expended},Total_Annual_Expenses_by_Mode = ${Total_Annual_Expenses_by_Mode}",
                        "check_status": result,
                        "Description": description}
            output.append(output_line)
            
        else:
            pass

    checks = pd.DataFrame(output).sort_values(by="Organization")
    logger.info("Ran checks for RR20F-001OA NTD warning on equating revenues & expenses!")
    return checks


def rr20f_001c(df, this_year, logger):
    agencies = df[df['Fiscal_Year']==this_year]['Organization_Legal_Name'].unique()
    output = []

    for agency in agencies:
        agency_df = df[(df['Organization_Legal_Name'] == agency) & (df['Fiscal_Year'] == this_year)]
        df_capital_finances = agency_df[agency_df['Operating_Capital']=='Capital']
        
        if (len(agency_df) == 0):
            print(f"There is no data for {agency}")
            continue
        
        sum_a = df_capital_finances['Total_Annual_Expenses_by_Mode'].values[0]
        start_index = df.columns.get_loc('Other_Directly_Generated_Funds')
        sum_b = (df_capital_finances.iloc[:, start_index:len(agency_df.columns)]
                 .sum(numeric_only=True, axis=1).values[0])
        
        if round(sum_a) == round(sum_b):
            result = "pass"
            check_name = "RR20F-001C: equal totals for capital expenses by mode and funding source expenditures"
            description = ""
        elif round(sum_a) != round(sum_b):
            result = "fail"
            check_name = f"RR20F-001C: equal totals for capital expenses by mode and funding source expenditures"
            description = f"The sum of Total Expenses for all modes for Uses of Capital {sum_a} does not equal the sum of all values entered for Directly Generated, Non-Federal and Federal Government Funds {sum_b} for Uses of Capital. Please revise or explain."
        
        output_line = {"Organization": agency,
               "name_of_check" : check_name,
                "value_checked": f"Total_Annual_Expenses_by_Mode = {sum_a},by funding source = {sum_b}",
                "check_status": result,
                "Description": description}

        output.append(output_line)
    checks = pd.DataFrame(output).sort_values(by="Organization")
    logger.info("Ran checks for RR20F-001C NTD warning on capital expenses!")
    return checks


def rr20f_182(inv_df, fin_df, year):
    agencies = fin_df['Organization_Legal_Name'].unique()
    output = []

    for agency in agencies:
        agency_df = fin_df[(fin_df['Organization_Legal_Name']==agency) & (fin_df['Fiscal_Year']==year)].drop_duplicates()
        agency_inv = inv_df[inv_df['Organization']==agency].drop_duplicates()
        
        if len(agency_df) == 0:
            print(f"There is no data for {agency} in {year}")
            continue
        
        if len(agency_inv) == 0:
            print(f"There is no inventory data for {agency} in {year}")
            continue
            
        total_cap_expenses = agency_df[agency_df['Operating_Capital']=='Capital']['Total_Annual_Expenses_by_Mode'].values[0]
        newfleet_df = agency_inv[(agency_inv['In_Service_Date'].dt.year == year) & (agency_inv['Ownership_Type'].str.contains("OOPA"))].drop_duplicates()

        if (len(newfleet_df) > 0) and (total_cap_expenses != 0):
            result = "pass"
            check_name = "RR20F-182: new fleet has capital expenses"
            description = ""
        elif (len(newfleet_df) > 0) and (total_cap_expenses == 0):
            result = "fail"
            check_name = "RR20F-182: new fleet has capital expenses"
            description = f"There was $0 reported for Funds Expended on Capital for all modes on the RR-20 form, but {len(newfleet_df)} in the reporting year reported as Owned Outright by Public Agency (OOPA) in your inventory. Please provide narrative justification."
        else:
            result = "warning"
            check_name = "RR20F-182: new fleet has capital expenses"
            description = f"Either capital expenses or inventory data is lacking. Check manually."

        output_line = {"Organization": agency,
                   "name_of_check" : check_name,
                    "value_checked": f"New fleet OOPA={len(newfleet_df)}, Total_Annual_Expenses_by_Mode = ${total_cap_expenses}",
                    "check_status": result,
                    "Description": description}
        output.append(output_line)
    checks = pd.DataFrame(output).sort_values(by="Organization")
    return checks
            

def main():
    # Set up the logger object
    logger = write_to_log('rr20_financialchecks_log.log')
    
    ### Load data:
    this_year=datetime.datetime.now().year
    args = get_arguments(this_year)
    last_year = args.last_year
    this_date=datetime.datetime.now().date().strftime('%Y-%m-%d') #for suffix on various files
    
    bq_form_ref = args.form_to_check.replace("-","").lower() #this will convert "RR-20" to "rr20"
    bq_sheet_ref = args.worksheet.replace(" ", "_").replace("/", "_").replace(".", "_").replace("-", "").replace('\W+', '').lower()
    
    # For each org, get the rows with the latest date_uploaded, which is their latest submitted report.
    # 2022 data was only uploaded once so has slightly different schema
    bq_data_query = f"""SELECT * FROM 
        (select *,
        RANK() OVER(PARTITION BY Organization_Legal_Name ORDER BY date_uploaded DESC) rank_date 
        from `cal-itp-data-infra.blackcat_raw.{this_year}_{bq_form_ref}_{bq_sheet_ref}`) s 
        WHERE rank_date = 1;"""
    client = bigquery.Client()
    rr20_financial = client.query(bq_data_query).to_dataframe()
    rr20_financial = rr20_financial.drop_duplicates().drop('rank_date', axis=1)
    logger.info(f"Got {this_year} data from blackcat_raw.{this_year}_{bq_form_ref}_{bq_sheet_ref}, with {len(rr20_financial)} rows.")
    
    bq_2022_query = f"""SELECT * FROM `cal-itp-data-infra.blackcat_raw.{last_year}_{bq_form_ref}_{bq_sheet_ref}` ;"""
    rr20_financial_2022 = client.query(bq_2022_query).to_dataframe()
    rr20_financial_2022 = rr20_financial_2022.drop_duplicates()
    logger.info(f"Got {last_year} data from blackcat_raw.{last_year}_{bq_form_ref}_{bq_sheet_ref}, with {len(rr20_financial_2022)} rows.")

    allyears = pd.concat([rr20_financial, rr20_financial_2022], ignore_index = True)
    numeric_columns = allyears.select_dtypes(include=['number']).columns
    allyears[numeric_columns] = allyears[numeric_columns].fillna(0)

    ### Run validation checks on financial data
    v_5311 = financial_checks(allyears, 'FTA_Formula_Grants_for_Rural_Areas_5311', this_year, last_year, logger)
    v_odg = financial_checks(allyears, 'Other_Directly_Generated_Funds', this_year, last_year, logger)
    v_farerev = financial_checks(allyears, 'Fare_Revenues', this_year, last_year, logger)
    v_equ_totals = equal_totals(this_year, allyears, logger)
    v_cap_expenses = rr20f_001c(allyears, this_year, logger)
    
    # Run validation check against vehicle inventory
    veh_inv_query = f"""SELECT * FROM `cal-itp-data-infra.blackcat_raw.{this_year}_revenue_vehicle_inventory`"""
    veh_inv = client.query(veh_inv_query).to_dataframe()
    logger.info(f"Got {this_year} data from blackcat_raw.{this_year}_revenue_vehicle_inventory, with {len(veh_inv)} rows.")

    numeric_columns = rr20_financial.select_dtypes(include=['number']).columns
    rr20_financial[numeric_columns] = rr20_financial[numeric_columns].fillna(0)
    v_newfleet = rr20f_182(veh_inv, rr20_financial, this_year)
    logger.info("Ran checks for RR20F-182 on whether new fleets show capital expenses!")

    f_checks  = (pd.concat([v_5311, v_odg, v_farerev, v_equ_totals, v_cap_expenses, v_newfleet], ignore_index=True)
                 .sort_values(by="Organization"))

    GCS_FILE_PATH_VALIDATED = f"gs://calitp-ntd-report-validation/validation_reports_{this_year}" 
    with pd.ExcelWriter(f"{GCS_FILE_PATH_VALIDATED}/rr20_financials_check_report_{this_date}.xlsx") as writer:
        f_checks.to_excel(writer, sheet_name="rr20_financial_checks_full", index=False, startrow=2)

        workbook = writer.book
        worksheet = writer.sheets["rr20_financial_checks_full"]
        cell_highlight = workbook.add_format({
            'fg_color': 'yellow',
            'bold': True,
            'border': 1
        })
        report_title = "NTD Data Validation Report"
        title_format = workbook.add_format({
                'bold': True,
                'valign': 'center',
                'align': 'left',
                'font_color': '#1c639e',
                'font_size': 15
                })
        subtitle = "Reduced Reporting RR-20: Financial Validation Warnings"
        subtitle_format = workbook.add_format({
            'bold': True,
            'align': 'left',
            'font_color': 'black',
            'font_size': 19
            })
        
        worksheet.write('A1', report_title, title_format)
        worksheet.merge_range('A2:C2', subtitle, subtitle_format)
        worksheet.write('F3', 'Agency Response', cell_highlight)
        worksheet.write('G3', 'Response Date', cell_highlight)
        worksheet.set_column(0, 1, 35) #cols A-B width
        worksheet.set_column(2, 2, 22) #col C width
        worksheet.set_column(3, 3, 11) #col D width
        worksheet.set_column(4, 6, 53) #col E-G width
        worksheet.freeze_panes('B4')

    logger.info("Finished running checks on RR-20 financial data!")

if __name__ == "__main__":
    main()



    
    