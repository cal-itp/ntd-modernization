from argparse import ArgumentParser
import pandas as pd
import datetime
import gcsfs

'''Script for checking RR-20 NTD reprt. 
Grabs data from GCS buckets for "this year" and "last year". Right now "this year" is manually set to 2022 for testing, will rewrite once 2023 data is live.
Writes validated data into a folder called "gs://calitp-ntd-report-validation/validation_reports_2023"

To run from command line with the default datasources,, navigate to folder and type: 
python rr20_check.py'''

def get_arguments(this_year, last_year):
    GCS_FILE_PATH_LASTYR = f"gs://calitp-ntd-report-validation/blackcat_ntd_reports_{last_year}_raw"
    GCS_FILE_PATH_RAW = f"gs://calitp-ntd-report-validation/blackcat_ntd_reports_{this_year}_raw"
    GCS_FILE_PATH_PARSED = "gs://calitp-ntd-report-validation/blackcat_ntd_reports_2023_parsed" # f"gs://calitp-ntd-report-validation/blackcat_ntd_reports_{this_year}_parsed"

    """Get the data as input arguments (for now)"""
    parser = ArgumentParser(description="RR-20 ratios check")
    parser.add_argument('--this_year', default=(datetime.datetime.now().year)) 
    parser.add_argument('--last_year', default=(this_year-1))
    parser.add_argument('--subrecipients', default=f"{GCS_FILE_PATH_PARSED}/organizations.csv")
    parser.add_argument('--rr20_service_data', default=f"{GCS_FILE_PATH_RAW}/NTD_Annual_Report_Rural_{this_year}.xlsx")
    parser.add_argument('--rr20_service_data_lastyr', default=f"{GCS_FILE_PATH_LASTYR}/NTD_Annual_Report_Rural_{last_year}.xlsx")
    parser.add_argument('--rr20_expenditure_data', default=f"{GCS_FILE_PATH_RAW}/NTD_Annual_Report_Rural_{this_year}.xlsx")
    parser.add_argument('--rr20_expenditure_data_lastyr', default=f"{GCS_FILE_PATH_LASTYR}/NTD_Annual_Report_Rural_{last_year}.xlsx")

    args = parser.parse_args()
    return args


def load_excel_data(filename, sheetname):
    df = pd.read_excel(filename, sheet_name=sheetname,
                            index_col=None)
    return df


def make_ratio_cols(df, numerator, denominator, col_name):
    if col_name is not None:
        # If a user specify a column name, use it
        # Raise error if the column already exists
        if col_name in df.columns:
            raise ValueError(f"Dataframe already has column '{col_name}'")
        else:
            _col_name = col_name
            
    df = (df.groupby(['Organization Legal Name','Mode', 'Fiscal Year'])
          .apply(lambda x: x.assign(**{_col_name:
                 lambda x: x[numerator].sum() / x[denominator]}))
                )
    return df

def rr20_ratios(df, variable, threshold, this_year, last_year):
    agencies = df['Organization Legal Name'].unique()
    output = []
    for agency in agencies:

        if len(df[df['Organization Legal Name']==agency]) > 0:
            
            # Check whether data for both years is present
            if (len(df[(df['Organization Legal Name']==agency) & (df['Fiscal Year']==this_year)]) > 0) \
                & (len(df[(df['Organization Legal Name']==agency) & (df['Fiscal Year']==last_year)]) > 0): 

                for mode in df[df['Organization Legal Name'] == agency]['Mode'].unique():
                    value_thisyr = (round(df[(df['Organization Legal Name'] == agency) 
                                          & (df['Mode']==mode)
                                          & (df['Fiscal Year'] == this_year)]
                                  [variable].unique()[0], 2))
                    value_lastyr = (round(df[(df['Organization Legal Name'] == agency) 
                                          & (df['Mode']==mode)
                                          & (df['Fiscal Year'] == last_year)]
                                  [variable].unique()[0], 2))
                    
                    if (value_lastyr == 0) and (value_thisyr - value_lastyr >= threshold):
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} has changed from last year by > = {threshold*100}%, please provide a narrative justification.")
                    elif abs((value_lastyr - value_thisyr)/value_lastyr) >= threshold:
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} has changed from last year by {round(abs((value_lastyr - value_thisyr)/value_lastyr)*100, 1)}%, please provide a narrative justification.")
                    else:
                        result = "pass"
                        check_name = f"{variable}"
                        mode = mode
                        description = ""

                    output_line = {"Organization": agency,
                                   "name_of_check" : check_name,
                                   "mode": mode,
                                   "value_checked": f"{this_year} = {value_thisyr}, {last_year} = {value_lastyr}",
                                   "check_status": result,
                                   "Description": description}
                    output.append(output_line)
        else:
            print(f"There is no data for {agency}")
    checks = pd.DataFrame(output).sort_values(by="Organization")
    return checks


def check_single_number(df, variable, threshold, this_year, last_year):
    agencies = df['Organization Legal Name'].unique()
    output = []
    for agency in agencies:

        if len(df[df['Organization Legal Name']==agency]) > 0:
        # Check whether data for both years is present, if so perform prior yr comparison.
            if (len(df[(df['Organization Legal Name']==agency) & (df['Fiscal Year']==this_year)]) > 0) \
                & (len(df[(df['Organization Legal Name']==agency) & (df['Fiscal Year']==last_year)]) > 0): 

                for mode in df[df['Organization Legal Name'] == agency]['Mode'].unique():
                    value_thisyr = (round(df[(df['Organization Legal Name'] == agency) 
                                          & (df['Mode']==mode)
                                          & (df['Fiscal Year'] == this_year)]
                                  [variable].unique()[0], 2))
                    value_lastyr = (round(df[(df['Organization Legal Name'] == agency) 
                                          & (df['Mode']==mode)
                                          & (df['Fiscal Year'] == last_year)]
                                  [variable].unique()[0], 2))
                    if (value_lastyr == 0) and (value_thisyr - value_lastyr >= threshold):
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} was 0 last year and has changed by > = {threshold*100}%, please provide a narrative justification.")
                    elif abs((value_lastyr - value_thisyr)/value_lastyr) >= threshold:
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} has changed from last year by {round(abs((value_lastyr - value_thisyr)/value_lastyr)*100, 1)}%; please provide a narrative justification.")
                    elif (round(value_thisyr)==0 and round(value_lastyr) != 0) | (round(value_thisyr)!=0 and round(value_lastyr) == 0):
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} has changed either from or to zero compared to last year. Please provide a narrative justification.")
                    else:
                        result = "pass"
                        check_name = f"{variable}"
                        mode = mode
                        description = ""

                    output_line = {"Organization": agency,
                           "name_of_check" : check_name,
                                   "mode": mode,
                            "value_checked": f"{this_year} = {value_thisyr}, {last_year} = {value_lastyr}",
                            "check_status": result,
                            "Description": description}
                    output.append(output_line)
        else:
            print(f"There is no data for {agency}")
    checks = pd.DataFrame(output).sort_values(by="Organization")
    return checks


def main():
    #Load data:
    this_year=2022 #datetime.datetime.now().year #for testing purposes
    last_year = this_year-1
    
    args = get_arguments(this_year, last_year)
    rr20_service =  load_excel_data(args.rr20_service_data, "Service Data")
    rr20_service_lastyr = load_excel_data(args.rr20_service_data_lastyr, "Service Data")
    rr20_exp_by_mode = load_excel_data(args.rr20_expenditure_data, "Expenses By Mode")
    rr20_exp_by_mode_lastyr = load_excel_data(args.rr20_expenditure_data_lastyr, "Expenses By Mode")
    orgs = pd.read_csv(args.subrecipients)
    
    # Combine datasets into one, on which to run validation checks. Filter down to only subrecipients.
    data = (rr20_service.merge(orgs, left_on ='Organization Legal Name', right_on = 'Organization', 
                            indicator=True).query('_merge == "both"').drop(columns=['_merge', 'Organization'])
                            .merge(rr20_exp_by_mode, on = ['Organization Legal Name', 'Common Name/Acronym/DBA', 'Fiscal Year', 'Mode']))

    data_lastyear = (rr20_service_lastyr.merge(orgs, left_on ='Organization Legal Name', right_on = 'Organization', 
                            indicator=True).query('_merge == "both"').drop(columns=['_merge', 'Organization'])
                            .merge(rr20_exp_by_mode_lastyr, on = ['Organization Legal Name', 'Common Name/Acronym/DBA', 'Fiscal Year', 'Mode']))
    allyears = pd.concat([data, data_lastyear], ignore_index = True)
    numeric_columns = allyears.select_dtypes(include=['number']).columns
    allyears[numeric_columns] = allyears[numeric_columns].fillna(0)


#### Extra airflow job and function will save the above datasets into the "parsed" folder. Skipping for now.###

    # Calculate needed ratios
    allyears = make_ratio_cols(allyears, 'Total Annual Expenses By Mode', 'Annual VRH', 'cost_per_hr')
    allyears = make_ratio_cols(allyears, 'Annual VRM', 'VOMX', 'miles_per_veh')

    # Run validation checks
    cph_checks = rr20_ratios(allyears, 'cost_per_hr', .30, this_year, last_year)
    mpv_checks = rr20_ratios(allyears, 'miles_per_veh', .20, this_year, last_year)
    vrm_checks = check_single_number(allyears, 'Annual VRM', .30, this_year, last_year)

    # Combine checks into one table
    rr20_checks = pd.concat([cph_checks, mpv_checks, vrm_checks], ignore_index=True).sort_values(by="Organization")

    GCS_FILE_PATH_VALIDATED = "gs://calitp-ntd-report-validation/validation_reports_2023" # f"gs://calitp-ntd-report-validation/validation_reports_{this_year}"
    with pd.ExcelWriter(f"{GCS_FILE_PATH_VALIDATED}/rr20_check_report.xlsx") as writer:
        rr20_checks.to_excel(writer, sheet_name="rr20_checks_full", index=False, startrow=2)

        workbook = writer.book
        worksheet = writer.sheets["rr20_checks_full"]
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
        subtitle = "Reduced Reporting RR-20: Validation Warnings"
        subtitle_format = workbook.add_format({
            'bold': True,
            'align': 'left',
            'font_color': 'black',
            'font_size': 19
            })
        
        worksheet.write('A1', report_title, title_format)
        worksheet.merge_range('A2:C2', subtitle, subtitle_format)
        worksheet.write('G3', 'Agency Response', cell_highlight)
        worksheet.write('H3', 'Response Date', cell_highlight)
        worksheet.set_column(0, 0, 35) #col A width
        worksheet.set_column(1, 3, 22) #cols B-D width
        worksheet.set_column(4, 4, 11) #col D width
        worksheet.set_column(5, 6, 53) #col E-G width
        worksheet.freeze_panes('B4')

    print("RR-20 ratios check is complete!")

if __name__ == "__main__":
    main()
