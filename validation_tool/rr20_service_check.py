from google.cloud import bigquery
import pandas as pd
import numpy as np
import datetime
import logging

'''Script for checking RR-20 NTD report for Service Data. 
Grabs data from BigQuery "raw" tables for "this year" and "last year". 
Will write validated data into two places:
- a folder called "gs://calitp-ntd-report-validation/validation_reports_2023"
- BigQuery tables

To run from command line navigate to folder. Type: 
python rr20_service_check.py           
'''


def get_bq_data(client, year, tablename):
    bq_data_query = f"""SELECT * FROM 
          (select *,
          RANK() OVER(PARTITION BY Organization_Legal_Name ORDER BY date_uploaded DESC) rank_date 
        from `cal-itp-data-infra.blackcat_raw.{year}_{tablename}`) s 
        WHERE rank_date = 1;
        """
    
    df = client.query(bq_data_query).to_dataframe()
    df = df.drop_duplicates().drop(['rank_date', 'date_uploaded'], axis=1)
    return df


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


def check_missing_servicedata(df):
    agencies = df['Organization_Legal_Name'].unique()
    
    mask = df['Annual_VRM'].isnull() | df['Annual_VRH'].isnull() | df['Annual_UPT'].isnull() | df['Annual_UPT'].isnull() | df["VOMX"].isnull()
    orgs_missing_data = df[mask]['Organization_Legal_Name'].unique()
    orgs_not_missing_data = list(set(agencies) - set(orgs_missing_data))
    
    output = []
    for x in agencies:
        if x in orgs_missing_data:
            result = "fail"
            check_name = "RR20F-179: Missing service data check"
            mode = ""
            description = ("One or more service data values is missing in these columns. Please revise in BlackCat and resubmit.'Annual VRM', 'Annual VRH', 'Annual UPT','Sponsored UPT', 'VOMX'")
        elif x in orgs_not_missing_data:
            result = "pass"
            check_name = "RR20F-179: Missing service data check"
            mode = ""
            description = ""
        output_line = {"Organization": x,
                    "name_of_check" : check_name,
                    "mode": mode,
                        "value_checked": "Service data columns",
                        "check_status": result,
                        "Description": description}
        output.append(output_line)
    checks = pd.DataFrame(output).sort_values(by="Organization")
    
    return checks


def rr20_ratios(df, variable, threshold, this_year, last_year, logger):
    agencies = df['Organization_Legal_Name'].unique()
    output = []
    for agency in agencies:
        agency_df = df[df['Organization_Legal_Name']==agency]
        logger.info(f"Checking {agency} for {variable} info.")
        if len(agency_df) > 0:
            
            # Check whether data for both years is present
            if (len(agency_df[agency_df['Fiscal_Year']==this_year]) > 0) \
                & (len(agency_df[agency_df['Fiscal_Year']==last_year]) > 0): 

                for mode in agency_df[(agency_df['Fiscal_Year']==this_year)]['Mode'].unique():
                    value_thisyr = (round(agency_df[(agency_df['Mode']==mode) & 
                                                    (agency_df['Fiscal_Year'] == this_year)]
                                                    [variable].unique()[0], 2))
                    if len(agency_df[(agency_df['Mode']==mode) & (agency_df['Fiscal_Year'] == last_year)][variable]) == 0:
                        value_lastyr = 0
                    else:
                        value_lastyr = (round(agency_df[(agency_df['Mode']==mode)
                                          & (agency_df['Fiscal_Year'] == last_year)]
                                  [variable].unique()[0], 2))
                        print(f"Value last yr = {value_lastyr}")
                    
                    if (value_lastyr == 0) and (abs(value_thisyr - value_lastyr) >= threshold):
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} has changed from last year by > = {threshold*100}%, please provide a narrative justification.")
                    elif (value_lastyr != 0) and abs((value_lastyr - value_thisyr)/value_lastyr) >= threshold:
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
            logger.info(f"There is no data for {agency}")
    checks = pd.DataFrame(output).sort_values(by="Organization")
    return checks


def check_single_number(df, variable, this_year, last_year, logger, threshold=None,):
    agencies = df['Organization_Legal_Name'].unique()
    output = []
    for agency in agencies:

        if len(df[df['Organization_Legal_Name']==agency]) > 0:
            logger.info(f"Checking {agency} for {variable} info.")
            # Check whether data for both years is present, if so perform prior yr comparison.
            if (len(df[(df['Organization_Legal_Name']==agency) & (df['Fiscal_Year']==this_year)]) > 0) \
                & (len(df[(df['Organization_Legal_Name']==agency) & (df['Fiscal_Year']==last_year)]) > 0): 

                for mode in df[(df['Organization_Legal_Name'] == agency) & (df['Fiscal_Year']==this_year)]['Mode'].unique():
                    value_thisyr = (round(df[(df['Organization_Legal_Name'] == agency) 
                                          & (df['Mode']==mode)
                                          & (df['Fiscal_Year'] == this_year)]
                                  [variable].unique()[0], 2))
                    # If there's no data for last yr:
                    if len(df[(df['Organization_Legal_Name'] == agency) 
                                          & (df['Mode']==mode)
                                          & (df['Fiscal_Year'] == last_year)][variable]) == 0:
                        value_lastyr = 0
                    else:
                        value_lastyr = (round(df[(df['Organization_Legal_Name'] == agency) 
                                          & (df['Mode']==mode)
                                          & (df['Fiscal_Year'] == last_year)]
                                  [variable].unique()[0], 2))
                    
                    if (round(value_thisyr)==0 and round(value_lastyr) != 0) | (round(value_thisyr)!=0 and round(value_lastyr) == 0):
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} has changed either from or to zero compared to last year. Please provide a narrative justification.")
                    # run only the above check on whether something changed from zero to non-zero, if no threshold is given
                    elif threshold==None:
                        result = "pass"
                        check_name = f"{variable}"
                        mode = mode
                        description = ""
                        pass
                    # also check for pct change, if a threshold parameter is passed into function
                    elif (value_lastyr == 0) and (abs(value_thisyr - value_lastyr) >= threshold):
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} was 0 last year and has changed by > = {threshold*100}%, please provide a narrative justification.")
                    elif (value_lastyr != 0) and abs((value_lastyr - value_thisyr)/value_lastyr) >= threshold:
                        result = "fail"
                        check_name = f"{variable}"
                        mode = mode
                        description = (f"The {variable} for {mode} has changed from last year by {round(abs((value_lastyr - value_thisyr)/value_lastyr)*100, 1)}%; please provide a narrative justification.")                        
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
            logger.info(f"There is no data for {agency}")
    checks = pd.DataFrame(output).sort_values(by="Organization")
    return checks


def main():
    # Set up the logger object
    logger = write_to_log('rr20_servicechecks_log.log')
    this_year=datetime.datetime.now().year
    last_year = this_year-1
    this_date=datetime.datetime.now().date().strftime('%Y-%m-%d') #for suffix on various files

    #Load data from BigQuery:
    # For each org, get the rows with the latest date_uploaded, which is their latest submitted report.
    client = bigquery.Client()
    rr20_service = get_bq_data(client, this_year, "rr20_service_data")
    rr20_exp_by_mode = get_bq_data(client, this_year, "rr20_expenses_by_mode")
    rr20_fin = get_bq_data(client, this_year, "rr20_financials__2")
    rr20_fin2 = rr20_fin[['Organization_Legal_Name', 'Common_Name_Acronym_DBA', 'Fiscal_Year', 'Operating_Capital', 'Fare_Revenues']]
    orgs_q = """SELECT * FROM `cal-itp-data-infra.blackcat_raw.2023_organizations`"""
    orgs = client.query(orgs_q).to_dataframe().drop_duplicates().drop(['date_uploaded'], axis=1)

    # 2022 data was only uploaded once so has slightly different schema
    bq_2022_query = f"""SELECT * FROM `cal-itp-data-infra.blackcat_raw.{last_year}_rr20_service_data`"""
    rr20_service_lastyr = client.query(bq_2022_query).to_dataframe().drop_duplicates()
    exp_2022_query = f"""SELECT * FROM `cal-itp-data-infra.blackcat_raw.{last_year}_rr20_expenses_by_mode`"""
    rr20_exp_by_mode_lastyr = client.query(exp_2022_query).to_dataframe().drop_duplicates()
    fin_2022_query = f"""SELECT * FROM `cal-itp-data-infra.blackcat_raw.{last_year}_rr20_financials__2`"""
    fin_2022 = client.query(fin_2022_query).to_dataframe().drop_duplicates()
    
    # Combine datasets into one, on which to run validation checks. Filter down to only subrecipients.
    service_exp = (rr20_service.merge(orgs, left_on ='Organization_Legal_Name', right_on = 'Organization', 
                          indicator=True).query('_merge == "both"').drop(columns=['_merge', 'Organization'])
                          .merge(rr20_exp_by_mode, on = ['Organization_Legal_Name', 'Common_Name_Acronym_DBA', 'Fiscal_Year', 'Mode']))
    data = service_exp.merge(rr20_fin2, on =['Organization_Legal_Name', 'Common_Name_Acronym_DBA', 'Fiscal_Year', 'Operating_Capital'],
                          indicator=True).query('_merge == "both"').drop(columns=['_merge'])

    data_lastyear = (rr20_service_lastyr.merge(orgs, left_on ='Organization_Legal_Name', right_on = 'Organization', 
                            indicator=True).query('_merge == "both"').drop(columns=['_merge', 'Organization'])
                            .merge(rr20_exp_by_mode_lastyr, on = ['Organization_Legal_Name', 'Fiscal_Year', 'Mode'])
                .sort_values(by="Organization_Legal_Name"))
        
    # 2022: we use the "Common Name" from the service data, if empty then from the expenses table. If neither empty, still use from the service data
    data_lastyear['Common_Name_Acronym_DBA'] = data_lastyear['Common_Name_Acronym_DBA_x'].combine_first(data_lastyear['Common_Name_Acronym_DBA_y'])
    data_lastyear.drop(columns=['Common_Name_Acronym_DBA_x', 'Common_Name_Acronym_DBA_y'], inplace=True)
    # Add in 2022 Fare Revenue data
    data_all_lastyear = data_lastyear.merge(fin_2022, on =['Organization_Legal_Name', 'Fiscal_Year', 'Operating_Capital'],
                          indicator=True).query('_merge == "both"').drop(columns=['_merge'])
    data_all_lastyear['Common_Name_Acronym_DBA'] = data_all_lastyear['Common_Name_Acronym_DBA_x'].combine_first(data_all_lastyear['Common_Name_Acronym_DBA_y'])
    data_all_lastyear.drop(columns=['Common_Name_Acronym_DBA_x', 'Common_Name_Acronym_DBA_y'], inplace=True)
    data_all_lastyear = data_all_lastyear[['Organization_Legal_Name','Common_Name_Acronym_DBA','Fiscal_Year','Mode',
                                           'Annual_VRM','Annual_VRH','Annual_UPT','Sponsored_UPT','VOMX','Operating_Capital',
                                           'Total_Annual_Expenses_By_Mode','Fare_Revenues']]

    # Combine 2022 & 2023
    allyears = pd.concat([data, data_all_lastyear], ignore_index = True)

    # Check for missing data in any of the service data columns. We do this before any other checks...
    # ... because subsequent ones fill NAs with 0's 
    missingdata_check = check_missing_servicedata(allyears)

    # Calculate needed ratios, added as new columns
    numeric_columns = allyears.select_dtypes(include=['number']).columns
    allyears[numeric_columns] = allyears[numeric_columns].fillna(value=0, inplace = False, axis=1)
    
    allyears1 = allyears[allyears['Operating_Capital']=="Operating"]
    # Cost per hr
    allyears2 = (allyears1.groupby(['Organization_Legal_Name', 'Common_Name_Acronym_DBA','Mode', 'Fiscal_Year'], dropna=False)
                       .apply(lambda x: x.assign(cost_per_hr=x['Total_Annual_Expenses_By_Mode']/ x['Annual_VRH']))
                           .reset_index(drop=True))
    # Miles per vehicle
    allyears2 = (allyears2.groupby(['Organization_Legal_Name','Common_Name_Acronym_DBA', 'Mode', 'Fiscal_Year'], dropna=False)
                 .apply(lambda x: x.assign(miles_per_veh=lambda x: x['Annual_VRM'].sum() / x['VOMX']))
                 .reset_index(drop=True))
    # Fare revenues
    allyears2 = (allyears2.groupby(['Organization_Legal_Name','Common_Name_Acronym_DBA', 'Fiscal_Year'], dropna=False)
                 .apply(lambda x: x.assign(fare_rev_per_trip=lambda x: x['Fare_Revenues'].sum() / x['Annual_UPT']))
                 .reset_index(drop=True))
    # Revenue Speed
    allyears2 = (allyears2.groupby(['Organization_Legal_Name','Common_Name_Acronym_DBA', 'Fiscal_Year'], dropna=False)
                 .apply(lambda x: x.assign(rev_speed=lambda x: x['Annual_VRM'] / x['Annual_VRH']))
                 .reset_index(drop=True))
    # Trips per hr
    allyears2 = (allyears2.groupby(['Organization_Legal_Name','Common_Name_Acronym_DBA', 'Fiscal_Year'], dropna=False)
                 .apply(lambda x: x.assign(trips_per_hr=lambda x: x['Annual_UPT'] / x['Annual_VRH']))
                 .reset_index(drop=True))
    

    # Run validation checks
    cph_checks = rr20_ratios(allyears2, 'cost_per_hr', .30, this_year, last_year, logger)
    mpv_checks = rr20_ratios(allyears2, 'miles_per_veh', .20, this_year, last_year, logger)
    vrm_checks = check_single_number(allyears2, 'Annual_VRM', this_year, last_year, logger, threshold=.30)
    frpt_checks = rr20_ratios(allyears2, 'fare_rev_per_trip', .25, this_year, last_year, logger)
    rev_speed_checks = rr20_ratios(allyears2, 'rev_speed', .15, this_year, last_year, logger)
    tph_checks = rr20_ratios(allyears2, 'trips_per_hr', .30, this_year, last_year, logger)
    voms0_check = check_single_number(allyears2, 'VOMX', this_year, last_year, logger)

    # Combine checks into one table
    rr20_checks = pd.concat([missingdata_check, cph_checks, mpv_checks, vrm_checks, 
                             frpt_checks, rev_speed_checks, 
                             tph_checks, voms0_check], ignore_index=True).sort_values(by="Organization")

    GCS_FILE_PATH_VALIDATED = f"gs://calitp-ntd-report-validation/validation_reports_{this_year}" 
    with pd.ExcelWriter(f"{GCS_FILE_PATH_VALIDATED}/rr20_service_check_report_{this_date}.xlsx") as writer:
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

    logger.info(f"RR-20 service data checks conducted on {this_date} is complete!")

if __name__ == "__main__":
    main()
