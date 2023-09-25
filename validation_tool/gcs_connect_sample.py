from argparse import ArgumentParser
import pandas as pd
import datetime
import gcsfs

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

    print("Connected to the Google Cloud data!")
    print("RR20 2022 data table sample:")
    print(rr20_service.head())


if __name__ == "__main__":
    main()
