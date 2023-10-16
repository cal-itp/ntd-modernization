from argparse import ArgumentParser
import pandas as pd

'''This file loads one dataset (A-10 form) that originates from Black Cat.
To run from command line, navigate to folder: 
* To run with the default datasources, type: python a10_facilities_check.py
* To specify data source file, type: python a10_facilities_check.py --a10_data <filepath> --a10_lastyr_data <filepath> 
                                
Performs 3 checks:
* Check 1. Check that sum of total facilities for each agency, across all modes, is a whole number.
* Check 2. Check that the sum of all total facilities is not zero.
* Check 3. Check whether total gen purpose facilities (all but heavy maintenance) is > 1. If so mark as "failure".

For more details on forms, SEE THIS NOTEBOOK: "2b_a10_validation_development.ipynb"
'''

def get_arguments():
    """Get the data as input arguments (for now)"""
    parser = ArgumentParser(description="VOMS inventory check")
    parser.add_argument('--a10_data', default="data/2021_a10_submitted_partialdata.csv")
    parser.add_argument('--a10_lastyr_data', default = "data/2020_a10_submitted_partialdata.csv")

    args = parser.parse_args()
    return args


def facility_checks(df, this_year, last_year):
    a10_agencies = df['Agency'].unique()

    output = []
    for agency in a10_agencies:
        
        if len(df[df['Agency']==agency]) > 0:
            
            ##Total facilities checks
            total_fac = round(df[(df['Agency']==agency) & ((df['year']==this_year))]['Total Facilities'].sum())
            
            # whole number check
            if total_fac % 1 == 0:
                result = "pass"
                description = ""
                check_name = "Whole Number Facilities"
            else:
                result = "fail"
                description = "The reported total facilities do not add up to a whole number. Please explain."
                check_name = "Whole Number Facilities"
            
            output_line = {"Organization": agency,
                        "name_of_check" : check_name,
                            "value_checked": f"Total Facilities: {total_fac}",
                            "check_status": result,
                            "Description": description}
            output.append(output_line)
            
            # Non-zero check
            if total_fac != 0:
                result = "pass"
                description = ""
                check_name = "Non-zero Facilities"
            else:
                result = "fail"
                description = "There are no reported facilities. Please explain."
                check_name = "Non-zero Facilities"
            
            output_line = {"Organization": agency,
                        "name_of_check" : check_name,
                            "value_checked": f"Total Facilities: {total_fac}",
                            "check_status": result,
                            "Description": description}
            output.append(output_line)
            
            ## General purpose facilities checks (all except "heavy maintenance")
            total_gen_fac = round(df[df['Agency']==agency]
                            [['Under 200 Vehicles', 
                                '200 to 300 Vehicles',
                                'Over 300 Vehicles']].sum().sum())
            
            # check on whether there's >1 gen purpose fac and/or none reported
        if (round(total_gen_fac) <= 1) & (round(total_gen_fac) != 0):
            result = "pass"
            description = ""
            check_name = "Gen Purpose Facilities"
        elif round(total_gen_fac) > 1:
            result = "fail"
            description = "You reported > 1 general purpose facility. Please verify whether this is correct."
            check_name = "Multiple Gen Purpose Facilities"
        elif round(total_gen_fac) == 0:
            result = "fail"
            description = "You reported no general purpose facilities. Please verify whether this is correct."
            check_name = "Non-zero Gen Purpose Facilities"
        else:
            pass
        
        output_line = {"Organization": agency,
                       "name_of_check" : check_name,
                        "value_checked": f"Gen Purpose Facilities: {total_gen_fac}",
                        "check_status": result,
                        "Description": description}
        output.append(output_line)
        
        # Check whether data for both years is present, if so perform prior yr comparison.
        if (len(df[(df['Agency']==agency) & (df['year']==this_year)]) > 0) & (len(df[(df['Agency']==agency) & (df['year']==last_year)]) > 0): 
            
            last_yr_gen_fac = round(df[(df['Agency']==agency) & (df['year']==last_year)]
                                     [['Under 200 Vehicles', 
                                        '200 to 300 Vehicles',
                                        'Over 300 Vehicles']].sum().sum())
             
            if round(total_gen_fac) == round(last_yr_gen_fac):
                result = "pass"
                description = ""
                check_name = "Comparison to last yr: Gen Purpose Facilities"
            else:
                result = "fail"
                description = "Num. of general purpose facilities differs that last year - please verify or clarify."
                check_name = "Comparison to last yr: Gen Purpose Facilities"

            output_line = {"Organization": agency,
                           "name_of_check" : check_name,
                            "value_checked": f"{total_gen_fac} in {this_year}, {last_yr_gen_fac} in {last_year} (Gen Purpose Facilities)", 
                            "check_status": result,
                            "Description": description}
            output.append(output_line)

        else:
             pass
        
    facility_checks = pd.DataFrame(output).sort_values(by="Organization")
    return facility_checks


def main():
    #Load data:
    args = get_arguments()
    df = pd.read_csv(args.a10_data, index_col = 0)
    df_lastyr = pd.read_csv(args.a10_lastyr_data, index_col = 0)

    # this_year = datetime.datetime.now().year # uncomment after this year's reporting starts
    this_year = 2021
    last_year = this_year - 1

    
    # Run validation checks
    a10_checks = facility_checks(df, this_year, last_year)

    # Write results to an Excel file
    with pd.ExcelWriter("reports/a10_facility_check_report.xlsx") as writer:
        a10_checks.to_excel(writer, sheet_name="a10_checks_full", index=False, startrow=2)

        workbook = writer.book
        worksheet = writer.sheets["a10_checks_full"]
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
        subtitle = "A-10 Facilities: Validation Warnings"
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
        worksheet.set_column(0, 0, 35) #col A width
        worksheet.set_column(1, 2, 22) #cols B-C width
        worksheet.set_column(3, 3, 11) #col D width
        worksheet.set_column(4, 6, 53) #col E-G width
        worksheet.freeze_panes('B4')

        # Add some readme text to the output file for context
        worksheet2 = workbook.add_worksheet("readme")
        worksheet2.write('A1', "Read Me", title_format)
        worksheet2.write('A2', "This file runs 6 validation checks on submitted 2023-A-10 form data, based on historical NTD validation errors.")
        worksheet2.write('A4', "Total Facilities checks", subtitle_format)
        worksheet2.write('A5', '1. "Whole Number Facilities": Check that sum of total facilities for each agency, across all modes, is a whole number.')
        worksheet2.write('A6', '2. "Non-zero Facilities" check: Check that the sum of all total facilities is not zero.')
        worksheet2.write('A8', "General Purpose Facilities checks (all except \"heavy maintenance\")", subtitle_format)
        worksheet2.write('A9', '3. "Gen  Purpose Facilities": Check whether total gen purpose facilities (all but heavy maintenance) is > 1. If so mark as "failure".')
        worksheet2.write('A10', '4. "Multiple Gen Purpose Facilities": if > 1 reported, ask for narrative justification.')
        worksheet2.write('A11', '5. "Comparison to last yr: Gen Purpose Facilities": Fail if the total differs from last year')
        worksheet2.write('A12', '6. "Non-zero Gen Purpose Facilities": fail if this is reported as 0')

        print("Validation of A-10 form is complete!")

if __name__ == "__main__":
    main()

