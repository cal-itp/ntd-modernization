from argparse import ArgumentParser
import pandas as pd

'''This file loads 3 datasets (A-30, RR-20 Service data, Revenue Vehicle Inventory) that originate from Black Cat.
To run from command line, navigate to folder: 
* To run with the default datasources, type: python voms_inventory_check.py
* To specify datasources type: python voms_inventory_check.py 
                                --rev_vehicle_inventory_data <filepath> 
                                --a30_data <filepath> 
                                --rr20_service_data <filepath>
            
Performs 3 checks:
* Check 1. Compare each A-30 vehicle's VIN to inventory ensure that all vehicles in the lists are the same.
* Check 2. Ensure  the total "active" vehicles in the inventory are equal to or less than what is reported on the A-30.
* Check 3. Ensure that the total VOMS vehicles reported on the RR-20 is equal to or fewer than the total vehicles on the A-30.

For more details on forms, SEE THIS NOTEBOOK: "1_VOMS_inventory_checks.ipynb"
'''

def get_arguments():
    """Get the data as input arguments (for now)"""
    parser = ArgumentParser(description="VOMS inventory check")
    parser.add_argument('--rev_vehicle_inventory_data', default="data/RevenueVehicles_9_2_2023.xlsx")
    parser.add_argument('--a30_data', default="data/A_30_Revenue_Vehicle_Report_9_1_2023.xlsx")
    parser.add_argument('--rr20_service_data', default="data/NTD_Annual_Report_Rural_2022.xlsx")

    args = parser.parse_args()
    return args


def load_excel_data(filename, sheetname):
    df = pd.read_excel(filename, sheet_name=sheetname,
                            index_col=None)
    return df


def vins_all_checks(a30_data, a30_agencies, inventory_data):
    """ Compare A-30 VIN list with inventory VIN list (active vehicles).
        Returns full list of all A-30 VINS and whether they match inventory"""
    
    output = []
    for agency in a30_agencies:
        if len(a30_data[a30_data['Organization']==agency]) > 0:
            vins_a30 = a30_data[a30_data['Organization']==agency]['VIN'].unique()
            vins_inventory = inventory_data[(inventory_data['Organization']==agency) \
                    & (inventory_data['Status']=='Active')]['VIN'].unique()
            
            for v in vins_a30: #check whether each VIN exists in inventory
                if v in vins_inventory:
                    description = "Matched an Active vehicle in vehicle inventory."
                    result = "Y"
                else:
                    description = f"{v} not an active vehicle in the inventory. Investigate."
                    result = "N"

                output_line = {"Organization": agency,
                            "VIN": v,
                            "check_status": result,
                            "Description": description}
                output.append(output_line)
        
    full_vin_checklist = pd.DataFrame(output).sort_values(by="Organization")
    return full_vin_checklist


def partial_vin_checklist(a30_data, a30_agencies, inventory_data):
    """ Compare A-30 VIN list with inventory VIN list (active vehicles).
        Returns ONLY those that do not match."""
    
    output = []
    for agency in a30_agencies:
        if len(a30_data[a30_data['Organization']==agency]) > 0:
            vins_a30 = a30_data[a30_data['Organization']==agency]['VIN'].unique() #will list only active ins
            vins_inventory = inventory_data[(inventory_data['Organization']==agency) \
                    & (inventory_data['Status']=='Active')]['VIN'].unique()
            
            for v in vins_a30: #check whether each VIN exists in inventory
                if v not in vins_inventory:
                    description = f"Not an active vehicle in this org's inventory. Investigate."
                    result = "N"
                    
                    output_line = {"Organization": agency,
                            "VIN": v,
                            "check_status": result,
                            "Description": description}
                    output.append(output_line)
                else:
                    pass
    
    mismatched_vin_checklist = pd.DataFrame(output).sort_values(by="Organization")
    return mismatched_vin_checklist


def check_totals(a30_data, a30_agencies, inventory_data, rr20_data):
    """COmpare total reported vehicles across RR-20, A-30, inventory list"""

    output = []
    for agency in a30_agencies:
        if len(a30_data[a30_data['Organization']==agency]) > 0:
            a30_n = a30_data[a30_data['Organization']==agency]['VIN'].nunique()
            
        if len(inventory_data[inventory_data['Organization'] == agency]) > 0:
            inv_n = inventory_data[(inventory_data['Organization'] == agency) \
            & (inventory_data['Status']=='Active')]['VIN'].nunique()
        
        if len(rr20_data[rr20_data['Organization Legal Name']==agency]) > 0:
            rr20_n = rr20_data[rr20_data['Organization Legal Name']==agency]['VOMX'].sum()
            rr20_n = round(rr20_n)

        if (a30_n <= inv_n) & (rr20_n <= inv_n) & (a30_n >= rr20_n):
            result = "pass"
            description = "VOMS & A-30 vehicles reported are equal to and/or lower than active inventory."
        elif (a30_n > inv_n):
            result = "warning"
            description = "More A-30 vehicles reported than in active inventory."
        elif (a30_n < rr20_n):
            result = "fail"
            description = "Total VOMS is greater than total A-30 vehicles reported. Please clarify"

        output_line = {"Organization": agency,
                    "n_a30_vehicles": a30_n,
                    "n_rr20_VOMS": rr20_n,
                    "n_inventory_vehicles": inv_n,
                    "check_result": result,
                    "Description": description}
        output.append(output_line)
    totals_checklist = pd.DataFrame(output)
    
    return totals_checklist


def main():
    #Load data:
    args = get_arguments()
    rev_vehicle_inventory = load_excel_data(args.rev_vehicle_inventory_data, "Revenue Vehicles")
    a30 = load_excel_data(args.a30_data, "A-30 (Rural) RVI")
    rr20 = load_excel_data(args.rr20_service_data, "Service Data")

    #List of agencies with A-30 data
    a30_agencies = a30['Organization'].unique()

    # Generate the 3 typesof VOMS checks:
    full_vin_checklist = vins_all_checks(a30, a30_agencies, rev_vehicle_inventory)
    mismatched_vin_checklist = partial_vin_checklist(a30, a30_agencies, rev_vehicle_inventory)
    totals_checklist = check_totals(a30, a30_agencies, rev_vehicle_inventory, rr20)

    # Write them all to one Excel file, in different sheets:
    ## We also add a few more columns for Liaisions to manually track agency responses.
    with pd.ExcelWriter("reports/voms_check_report.xlsx") as writer:
   
        full_vin_checklist.to_excel(writer, sheet_name="vin_check_full", index=False, startrow=2)
        mismatched_vin_checklist.to_excel(writer, sheet_name="vin_check_fails_only", index=False, startrow=2)
        totals_checklist.to_excel(writer, sheet_name="totals_check", index=False, startrow=2)
        
        workbook = writer.book
        worksheet1 = writer.sheets["vin_check_full"]
        worksheet2 = writer.sheets["vin_check_fails_only"]
        worksheet3 = writer.sheets["totals_check"]
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
        subtitle1 = "VOMS Inventory Vehicle Check: Validation Warnings"
        subtitle2 = "VOMS RR-20 & A-30 check"
        subtitle_format = workbook.add_format({
            'bold': True,
            'align': 'left',
            'font_color': 'black',
            'font_size': 19
            })

        # Column width and format - sheet 1
        worksheet1.set_column(0, 0, 35) #col A width
        worksheet1.set_column(1, 2, 20) #cols B-C width
        worksheet1.set_column(3, 3, 53) #col D width
        worksheet1.write('A1', report_title, title_format)
        worksheet1.merge_range('A2:D2', subtitle1, subtitle_format)
        worksheet1.freeze_panes('B4')
        
        # Column width and format - sheet 2
        worksheet2.set_column(0, 0, 35) #col A width
        worksheet2.set_column(1, 2, 20) #cols B-C width
        worksheet2.set_column(3, 5, 53) #cols D-F width
        worksheet2.write('E3', 'Agency Response', cell_highlight)
        worksheet2.write('F3', 'Response Date', cell_highlight)
        worksheet2.write('A1', report_title, title_format)
        worksheet2.merge_range('A2:D2', subtitle1, subtitle_format)
        worksheet2.freeze_panes('B4')

        
        # Column width and format - sheet 3
        worksheet3.set_column(0, 0, 35) #col A width
        worksheet3.set_column(1, 4, 18) #cols B-E width
        worksheet3.set_column(5, 7, 53) #cols F-H width
        worksheet3.write('G3', 'Agency Response', cell_highlight)
        worksheet3.write('H3', 'Response Date', cell_highlight)
        worksheet3.write('A1', report_title, title_format)
        worksheet3.merge_range('A2:B2', subtitle2, subtitle_format)
        worksheet3.freeze_panes('B4')
    
    print("VOMS check is complete!")

if __name__ == "__main__":
    main()






