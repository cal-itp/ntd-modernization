# ntd-modernization
  
This repo contains multiple analyses related to helping make Caltrans NTD reporting more efficient. 
  
### Issues Analysis, 2020 - 2022
The `ntd_issues_analysis.Rmd` R Markdown file analyzes errors Caltrans received from NTD from 2020 - 22. The accompanying HTML file shows code and web-interactive graphics (if it does not render, download onto your computer and reopen).  
  
---
### Pre-submission Validator
The folder `validation_tool` contains code for prototyping a presubmission validation tool to anticipate issues in submitting NTD reports, based on 2020-22 issues. *The location of the final code still TBD.* 

*  `*.py` files: To run validations, run these files - instructions to run each, and which forms they validate, are in comments at top of the file. 
* `reports` folder: Excel files that the `*.py` files produce. These are meant for business users, to have a record of which sub-recipients passed/failed different validation checks in their submitted data. Business users will follow up with subrecipients. 
* `data` folder: Input data. For now we must prototype with spreadsheets as inputs; eventually there will be an API in place that thie pipeline will switch to for source data
* `notebooks` folder: Jupyter notebooks that show development of functions that are in the `*.py` files 
* `schemas` folder: These contain validation schemas upon which to check the incoming data against. They are made in conjunction with the Pandera validation library. At the moment, we are experimenting with using them, *but are not currently using any in the completed validation checks that are written so far. They remain here for purposes of showing what we explored with them so far and why we decided not to use it yet (details in specific notebooks).*

