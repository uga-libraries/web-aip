"""Purpose: This script tests the functionality of the seed_data function in web_functions.py, including
error handling and data calculations. It calls the seed_data function multiple times to create different input
conditions while still getting a simpler amount of data to analyze (rather than doing the entire date range
encompassed by the tests).

This is a separate test from the rest of the workflow for making AIPs to make it easier to create the variations.

Usage: python path/test_seed_data.py
"""
import os
import pandas as pd
import configuration as c
import web_functions as web

# Make the script output folder the current directory for easy saving of test results.
os.chdir(c.script_output)

# Test: 1 BMA seed with 2 WARCs. Not a department expected by the script.
seed_df = web.seed_data("2020-02-18", "2020-02-19")
print(seed_df.dtypes)
# TODO: 
# ex_df = pd.DataFrame({"Seed_ID": ["2028986"],
#                       "AIP_ID": [""],
#                       "Title": ["The Now Explosion Website"],
#                       "Department": [""],
#                       "UGA_Collection": [""],
#                       "AIT_Collection": ["12470"],
#                       "Job_ID": ["1085452"],
#                       "Size_GB": ["1.3"],
#                       "WARCs": ["2"],
#                       "WARC_Filenames": ["ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129224159397-00001-h3.warc.gz,ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129213514425-00000-h3.warc.gz"],
#                       "Seed_Metadata_Errors": [""],
#                       "Metadata_Report_Errors": ["Couldn't get all required metadata values from the seed report. Will not download files or make AIP."],
#                       "Metadata_Report_Info": [""],
#                       "WARC_API_Errors": [""],
#                       "WARC_Fixity_Errors": [""],
#                       "WARC_Unzip_Errors": [""]})
# compare_df = seed_df.merge(ex_df, indicator=True, how="outer")
# compare_df.to_csv("compare.csv", index=False)