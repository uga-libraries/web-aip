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


def compare_df(test, df_actual, df_expected):
    """Compares 2 dataframes and either prints that they match or saves a CSV with the differences. """

    # Replaces empty fields with string "BLANK" since this causes type errors when comparing to the
    # expected dataframe which is made by the script.
    df_actual.fillna("BLANK", inplace=True)

    # Makes a new dataframe with a merge of the two and removes the ones that match exactly (_merge is both).
    df = df_actual.merge(df_expected, indicator=True, how="outer")
    df = df[df["_merge"] != "both"]

    # If the merged dataframe is empty (everything matched), prints test success.
    # Otherwise, saves the rows that didn't match to a CSV in the script output directory:
    # left_only is the actual value and right_only is the expected value.
    if len(df) is 0:
        print(f"{test} test passes.")
    else:
        print(f"{test} test had errors. See CSV in script output directory for details.")
        df.to_csv(f"{test}_differences.csv", index=False)


# Make the script output folder the current directory for easy saving of test results.
os.chdir(c.script_output)

# Test: BMA seed 2028986 has 2 WARCs.
# Has minimum metadata in Archive-It but is not a department that uses the script so can't calculate values.
bma_df = web.seed_data("2020-02-18", "2020-02-19")
warc_list = ["ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129224159397-00001-h3.warc.gz,"
             "ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129213514425-00000-h3.warc.gz"]
bma_ex_df = pd.DataFrame({"Seed_ID": ["2028986"],
                          "AIP_ID": ["BLANK"],
                          "Title": ["The Now Explosion Website"],
                          "Department": ["BLANK"],
                          "UGA_Collection": ["BLANK"],
                          "AIT_Collection": [12470],
                          "Job_ID": [1085452],
                          "Size_GB": [1.3],
                          "WARCs": [2],
                          "WARC_Filenames": warc_list,
                          "Seed_Metadata_Errors": ["Couldn't get all required metadata values from the seed report. "
                                                   "Will not download files or make AIP."],
                          "Metadata_Report_Errors": ["BLANK"],
                          "Metadata_Report_Info": ["BLANK"],
                          "WARC_API_Errors": ["BLANK"],
                          "WARC_Fixity_Errors": ["BLANK"],
                          "WARC_Unzip_Errors": ["BLANK"]})
compare_df("BMA", bma_df, bma_ex_df)
