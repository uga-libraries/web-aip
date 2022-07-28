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

# # Test: 4 Hargrett seeds, including 1 and multiple WARCS. None have related collection.
# harg_df = web.seed_data("2020-06-08", "2020-06-09")
# harg_ex_df = pd.DataFrame({"Seed_ID": ["2187482", "2030942", "2084785", "2270486"],
#                            "AIP_ID": ["harg-0000-web-202006-0001", "harg-0000-web-202006-0002", "harg-0000-web-202006-0003", "harg-0000-web-202006-0004"],
#                            "Title": ["Student Government Association Facebook", "UGA NAACP Twitter", "Zeta Pi Chapter of Alpha Phi Alpha Twitter", "UGA Commencement"],
#                            "Department": ["hargrett", "hargrett", "hargrett", "hargrett"],
#                            "UGA_Collection": ["0000", "0000", "0000", "0000"],
#                            "AIT_Collection": [12181, 12181, 12181, 12912],
#                            "Job_ID": [1177700, 1177700, 1177700, 1176433],
#                            "Size_GB": [3.62, 0.05, 0.11, 0.06],
#                            "WARCs": [4, 1, 1, 1],
#                            "WARC_Filenames": ["ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
#                                               "ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
#                                               "ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
#                                               "ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz"],
#                            "Seed_Metadata_Errors": ["Successfully calculated seed metadata", "Successfully calculated seed metadata",
#                                                     "Successfully calculated seed metadata", "Successfully calculated seed metadata"],
#                            "Metadata_Report_Errors": ["BLANK", "BLANK", "BLANK", "BLANK"],
#                            "Metadata_Report_Info": ["BLANK", "BLANK", "BLANK", "BLANK"],
#                            "WARC_API_Errors": ["BLANK", "BLANK", "BLANK", "BLANK"],
#                            "WARC_Fixity_Errors": ["BLANK", "BLANK", "BLANK", "BLANK"],
#                            "WARC_Unzip_Errors": ["BLANK", "BLANK", "BLANK", "BLANK"]})
# compare_df("Hargrett", harg_df, harg_ex_df)
#
# # Test: 2 MAGIL seeds, including 1 and multiple WARCS.
# magil_df = web.seed_data("2022-04-11", "2022-04-13")
# magil_ex_df = pd.DataFrame({"Seed_ID": ["2529647", "2472043"],
#                            "AIP_ID": ["magil-ggp-2529647-2022-04", "magil-ggp-2472043-2022-04"],
#                            "Title": ["Georgia Department of Transportation", "Georgia Department of Audits and Accounts"],
#                            "Department": ["magil", "magil"],
#                            "UGA_Collection": ["0000", "0000"],
#                            "AIT_Collection": [15678, 15678],
#                            "Job_ID": [1583117, 1583270],
#                            "Size_GB": [12.87, .12],
#                            "WARCs": [13, 1],
#                            "WARC_Filenames": ["ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220331151300136-00000-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220331223248449-00001-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401021708141-00002-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401024320779-00003-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401042238966-00004-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401055943204-00005-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401141604806-00006-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402030302049-00007-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402081423469-00008-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402113301104-00009-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402141557823-00010-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402201602620-00011-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402230559530-00012-urcpzn95.warc.gz",
#                                               "ARCHIVEIT-15678-TEST-JOB1583270-0-SEED2472043-20220331170557241-00000-xzgq9wm8.warc.gz"],
#                            "Seed_Metadata_Errors": ["Successfully calculated seed metadata", "Successfully calculated seed metadata"],
#                            "Metadata_Report_Errors": ["BLANK", "BLANK"],
#                            "Metadata_Report_Info": ["BLANK", "BLANK"],
#                            "WARC_API_Errors": ["BLANK", "BLANK"],
#                            "WARC_Fixity_Errors": ["BLANK", "BLANK"],
#                            "WARC_Unzip_Errors": ["BLANK", "BLANK"]})
# compare_df("MAGIL", magil_df, magil_ex_df)

# # Test: 6 Russell seeds, including 1 and multiple WARCS.
# # All have related collection, some collections have multiple seeds.
# rbrl_df = web.seed_data("2021-02-11", "2021-02-12")
# rbrl_ex_df = pd.DataFrame({"Seed_ID": ["2467336", "2454513", "2454516", "2444048", "2454520", "2467334"],
#                            "AIP_ID": ["rbrl-496-web-202102-0001", "rbrl-495-web-202102-0001", "rbrl-497-web-202102-0001", "rbrl-494-web-202102-0001", "rbrl-497-web-202102-0002", "rbrl-496-web-202102-0002"],
#                            "Title": ["Senator Kelly Loeffler (@senloeffler) - Instagram photos and videos", "Doug Collins (@RepDougCollins) / Twitter", "Rob Woodall (@RepRobWoodall) / Twitter", "Bob Trammell (@TrammellBob) / Twitter", "Rep. Rob Woodall (@reprobwoodall) - Instagram photos and videos", "Senator Kelly Loeffler - YouTube"],
#                            "Department": ["russell", "russell", "russell", "russell", "russell", "russell"],
#                            "UGA_Collection": ["rbrl-496", "rbrl-495", "rbrl-497", "rbrl-494", "rbrl-497", "rbrl-496"],
#                            "AIT_Collection": [12265, 12265, 12265, 12265, 12265, 12265],
#                            "Job_ID": [1343968, 1331748, 1331754, 1331741, 1362694, 1343961],
#                            "Size_GB": [0, 0.38, 0.14, 0.07, 0, 1.46],
#                            "WARCs": [1, 1, 1, 2, 1, 3],
#                            "WARC_Filenames": ["ARCHIVEIT-12265-TEST-JOB1343968-0-SEED2467336-20210107234951732-00000-ntej8lcr.warc.gz",
#                                               "ARCHIVEIT-12265-TEST-JOB1331748-0-SEED2454513-20201215211605582-00000-hr7k2if1.warc.gz",
#                                               "ARCHIVEIT-12265-TEST-JOB1331754-0-SEED2454516-20201215212108745-00000-wtezbqjr.warc.gz",
#                                               "ARCHIVEIT-12265-TEST-JOB1331741-0-SEED2444048-20201215210645991-00000-cwumey2o.warc.gz,ARCHIVEIT-12265-TEST-JOB1331741-0-SEED2444048-20201215212055722-00000-2ec78ok3.warc.gz",
#                                               "ARCHIVEIT-12265-TEST-JOB1362694-0-SEED2454520-20210210182243522-00000-ucgqbp1w.warc.gz",
#                                               "ARCHIVEIT-12265-TEST-JOB1343961-0-SEED2467334-20210107234944827-00000-pacx1ueb.warc.gz,ARCHIVEIT-12265-TEST-JOB1343961-0-SEED2467334-20210107233308683-00000-4fqdlznu.warc.gz,ARCHIVEIT-12265-TEST-JOB1343961-0-SEED2467334-20210107234913594-00001-4fqdlznu.warc.gz"],
#                            "Seed_Metadata_Errors": ["Successfully calculated seed metadata", "Successfully calculated seed metadata",
#                                                     "Successfully calculated seed metadata", "Successfully calculated seed metadata",
#                                                     "Successfully calculated seed metadata", "Successfully calculated seed metadata"],
#                            "Metadata_Report_Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
#                            "Metadata_Report_Info": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
#                            "WARC_API_Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
#                            "WARC_Fixity_Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
#                            "WARC_Unzip_Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]})
# compare_df("Russell", rbrl_df, rbrl_ex_df)

# # Test: BMA seed 2028986 has 2 WARCs.
# # Has minimum metadata in Archive-It but is not a department that uses the script so can't calculate values.
# bma_df = web.seed_data("2020-02-18", "2020-02-19")
# bma_ex_df = pd.DataFrame({"Seed_ID": ["2028986"],
#                           "AIP_ID": ["BLANK"],
#                           "Title": ["The Now Explosion Website"],
#                           "Department": ["BLANK"],
#                           "UGA_Collection": ["BLANK"],
#                           "AIT_Collection": [12470],
#                           "Job_ID": [1085452],
#                           "Size_GB": [1.3],
#                           "WARCs": [2],
#                           "WARC_Filenames": ["ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129224159397-00001-h3.warc.gz,"
#                                              "ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129213514425-00000-h3.warc.gz"],
#                           "Seed_Metadata_Errors": ["Couldn't get all required metadata values from the seed report. "
#                                                    "Will not download files or make AIP."],
#                           "Metadata_Report_Errors": ["BLANK"],
#                           "Metadata_Report_Info": ["BLANK"],
#                           "WARC_API_Errors": ["BLANK"],
#                           "WARC_Fixity_Errors": ["BLANK"],
#                           "WARC_Unzip_Errors": ["BLANK"]})
# compare_df("BMA", bma_df, bma_ex_df)

# Test:
mix_df = web.seed_data("2019-06-26", "2019-07-04")
mix_ex_df = pd.DataFrame({"Seed_ID": ["2024640", "2024246", "2024250", "2024247", "2024249", "2024248"],
                          "AIP_ID": ["rbrl-057-web-201907-0001", "harg-0000-web-201907-0001", "BLANK", "harg-0000-web-201907-0002", "BLANK", "BLANK"],
                          "Title": ["Democratic Party of Georgia - Help Move Georgia Forward", "UGA Black Theatrical Ensemble Twittter", "BLANK", "Infusion Magazine website", "BLANK", "BLANK"],
                          "Department": ["russell", "hargrett", "BLANK", "hargrett", "BLANK", "BLANK"],
                          "UGA_Collection": ["rbrl-057", "0000", "BLANK", "0000", "BLANK", "BLANK"],
                          "AIT_Collection": [12265, 12181, 12181, 12181, 12181, 12181],
                          "Job_ID": [940298, 938127, 938127, 938127, 938127, 938127],
                          "Size_GB": [0.67, 1.94, 0.85, 0.91, 0.24, 1.17],
                          "WARCs": [1, 2, 1, 1, 1, 2],
                          "WARC_Filenames": ["ARCHIVEIT-12265-TEST-JOB940298-SEED2024640-20190701154831318-00000-h3.warc.gz",
                                             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024246-20190626135802459-00001-h3.warc.gz,ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024246-20190625222324356-00000-h3.warc.gz",
                                             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024250-20190625222414653-00000-h3.warc.gz",
                                             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024247-20190625222324141-00000-h3.warc.gz",
                                             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024249-20190625222349710-00000-h3.warc.gz",
                                             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024248-20190626131257414-00001-h3.warc.gz,ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024248-20190625222324244-00000-h3.warc.gz"],
                          "Seed_Metadata_Errors": ["Successfully calculated seed metadata",
                                                   "Successfully calculated seed metadata",
                                                   "Couldn't get all required metadata values from the seed report. Will not download files or make AIP.",
                                                   "Successfully calculated seed metadata",
                                                   "Couldn't get all required metadata values from the seed report. Will not download files or make AIP.",
                                                   "Couldn't get all required metadata values from the seed report. Will not download files or make AIP."],
                          "Metadata_Report_Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                          "Metadata_Report_Info": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                          "WARC_API_Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                          "WARC_Fixity_Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                          "WARC_Unzip_Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]})
compare_df("Mix", mix_df, mix_ex_df)
