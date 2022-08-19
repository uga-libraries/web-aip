"""Purpose: This script tests the functionality of the seed_data function in web_functions.py, including
error handling and data calculations. It calls the seed_data function multiple times to create different input
conditions while still getting a simpler amount of data to analyze (rather than doing the entire date range
encompassed by the tests).

This is a separate test from the rest of the workflow for making AIPs to make it easier to create the variations and
because it does not rely on any other part of the script.
"""

# Usage: python path/test_seed_data_function.py

import os
import numpy as np
import pandas as pd
import configuration as c
import web_functions as web


def make_expected_df(test):
    """Makes and returns a dataframe with the expected values for the specified test scenario. Starts by making
    lists for each column with the values for that test scenario, and then combines those into a dataframe."""

    if test == "bma":
        rows = [["2028986", np.NaN, "The Now Explosion Website", np.NaN, np.NaN, 12470, "1085452", 1.3, 2,
                "ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129224159397-00001-h3.warc.gz,ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129213514425-00000-h3.warc.gz",
                "Couldn't get all required metadata values from the seed report. Will not download files or make AIP.",
                np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]

    elif test == "hargrett":
        rows = [
            ["2030942", "harg-0000-web-202006-0002", "UGA NAACP Twitter",
             "hargrett", "0000", 12181, "1177700", 0.05, 1,
             "ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2084785", "harg-0000-web-202006-0003", "Zeta Pi Chapter of Alpha Phi Alpha Twitter",
             "hargrett", "0000", 12181, "1177700", 0.11, 1,
             "ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2187482", "harg-0000-web-202006-0001", "Student Government Association Facebook",
             "hargrett", "0000", 12181, "1177700", 3.62, 4,
             "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
            "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2270486", "harg-0000-web-202006-0004", "UGA Commencement",
             "hargrett", "0000", 12912, "1176433", 0.06, 1,
             "ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]

    elif test == "magil":
        rows = [
        ["2472043", "magil-ggp-2472043-2022-04", "Georgia Department of Audits and Accounts",
            "magil", "0000", 15678, "1583270", .12, 1,
            "ARCHIVEIT-15678-TEST-JOB1583270-0-SEED2472043-20220331170557241-00000-xzgq9wm8.warc.gz",
            "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2529647", "magil-ggp-2529647-2022-04", "Georgia Department of Transportation",
             "magil", "0000", 15678, "1583117", 12.87, 13,
             "ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220331151300136-00000-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220331223248449-00001-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401021708141-00002-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401024320779-00003-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401042238966-00004-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401055943204-00005-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220401141604806-00006-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402030302049-00007-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402081423469-00008-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402113301104-00009-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402141557823-00010-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402201602620-00011-urcpzn95.warc.gz,ARCHIVEIT-15678-TEST-JOB1583117-0-SEED2529647-20220402230559530-00012-urcpzn95.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]

    elif test == "mix":
        rows = [
            ["2024246", "harg-0000-web-201907-0001", "UGA Black Theatrical Ensemble Twittter",
             "hargrett", "0000", 12181, "938127", 1.94, 2,
             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024246-20190626135802459-00001-h3.warc.gz,ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024246-20190625222324356-00000-h3.warc.gz", "Successfully calculated seed metadata",
             np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2024247", "harg-0000-web-201907-0002", "Infusion Magazine website",
             "hargrett", "0000", 12181, "938127", 0.91, 1,
             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024247-20190625222324141-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2024248", np.NaN, np.NaN, np.NaN, np.NaN, 12181, "938127", 1.17, 2,
             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024248-20190626131257414-00001-h3.warc.gz,ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024248-20190625222324244-00000-h3.warc.gz",
             "Couldn't get all required metadata values from the seed report. Will not download files or make AIP.",
             np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2024249", np.NaN, np.NaN, np.NaN, np.NaN, 12181, "938127", 0.24, 1,
             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024249-20190625222349710-00000-h3.warc.gz",
             "Couldn't get all required metadata values from the seed report. Will not download files or make AIP.",
             np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2024250", np.NaN, np.NaN, np.NaN, np.NaN, 12181, "938127", 0.85, 1,
             "ARCHIVEIT-12181-CRAWL_SELECTED_SEEDS-JOB938127-SEED2024250-20190625222414653-00000-h3.warc.gz",
             "Couldn't get all required metadata values from the seed report. Will not download files or make AIP.",
             np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2024640", "rbrl-057-web-201907-0001", "Democratic Party of Georgia - Help Move Georgia Forward",
            "russell", "rbrl-057", 12265, "940298", 0.67, 1,
            "ARCHIVEIT-12265-TEST-JOB940298-SEED2024640-20190701154831318-00000-h3.warc.gz",
            "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]

    elif test == "multiple":
        rows = [
            ["2184360", "harg-0000-web-202102-0001", "UGA Today",
            "hargrett", "0000", 12912, "1365262", 2.72, 3,
            "ARCHIVEIT-12912-MONTHLY-JOB1365262-SEED2184360-20210218011718988-00002-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1365262-SEED2184360-20210216082333741-00001-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1365262-SEED2184360-20210215221717805-00000-h3.warc.gz",
            "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2454528", "rbrl-495-web-202102-0001", "Doug Collins for Senate - Home | Facebook",
             "russell", "rbrl-495", 12265, "1365541", 2.69, 3,
             "ARCHIVEIT-12265-TEST-JOB1365541-SEED2454528-20210216160709989-00000-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1365541-SEED2454528-20210216175007597-00001-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1365541-SEED2454528-20210217005857702-00002-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2467332", "rbrl-496-web-202102-0001", "Sen. Kelly Loeffler - Home | Facebook",
             "russell", "rbrl-496", 12265, "1365539,1360420", 1.38, 3,
             "ARCHIVEIT-12265-TEST-JOB1365539-SEED2467332-20210216160534209-00000-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1365539-SEED2467332-20210217050217105-00001-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1360420-0-SEED2467332-20210205184307156-00000-htl6r9yj.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2481631", "rbrl-496-web-202102-0005", "Sen. Kelly Loeffler - About | Facebook",
             "russell", "rbrl-496", 12265, "1365539,1360420", 0.8, 2,
             "ARCHIVEIT-12265-TEST-JOB1365539-SEED2481631-20210216162203968-00000-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1360420-0-SEED2481631-20210205184307392-00000-7gwb8haf.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2481632", "rbrl-496-web-202102-0003", "Sen. Kelly Loeffler - Videos | Facebook",
             "russell", "rbrl-496", 12265, "1365539,1360420", 0.86, 2,
             "ARCHIVEIT-12265-TEST-JOB1365539-SEED2481632-20210216162414083-00000-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1360420-0-SEED2481632-20210205184307213-00000-5ejos6fh.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2481633", "rbrl-496-web-202102-0002", "Sen. Kelly Loeffler - Posts | Facebook",
             "russell", "rbrl-496", 12265, "1365539,1360420", 1.1, 3,
             "ARCHIVEIT-12265-TEST-JOB1365539-SEED2481633-20210216163158712-00000-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1365539-SEED2481633-20210219104712285-00001-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1360420-0-SEED2481633-20210205184316027-00000-8r6d2i9m.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2481634", "rbrl-496-web-202102-0004", "Sen. Kelly Loeffler - Photos | Facebook",
             "russell", "rbrl-496", 12265, "1365539,1360420", 1.47, 2,
             "ARCHIVEIT-12265-TEST-JOB1365539-SEED2481634-20210216165211862-00000-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1360420-0-SEED2481634-20210205184307279-00000-j3vx7i5n.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2483019", "rbrl-495-web-202102-0005", "Doug Collins - About | Facebook",
             "russell", "rbrl-495", 12265, "1365541", 0.78, 1,
             "ARCHIVEIT-12265-TEST-JOB1365541-SEED2483019-20210216161617342-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2483020", "rbrl-495-web-202102-0002", "Doug Collins - Posts | Facebook",
             "russell", "rbrl-495", 12265, "1365541", 2.22, 3,
             "ARCHIVEIT-12265-TEST-JOB1365541-SEED2483020-20210216161756658-00000-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1365541-SEED2483020-20210217025521093-00001-h3.warc.gz,ARCHIVEIT-12265-TEST-JOB1365541-SEED2483020-20210218125429980-00002-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2483021", "rbrl-495-web-202102-0007", "Doug Collins - Videos | Facebook",
             "russell", "rbrl-495", 12265, "1365541", 0.79, 1,
             "ARCHIVEIT-12265-TEST-JOB1365541-SEED2483021-20210216162201596-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2483022", "rbrl-495-web-202102-0004", "Doug Collins - Events | Facebook",
             "russell", "rbrl-495", 12265, "1365541", 0.77, 1,
             "ARCHIVEIT-12265-TEST-JOB1365541-SEED2483022-20210216162544981-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2483023", "rbrl-495-web-202102-0003", "Doug Collins - Photos | Facebook",
             "russell", "rbrl-495", 12265, "1365541", 0.79, 1,
             "ARCHIVEIT-12265-TEST-JOB1365541-SEED2483023-20210216163040755-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2483024", "rbrl-495-web-202102-0006", "Doug Collins - Community | Facebook",
             "russell", "rbrl-495", 12265, "1365541", 0.77, 1,
             "ARCHIVEIT-12265-TEST-JOB1365541-SEED2483024-20210216163135976-00000-h3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]

    elif test == "russell":
        rows = [
            ["2444048", "rbrl-494-web-202102-0001", "Bob Trammell (@TrammellBob) / Twitter",
             "russell", "rbrl-494", 12265, "1331741", 0.07, 2,
             "ARCHIVEIT-12265-TEST-JOB1331741-0-SEED2444048-20201215210645991-00000-cwumey2o.warc.gz,ARCHIVEIT-12265-TEST-JOB1331741-0-SEED2444048-20201215212055722-00000-2ec78ok3.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2454513", "rbrl-495-web-202102-0001", "Doug Collins (@RepDougCollins) / Twitter",
             "russell", "rbrl-495", 12265, "1331748", 0.38, 1,
             "ARCHIVEIT-12265-TEST-JOB1331748-0-SEED2454513-20201215211605582-00000-hr7k2if1.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2454516", "rbrl-497-web-202102-0001", "Rob Woodall (@RepRobWoodall) / Twitter",
             "russell", "rbrl-497", 12265, "1331754", 0.14, 1,
             "ARCHIVEIT-12265-TEST-JOB1331754-0-SEED2454516-20201215212108745-00000-wtezbqjr.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2454520", "rbrl-497-web-202102-0002", "Rep. Rob Woodall (@reprobwoodall) - Instagram photos and videos",
             "russell", "rbrl-497", 12265, "1362694", 0, 1,
             "ARCHIVEIT-12265-TEST-JOB1362694-0-SEED2454520-20210210182243522-00000-ucgqbp1w.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2467334", "rbrl-496-web-202102-0002", "Senator Kelly Loeffler - YouTube",
             "russell", "rbrl-496", 12265, "1343961", 1.46, 3,
             "ARCHIVEIT-12265-TEST-JOB1343961-0-SEED2467334-20210107234944827-00000-pacx1ueb.warc.gz,ARCHIVEIT-12265-TEST-JOB1343961-0-SEED2467334-20210107233308683-00000-4fqdlznu.warc.gz,ARCHIVEIT-12265-TEST-JOB1343961-0-SEED2467334-20210107234913594-00001-4fqdlznu.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2467336", "rbrl-496-web-202102-0001", "Senator Kelly Loeffler (@senloeffler) - Instagram photos and videos",
             "russell", "rbrl-496", 12265, "1343968", 0, 1,
             "ARCHIVEIT-12265-TEST-JOB1343968-0-SEED2467336-20210107234951732-00000-ntej8lcr.warc.gz",
             "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]

    column_names = ["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                    "Size_GB", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                    "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"]
    df = pd.DataFrame(rows, columns=column_names)
    return df


def compare_df(test, df_actual, df_expected):
    """Compares 2 dataframes and either prints that they match or saves a CSV with the differences. """

    # Makes a new dataframe with a merge of the two and removes the ones that match exactly (_merge is both).
    df = df_actual.merge(df_expected, indicator=True, how="outer")
    df = df[df["_merge"] != "both"]

    # If the merged dataframe is empty (everything matched), prints test success.
    # Otherwise, saves the rows that didn't match to a CSV in the script output directory:
    # left_only is the actual value and right_only is the expected value.
    if len(df) == 0:
        print(f"Test passes: {test}")
    else:
        print(f"Test fails: {test}. See CSV in script output directory for details.")
        df.to_csv(f"{test}_differences.csv", index=False)


# Changes the current directory to the script output folder for short paths for saving test results.
os.chdir(c.script_output)

# Test: BMA seed 2028986 has 2 WARCs.
# Has minimum metadata in Archive-It but is not a department that uses the script so can't calculate additional values.
bma_df = web.seed_data("2020-02-18", "2020-02-19")
bma_expected_df = make_expected_df("bma")
compare_df("BMA", bma_df, bma_expected_df)

# Test: 4 Hargrett seeds, including 1 WARC and multiple WARCS. None have related collections.
hargrett_df = web.seed_data("2020-06-08", "2020-06-09")
hargrett_expected_df = make_expected_df("hargrett")
compare_df("Hargrett", hargrett_df, hargrett_expected_df)

# Test: 2 MAGIL seeds, including 1 WARC and multiple WARCS. None have related collections.
magil_df = web.seed_data("2022-04-11", "2022-04-13")
magil_expected_df = make_expected_df("magil")
compare_df("MAGIL", magil_df, magil_expected_df)

# Test: Combination of Hargrett, Russell, and deleted seeds with no metadata.
mix_df = web.seed_data("2019-06-26", "2019-07-04")
mix_expected_df = make_expected_df("mix")
compare_df("Mix", mix_df, mix_expected_df)

# Test: combination of Hargrett and Russell with some that have multiple crawl jobs.
multiple_df = web.seed_data("2021-02-16", "2021-02-20")
multiple_expected_df = make_expected_df("multiple")
compare_df("Multiple Jobs", multiple_df, multiple_expected_df)

# Test: 6 Russell seeds, including 1 WARC and multiple WARCS.
# All have related collection; some collections have multiple seeds.
russell_df = web.seed_data("2021-02-11", "2021-02-12")
russell_expected_df = make_expected_df("russell")
compare_df("Russell", russell_df, russell_expected_df)
