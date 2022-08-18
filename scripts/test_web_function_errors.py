"""Purpose: This script generates every known error to use for testing the error handling of web_aip_batch.py.

Usage: python /path/test_web_function_errors.py
Date values need to be 2022-03-20 and 2022-03-25 to test for predictable results,
so included in this script and not as arguments.

"""

import datetime
import numpy as np
import os
import pandas as pd
import re
import requests
import subprocess
import sys
import time

# Import functions and constant variables from other UGA scripts.
import aip_functions as a
import configuration as c
import web_functions as web


# ----------------------------------------------------------------------------------------------------------------
# Functions to make dataframes of expected values for all script outputs.
# ----------------------------------------------------------------------------------------------------------------
def make_expected_seed_df():
    """Makes and returns a dataframe with the expected values for seed_df.
    Starts by making a list of the values for each row in the dataframe."""

    rows = [

        ["2173769", "harg-0000-web-202203-0012", "Coronavirus (COVID-19) Information and Resources website",
         "hargrett", "0000", 12912, "1496119,1493118,1490068", 0.85, 3,
         "ARCHIVEIT-12912-WEEKLY-JOB1496119-SEED2173769-20211013221706082-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1493118-SEED2173769-20211006221709530-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1490068-SEED2173769-20210929221708074-00000-h3.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "Empty report harg-0000-web-202203-0012_seedscope.csv not saved",
         "API error 999: can't download ARCHIVEIT-12912-WEEKLY-JOB1496119-SEED2173769-20211013221706082-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1493118-SEED2173769-20211006221709530-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1490068-SEED2173769-20210929221708074-00000-h3.warc.gz",
         "Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1493118-SEED2173769-20211006221709530-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1490068-SEED2173769-20210929221708074-00000-h3.warc.gz fixity on",
         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1493118-SEED2173769-20211006221709530-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1490068-SEED2173769-20210929221708074-00000-h3.warc.gz"],

        ["2184360", "harg-0000-web-202203-0011", "UGA Today",
         "hargrett", "0000", 12912, "1497166", 35.78, 36,
         "ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017105304659-00034-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211018211851376-00035-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103754760-00033-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102717487-00031-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103234346-00032-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101611745-00029-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102203466-00030-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101049993-00028-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017100311992-00027-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094753960-00025-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017095516699-00026-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094020636-00024-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017093516000-00023-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017092934974-00022-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091918474-00021-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091205263-00020-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090702655-00019-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090255676-00018-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085824384-00017-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085024644-00016-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017081603923-00015-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017074632435-00014-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073724465-00013-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073125245-00012-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017071639642-00011-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070834820-00010-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070215160-00009-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017065508860-00008-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017064416641-00007-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063723971-00006-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063009068-00005-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017061815778-00004-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017060858471-00003-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017020639822-00002-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211016171927511-00001-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211015221823419-00000-h3.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "No additional information", np.NaN, np.NaN, np.NaN],

        ["2454506", "rbrl-246-web-202203-0002", "SenatorIsakson - YouTube [channel page]",
         "russell", "rbrl-246", 12265, "1542317", 8.05, 9,
         "ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220114214053021-00000-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220115132006340-00001-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220115233148240-00002-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116063911435-00003-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116131550492-00004-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117010033395-00005-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117200343134-00008-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117064534470-00006-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117130422336-00007-8dgxemts.warc.gz",
         "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["2454507", "rbrl-246-web-202203-0001", "SenatorIsakson - YouTube",
         "russell", "rbrl-246", 12265, "1542317", 0, 1,
         "ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "No additional information",
         "Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz",
         "Successfully verified ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz fixity on",
         "Error unzipping ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz: Error message stand-in."],

        ["2529627", "magil-ggp-2529627-2022-03", "Georgia Aviation Authority",
         "magil", "0000", 15678, "1573937", 0.06, 1,
         "ARCHIVEIT-15678-TEST-JOB1573937-SEED2529627-20220314173635069-00000-h3.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "Empty report magil-ggp-2529627-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529627-2022-03_collscope.csv not saved",
         "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1573937-SEED2529627-20220314173635069-00000-h3.warc.gz",
         "Fixity for ARCHIVEIT-15678-TEST-JOB1573937-SEED2529627-20220314173635069-00000-h3.warc.gz changed and it was deleted: 5074238e67149f99f5e8b3700196db97 before, abc123abc123abc123abc123 after",
         np.NaN],

        ["2529629", "magil-ggp-2529629-2022-03", "Georgia Board of Pharmacy",
         "magil", "0000", 15678, "1573937", 0.93, 1,
         "ARCHIVEIT-15678-TEST-JOB1573937-SEED2529629-20220314173634874-00000-h3.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "Empty report magil-ggp-2529629-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529629-2022-03_collscope.csv not saved",
         "API error 999: can't get info about ARCHIVEIT-15678-TEST-JOB1573937-SEED2529629-20220314173634874-00000-h3.warc.gz",
         np.NaN, np.NaN],

        ["2529631", "magil-ggp-2529631-2022-03", "Georgia Commission on Equal Opportunity",
         "magil", "0000", 15678, "1576492", 0.23, 1,
         "ARCHIVEIT-15678-TEST-JOB1576492-SEED2529631-20220318145704126-00000-h3.warc.gz",
         "Successfully calculated seed metadata",
         "magil-ggp-2529631-2022-03_seed.csv API error 999; magil-ggp-2529631-2022-03_seedscope.csv API error 999; magil-ggp-2529631-2022-03_collscope.csv API error 999; magil-ggp-2529631-2022-03_coll.csv API error 999; magil-ggp-2529631-2022-03_31104537364_crawldef.csv API error 999",
         "No additional information", np.NaN, np.NaN, np.NaN],

        ["2529634", "magil-ggp-2529634-2022-03", "Georgia Office of Attorney General Consumer Protection Division",
         "magil", "0000", 15678, "1576505", 1.2, 2,
         "ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220318151112447-00000-h3.warc.gz,ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220320234830550-00001-h3.warc.gz",
         "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["2529642", "magil-ggp-2529642-2022-03", "Georgia Department of Community Supervision",
         "magil", "0000", 15678, "1573937", 0.94, 1,
         "ARCHIVEIT-15678-TEST-JOB1573937-SEED2529642-20220314173635152-00000-h3.warc.gz",
         "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["2529652", "magil-ggp-2529652-2022-03", "Georgia Drugs and Narcotics Agency",
         "magil", "0000", 15678, "1576493", 0.19, 1,
         "ARCHIVEIT-15678-TEST-JOB1576493-SEED2529652-20220318145745554-00000-h3.warc.gz",
         "Successfully calculated seed metadata", np.NaN, np.NaN, np.NaN, np.NaN,np.NaN],

         ["2529660", "magil-ggp-2529660-2022-03", "Georgia Governor's Office of Disability Services Ombudsman",
         "magil", "0000", 15678, "1573937", 0.1, 1,
         "ARCHIVEIT-15678-TEST-JOB1573937-SEED2529660-20220314173634919-00000-h3.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "Empty report magil-ggp-2529660-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529660-2022-03_collscope.csv not saved",
         "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1573937-SEED2529660-20220314173634919-00000-h3.warc.gz",
         "Fixity for ARCHIVEIT-15678-TEST-JOB1573937-SEED2529660-20220314173634919-00000-h3.warc.gz cannot be extracted from md5deep output: b'@Something#Unexpected'",
         np.NaN],

        ["2529668", "magil-ggp-2529668-2022-03", "Georgia Office of the Child Advocate",
         "magil", "0000", 15678, "1577241", 0.39, 1,
         "ARCHIVEIT-15678-TEST-JOB1577241-SEED2529668-20220320182657667-00000-h3.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "Seed report does not have login columns to redact; Empty report magil-ggp-2529668-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529668-2022-03_collscope.csv not saved",
         np.NaN, np.NaN, np.NaN],

        ["2529676", "magil-ggp-2529676-2022-03", "Georgia State Board of Accountancy",
         "magil", "0000", 15678, "1577186", 0.19, 1,
         "ARCHIVEIT-15678-TEST-JOB1577186-SEED2529676-20220320144814871-00000-h3.warc.gz",
         "Successfully calculated seed metadata",
         "magil-ggp-2529676-2022-03_1577186_crawljob.csv API error 999; Crawl job was not downloaded so can't get crawl definition id",
         "Empty report magil-ggp-2529676-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529676-2022-03_collscope.csv not saved",
         np.NaN, np.NaN, np.NaN],

        ["2529681", "magil-ggp-2529681-2022-03", "Georgia State Properties Commission",
         "magil", "0000", 15678,"1577187", 0.33, 1,
         "ARCHIVEIT-15678-TEST-JOB1577187-SEED2529681-20220320144922213-00000-h3.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "Empty report magil-ggp-2529681-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529681-2022-03_collscope.csv not saved",
         np.NaN, np.NaN, np.NaN],

        ["2547114", "rbrl-506-web-202203-0001", "Latina South Podcast",
         "russell", "rbrl-506", 12265, "1465670", 0.03, 1,
         "ARCHIVEIT-12265-TEST-JOB1465670-0-SEED2547114-20210812134726007-00000-ja6tikrc.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "Empty report rbrl-506-web-202203-0001_seedscope.csv not saved",
         "API error 999: can't download ARCHIVEIT-12265-TEST-JOB1465670-0-SEED2547114-20210812134726007-00000-ja6tikrc.warc.gz"],

        ["2739136", "rbrl-499-web-202203-0001", "Savannah River Site",
         "russell", "rbrl-499", 12263, "1539793", 1.36, 2,
         "ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220110194710516-00000-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220111130848517-00001-h3.warc.gz",
         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
         "Empty report rbrl-499-web-202203-0001_seedscope.csv not saved",
         "API error 999: can't get info about ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220110194710516-00000-h3.warc.gz; API error 999: can't download ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220111130848517-00001-h3.warc.gz",
         np.NaN, np.NaN]]

    column_names = ["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                    "Size_GB", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                    "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"]

    df = pd.DataFrame(rows, columns=column_names)
    return df


def make_expected_aip_log_df():
    """Makes and returns a dataframe with the expected values for aip_log.
    Starts by making a list of the values for each row in the dataframe."""

    rows = [

        ["magil-ggp-2529627-2022-03", np.NaN, "Objects folder is empty.", np.NaN,
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529629-2022-03", np.NaN, "Objects folder is empty.", np.NaN,
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529631-2022-03", np.NaN, "Successfully created objects folder", "Successfully created metadata folder",
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529634-2022-03", np.NaN, "Successfully created objects folder", "Metadata folder is empty.",
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529642-2022-03", np.NaN, "Objects folder is missing.", np.NaN,
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529652-2022-03", np.NaN, "Successfully created objects folder", "Metadata folder is missing.",
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529660-2022-03", np.NaN, "Successfully created objects folder", "Successfully created metadata folder",
        np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529668-2022-03", np.NaN, "Successfully created objects folder", "Successfully created metadata folder",
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529676-2022-03", np.NaN, "Successfully created objects folder", "Successfully created metadata folder",
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["magil-ggp-2529681-2022-03", np.NaN, "Objects folder is empty.", np.NaN,
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["harg-0000-web-202203-0011", np.NaN, "Objects folder is missing.", np.NaN,
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["harg-0000-web-202203-0012", np.NaN, "Successfully created objects folder", "Successfully created metadata folder",
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["rbrl-246-web-202203-0001", np.NaN, "Successfully created objects folder", "Successfully created metadata folder",
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["rbrl-246-web-202203-0002", np.NaN, "Objects folder is missing.", np.NaN,
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["rbrl-499-web-202203-0001", np.NaN, "Objects folder is empty.", np.NaN,
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

        ["rbrl-506-web-202203-0001", np.NaN, "Objects folder is empty.", np.NaN,
         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]

    column_names = ["AIP ID", "Files Deleted", "Objects Folder", "Metadata Folder", "FITS Tool Errors",
                    "FITS Combination Errors", "Preservation.xml Made", "Preservation.xml Valid",
                    "Bag Valid", "Package Errors", "Manifest Errors", "Processing Complete"]

    df = pd.DataFrame(rows, columns=column_names)
    return df


def make_directory_structure_df():
    """Makes and returns a dataframe with the expected relative path for every file in the aips directory.
    Starts by making a list of the paths."""

    paths = [r"aips_2022-03-24\aip_log.csv",
             r"aips_2022-03-24\seeds.csv",
             r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_1497166_crawljob.csv",
             r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_31104307871_crawldef.csv",
             r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_coll.csv",
             r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_collscope.csv",
             r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_seed.csv",
             r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_seedscope.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529627-2022-03\metadata\magil-ggp-2529627-2022-03_1573937_crawljob.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529627-2022-03\metadata\magil-ggp-2529627-2022-03_31104535848_crawldef.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529627-2022-03\metadata\magil-ggp-2529627-2022-03_coll.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529627-2022-03\metadata\magil-ggp-2529627-2022-03_seed.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529629-2022-03\metadata\magil-ggp-2529629-2022-03_1573937_crawljob.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529629-2022-03\metadata\magil-ggp-2529629-2022-03_31104535848_crawldef.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529629-2022-03\metadata\magil-ggp-2529629-2022-03_coll.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529629-2022-03\metadata\magil-ggp-2529629-2022-03_seed.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529634-2022-03\objects\warc_placeholder.txt",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529652-2022-03\objects\warc_placeholder.txt",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529681-2022-03\metadata\magil-ggp-2529681-2022-03_1577187_crawljob.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529681-2022-03\metadata\magil-ggp-2529681-2022-03_31104537580_crawldef.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529681-2022-03\metadata\magil-ggp-2529681-2022-03_coll.csv",
             r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529681-2022-03\metadata\magil-ggp-2529681-2022-03_seed.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-499-web-202203-0001\metadata\rbrl-499-web-202203-0001_1539793_crawljob.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-499-web-202203-0001\metadata\rbrl-499-web-202203-0001_31104517628_crawldef.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-499-web-202203-0001\metadata\rbrl-499-web-202203-0001_coll.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-499-web-202203-0001\metadata\rbrl-499-web-202203-0001_collscope.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-499-web-202203-0001\metadata\rbrl-499-web-202203-0001_seed.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-506-web-202203-0001\metadata\rbrl-506-web-202203-0001_1465670_crawljob.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-506-web-202203-0001\metadata\rbrl-506-web-202203-0001_31104479829_crawldef.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-506-web-202203-0001\metadata\rbrl-506-web-202203-0001_coll.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-506-web-202203-0001\metadata\rbrl-506-web-202203-0001_collscope.csv",
             r"aips_2022-03-24\errors\incomplete_directory\rbrl-506-web-202203-0001\metadata\rbrl-506-web-202203-0001_seed.csv",
             r"aips_2022-03-24\harg-0000-web-202203-0012\metadata\harg-0000-web-202203-0012_1490068_crawljob.csv",
             r"aips_2022-03-24\harg-0000-web-202203-0012\metadata\harg-0000-web-202203-0012_1493118_crawljob.csv",
             r"aips_2022-03-24\harg-0000-web-202203-0012\metadata\harg-0000-web-202203-0012_1496119_crawljob.csv",
             r"aips_2022-03-24\harg-0000-web-202203-0012\metadata\harg-0000-web-202203-0012_31104307305_crawldef.csv",
             r"aips_2022-03-24\harg-0000-web-202203-0012\metadata\harg-0000-web-202203-0012_coll.csv",
             r"aips_2022-03-24\harg-0000-web-202203-0012\metadata\harg-0000-web-202203-0012_collscope.csv",
             r"aips_2022-03-24\harg-0000-web-202203-0012\metadata\harg-0000-web-202203-0012_seed.csv",
             r"aips_2022-03-24\harg-0000-web-202203-0012\objects\ARCHIVEIT-12912-WEEKLY-JOB1490068-SEED2173769-20210929221708074-00000-h3.warc",
             r"aips_2022-03-24\harg-0000-web-202203-0012\objects\ARCHIVEIT-12912-WEEKLY-JOB1493118-SEED2173769-20211006221709530-00000-h3.warc",
             r"aips_2022-03-24\magil-ggp-2529631-2022-03\metadata\magil-ggp-2529631-2022-03_1576492_crawljob.csv",
             r"aips_2022-03-24\magil-ggp-2529631-2022-03\objects\warc_placeholder.txt",
             r"aips_2022-03-24\magil-ggp-2529660-2022-03\metadata\magil-ggp-2529660-2022-03_1573937_crawljob.csv",
             r"aips_2022-03-24\magil-ggp-2529660-2022-03\metadata\magil-ggp-2529660-2022-03_31104535848_crawldef.csv",
             r"aips_2022-03-24\magil-ggp-2529660-2022-03\metadata\magil-ggp-2529660-2022-03_coll.csv",
             r"aips_2022-03-24\magil-ggp-2529660-2022-03\metadata\magil-ggp-2529660-2022-03_seed.csv",
             r"aips_2022-03-24\magil-ggp-2529660-2022-03\objects\ARCHIVEIT-15678-TEST-JOB1573937-SEED2529660-20220314173634919-00000-h3.warc.gz",
             r"aips_2022-03-24\magil-ggp-2529668-2022-03\metadata\magil-ggp-2529668-2022-03_1577241_crawljob.csv",
             r"aips_2022-03-24\magil-ggp-2529668-2022-03\metadata\magil-ggp-2529668-2022-03_31104537585_crawldef.csv",
             r"aips_2022-03-24\magil-ggp-2529668-2022-03\metadata\magil-ggp-2529668-2022-03_coll.csv",
             r"aips_2022-03-24\magil-ggp-2529668-2022-03\metadata\magil-ggp-2529668-2022-03_seed.csv",
             r"aips_2022-03-24\magil-ggp-2529668-2022-03\objects\warc_placeholder.txt",
             r"aips_2022-03-24\magil-ggp-2529676-2022-03\metadata\magil-ggp-2529676-2022-03_coll.csv",
             r"aips_2022-03-24\magil-ggp-2529676-2022-03\metadata\magil-ggp-2529676-2022-03_seed.csv",
             r"aips_2022-03-24\magil-ggp-2529676-2022-03\objects\warc_placeholder.txt",
             r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_1542317_crawljob.csv",
             r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_31104519079_crawldef.csv",
             r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_coll.csv",
             r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_collscope.csv",
             r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_seed.csv",
             r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_seedscope.csv",
             r"aips_2022-03-24\rbrl-246-web-202203-0001\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz",
             r"aips_2022-03-24\rbrl-246-web-202203-0001\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz.open",]

    df = pd.DataFrame({"File_Path": paths})
    return df


# ----------------------------------------------------------------------------------------------------------------
# ALTERNATIVE VERSIONS OF FUNCTIONS THAT GENERATE ERRORS.
# IF THERE IS MORE THAN ONE ERROR NEEDED, ADDS AN ARGUMENT FOR ERROR_TYPE TO SPECIFY.
# ----------------------------------------------------------------------------------------------------------------
def download_metadata(seed, seed_df, error_type):
    """"Uses the Partner API to download six metadata reports to include in the AIPs for archived websites,
    deletes any empty reports (meaning there was no data of that type for this seed),
    and redacts login information from the seed report.
    Any errors are added to the seed dataframe and saved to the script log at the end of the function."""

    def get_report(filter_type, filter_value, report_type, report_name):
        """Downloads a single metadata report and saves it as a csv in the AIP's metadata folder.
            Only saves if there is data of that type and also redacts the login info from the seed report.
            filter_type and filter_value are used to filter the API call to the right AIP's report
            report_type is the Archive-It name for the report
            report_name is the name for the report saved in the AIP, including the ARCHive metadata code """

        # Builds the API call to get the report as a csv.
        # Limit of -1 will return all matches. Default is only the first 100.
        filters = {"limit": -1, filter_type: filter_value, "format": "csv"}
        metadata_report = requests.get(f"{c.partner_api}/{report_type}", params=filters, auth=(c.username, c.password))

        # GENERATE ERROR 1: API error downloading metadata reports.
        # Doesn't make an error for crawl_job so that crawl_def error handling can be done separately.
        if error_type == "download" and not(report_type == "crawl_job"):
            metadata_report.status_code = 999

        # GENERATE ERROR 2: API error downloading crawl_job.
        # Without crawl_job, crawl definition will also not download as part of standard error handling.
        if error_type == "crawl_job" and report_type == "crawl_job":
            metadata_report.status_code = 999

        # Saves the metadata report if there were no API errors and there was data of this type (content isn't empty).
        # For scope rules, it is common for one or both to not have data since these aren't required.
        if metadata_report.status_code == 200:
            if metadata_report.content == b"":
                web.log(f"Empty report {report_name} not saved", seed_df, row_index, "Metadata_Report_Info")
                return
            else:
                with open(f"{seed.AIP_ID}/metadata/{report_name}", "wb") as report_csv:
                    report_csv.write(metadata_report.content)
        else:
            web.log(f"{report_name} API error {metadata_report.status_code}", seed_df, row_index, "Metadata_Report_Errors")
            return

        # Replaces the seed report with a redacted version of the file, removing login information if those columns
        # are present. Even if the columns are blank, replaces it with REDACTED. Since not all login information is
        # meaningful (some is from staff web browsers autofill information while viewing the metadata), knowing if
        # there was login information or not is misleading. ""
        if report_type == "seed":
            report_df = pd.read_csv(f"{seed.AIP_ID}/metadata/{report_name}")

            # GENERATE ERROR 3: No login columns in seed to redact.
            if error_type == "redact" and "login_password" in report_df.columns:
                report_df.drop(["login_username", "login_password"], axis=1, inplace=True)
                report_df.to_csv(f"{seed.AIP_ID}/metadata/{report_name}", index=False)

            if "login_password" in report_df.columns:
                report_df["login_username"] = "REDACTED"
                report_df["login_password"] = "REDACTED"
                report_df.to_csv(f"{seed.AIP_ID}/metadata/{report_name}")
            else:
                web.log("Seed report does not have login columns to redact", seed_df, row_index, "Metadata_Report_Info")

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # Downloads four of the six metadata reports from Archive-It needed to understand the context of the WARC.
    # These are reports where there is only one report per seed or collection.
    get_report("id", seed.Seed_ID, "seed", f"{seed.AIP_ID}_seed.csv")
    get_report("seed", seed.Seed_ID, "scope_rule", f"{seed.AIP_ID}_seedscope.csv")
    get_report("collection", seed.AIT_Collection, "scope_rule", f"{seed.AIP_ID}_collscope.csv")
    get_report("id", seed.AIT_Collection, "collection", f"{seed.AIP_ID}_coll.csv")

    # Downloads each of the crawl job reports and its corresponding crawl definition report (if new).
    # If a seed has more than one, Job_ID has a comma-separated string of the IDs.
    job_list = seed.Job_ID.split(",")
    for job in job_list:
        get_report("id", job, "crawl_job", f"{seed.AIP_ID}_{job}_crawljob.csv")
        try:
            report_df = pd.read_csv(f"{seed.AIP_ID}/metadata/{seed.AIP_ID}_{job}_crawljob.csv", dtype="object")
            crawl_def_id = report_df.loc[0, "crawl_definition"]
            if not os.path.exists(f"{seed.AIP_ID}/metadata/{seed.AIP_ID}_{crawl_def_id}_crawldef.csv"):
                get_report("id", crawl_def_id, "crawl_definition", f"{seed.AIP_ID}_{crawl_def_id}_crawldef.csv")
        except FileNotFoundError:
            web.log("Crawl job was not downloaded so can't get crawl definition id", seed_df, row_index,
                "Metadata_Report_Errors")

    # If there were no download errors (the dataframe still has no value in that cell), updates the log to show success.
    if pd.isnull(seed_df.at[row_index, "Metadata_Report_Errors"]):
        seed_df.loc[row_index, "Metadata_Report_Errors"] = "Successfully downloaded all metadata reports"
        seed_df.to_csv(os.path.join(c.script_output, "seeds.csv"), index=False)

    # If there is nothing in the report info field, updates the log with default text.
    # Can't assume that blank means success because it could mean API errors.
    if pd.isnull(seed_df.at[row_index, "Metadata_Report_Info"]):
        seed_df.loc[row_index, "Metadata_Report_Info"] = "No additional information"
        seed_df.to_csv(os.path.join(c.script_output, "seeds.csv"), index=False)


def download_warcs(seed, date_end, seed_df, error_type):
    """Downloads every WARC file and verifies that fixity is unchanged after downloading.

    FOR ERROR TESTING: make a text file instead of downloading because it is faster and add variable status_code
    to be able to test error handling from status code without having done an API call."""

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # Makes a list of the filenames for all WARCs for this seed.
    warc_names = seed.WARC_Filenames.split(",")

    # Downloads and validates every WARC.
    # If an error is caught at any point, logs the error and starts the next WARC.
    # FOR TESTING, ADD WARC COUNTER FOR WHEN DIFFERENT ERRORS HAPPEN TO DIFFERENT WARCS.
    warc_count = 0
    for warc in warc_names:
        warc_count += 1

        # Gets URL for downloading the WARC and WARC MD5 from Archive-It using WASAPI.
        warc_data = requests.get(f'{c.wasapi}?filename={warc}', auth=(c.username, c.password))

        # GENERATE ERROR 4: API error downloading WARC metadata.
        if error_type == "metadata" or (error_type == "everything" and warc_count == 1):
            warc_data.status_code = 999

        # Gets URL for downloading the WARC and WARC MD5 from Archive-It using WASAPI. (continued)
        if not warc_data.status_code == 200:
            web.log(f"API error {warc_data.status_code}: can't get info about {warc}",
                    seed_df, row_index, "WARC_API_Errors")
            continue
        py_warc = warc_data.json()
        warc_url = py_warc["files"][0]["locations"][0]
        warc_md5 = py_warc["files"][0]["checksums"]["md5"]

        # The path for where the WARC will be saved on the local machine (it is long and used twice in this script).
        warc_path = f'{c.script_output}/aips_{date_end}/{seed.AIP_ID}/objects/{warc}'

        # Downloads the WARC, which will be zipped.
        # Or if will later make text file for quicker testing, assign md5 and status code.
        # status_code variable is needed because anything that wasn't really downloaded doesn't have warc_download.status_code
        if error_type in ("fixity", "unzip") or (error_type == "everything" and warc_count == 7):
            warc_download = requests.get(f"{warc_url}", auth=(c.username, c.password))
            status_code = warc_download.status_code
        else:
            warc_md5 = "18c7f874cbf0b4de2dfb5dbeb46ac659"
            status_code = 200

        # GENERATE ERROR 5: API error downloading WARC.
        if error_type == "download" or (error_type == "everything" and warc_count == 2):
            status_code = 999

        # GENERATE ERROR 6: API error downloading the first WARC when seed has 2.
        if error_type == "download_partial" and warc_count == 1:
            status_code = 999

        # If there was an error with the API call, starts the next WARC.
        if not status_code == 200:
            web.log(f"API error {status_code}: can't download {warc}",
                    seed_df, row_index, "WARC_API_Errors")
            continue
        else:
            web.log(f"Successfully downloaded {warc}", seed_df, row_index, "WARC_API_Errors")

        # Saves the zipped WARC in the objects folder, keeping the original filename.
        # Saves a text file with the warc filename and extension for quicker tests if it isn't needed for the test.
        with open(warc_path, 'wb') as warc_file:
            if error_type in ("fixity", "unzip") or (error_type == "everything" and warc_count == 7):
                warc_file.write(warc_download.content)
            else:
                warc_file.write(b"Testing Text")

        # Calculates the md5 for the downloaded zipped WARC with md5deep.
        md5deep_output = subprocess.run(f'"{c.MD5DEEP}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)

        # GENERATES ERROR 7: Cannot extract fixity from MD5deep output (doesn't match regex pattern).
        if error_type == "md5deep" or (error_type == "everything" and warc_count == 3):
            md5deep_output.stdout = b"@Something#Unexpected"

        # Calculates the md5 for the downloaded zipped WARC with md5deep. (continued)
        try:
            regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(md5deep_output.stdout))
            downloaded_warc_md5 = regex_md5.group(1)
        except AttributeError:
            web.log(f"Fixity for {warc} cannot be extracted from md5deep output: {md5deep_output.stdout}",
                    seed_df, row_index, "WARC_Fixity_Errors")
            continue

        # GENERATES ERROR 8: WARC fixity after download doesn't match Archive-It record.
        if error_type == "fixity" or (error_type == "everything" and warc_count == 7):
            downloaded_warc_md5 = "abc123abc123abc123abc123"

        # Compares the md5 of the downloaded zipped WARC to Archive-It metadata.
        # If the md5 has changed, deletes the WARC.
        if not warc_md5 == downloaded_warc_md5:
            os.remove(warc_path)
            web.log(f"Fixity for {warc} changed and it was deleted: {warc_md5} before, {downloaded_warc_md5} after",
                    seed_df, row_index, "WARC_Fixity_Errors")
            continue
        else:
            web.log(f"Successfully verified {warc} fixity on {datetime.datetime.now()}",
                    seed_df, row_index, "WARC_Fixity_Errors")

        # PREVENTS ERROR: If text files are used in place of WARCs, rename to simulate unzipping.
        # Otherwise, the unzip steps give an error because the text files are no real zips.
        if error_type in ("fixity", "unzip") or (error_type == "everything" and warc_count == 4):
            # Extracts the WARC from the gzip file.
            unzip_output = subprocess.run(f'"C:/Program Files/7-Zip/7z.exe" x "{warc_path}" -o"{seed.AIP_ID}/objects"',
                                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True)

            # GENERATES ERROR 9: Error unzipping WARC.
            if error_type == "unzip" or (error_type == "everything" and warc_count == 4):
                unzip_output.stderr = b'Error message stand-in.'

            # Deletes the gzip file, unless 7zip had an error during unzipping.
            if unzip_output.stderr == b'':
                os.remove(warc_path)
                web.log(f"Successfully unzipped {warc}", seed_df, row_index, "WARC_Unzip_Errors")
            else:
                web.log(f"Error unzipping {warc}: {unzip_output.stderr.decode('utf-8')}",
                        seed_df, row_index, "WARC_Unzip_Errors")
        else:
            new_warc_path = warc_path.replace(".warc.gz", ".warc")
            os.replace(warc_path, new_warc_path)
            web.log(f"Successfully unzipped {warc}", seed_df, row_index, "WARC_Unzip_Errors")

        # Wait 15 second to give the API a rest.
        time.sleep(15)


def make_warc_placeholder(aip_id):
    """For tests that do not involve the objects folder, makes a text file inside the objects folder
    instead of downloading the WARC(s), which makes the test faster and prevents an empty objects folder error.
    It only makes one file, not the number of warcs associated with that AIP."""

    with open(os.path.join(aip_id, "objects", "warc_placeholder.txt"), "w") as warc_file:
        warc_file.write("WARC placeholder for testing")


# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT IS THE SAME AS web_aip_batch.py TO SET UP EVERYTHING CORRECTLY BEFORE THE DESIRED TESTS.
# ERROR HANDLING FOR SCRIPT ARGUMENTS AND THE CONFIGURATION FILE ARE TESTED BY GIVING THE WRONG INPUTS INSTEAD.
# Only difference is that I removed the code that lets the script keep restarting.
# ----------------------------------------------------------------------------------------------------------------

# The start and end dates that the test script requires to give predictable results.
# In the actual script, these are arguments.
date_start = "2021-10-02"
date_end = "2022-03-24"

# Tests the paths in the configuration file.
# In this case, making sure there are not unexpected errors which will impact these tests.
# Verifying the error handling for the configuration file isn't part of this script.
configuration_errors = a.check_configuration()
if len(configuration_errors) > 0:
    print("/nProblems detected with configuration.py:")
    for error in configuration_errors:
        print(error)
    print("Correct the configuration file and run the script again.")
    sys.exit()

# Makes a folder for AIPs within the script_output folder and makes it the current directory.
aips_directory = os.path.join(c.script_output, f"aips_{date_end}")
os.makedirs(aips_directory)
os.chdir(aips_directory)

# Gets the metadata about the seeds in the batch.
seed_df = web.seed_data(date_start, date_end)

# Makes the log for the AIP part of the script (used by check_directory).
a.log("header")

# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT MAKES A DIFFERENT ERROR EVERY TIME IT STARTS A NEW SEED.
# FOR ERRORS GENERATING WITHIN FUNCTIONS, IT USES A DIFFERENT VERSION OF THE FUNCTION.
# ----------------------------------------------------------------------------------------------------------------

# Errors are listed in script order. Seeds are processed out of order to get desired input for specific tests.
# For each seed, calls web functions but does not call the aip functions, which are tested elsewhere.
# Only calls the download_warcs() function if it is needed for the test, since downloading is slow.
for seed in seed_df.itertuples():

    # Displays the script progress.
    print(f"Processing seed {seed.Seed_ID} (AIP {seed.AIP_ID}).")

    # ERROR 1: API error downloading metadata reports.
    if seed.Seed_ID == "2529631":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        download_metadata(seed, seed_df, error_type="download")
        make_warc_placeholder(seed.AIP_ID)
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 2: API error downloading crawl_job so can't download crawl_definition.
    if seed.Seed_ID == "2529676":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        download_metadata(seed, seed_df, error_type="crawl_job")
        make_warc_placeholder(seed.AIP_ID)
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 3: No login columns in seed to redact.
    if seed.Seed_ID == "2529668":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        download_metadata(seed, seed_df, error_type="redact")
        make_warc_placeholder(seed.AIP_ID)
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 4: API error downloading WARC metadata. AIP has 1 WARC.
    if seed.Seed_ID == "2529629":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="metadata")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 5: API error downloading WARC. AIP has 1 WARC.
    if seed.Seed_ID == "2547114":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="download")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 6: API error downloading first WARC. AIP has 2 WARCs.
    if seed.Seed_ID == "2173769":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="download_partial")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 7: Cannot extract fixity from MD5deep output. AIP has 1 WARC.
    if seed.Seed_ID == "2529660":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="md5deep")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 8: WARC fixity after download doesn't match Archive-It record. AIP has 1 WARC.
    if seed.Seed_ID == "2529627":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="fixity")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 9: Error unzipping WARC. AIP has 1 WARC.
    if seed.Seed_ID == "2454507":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="unzip")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 10: All WARC errors happen to a single WARC and other WARCs have no errors.
    if seed.Seed_ID == "2739136":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="everything")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 11: No objects folder.
    if seed.Seed_ID == "2184360":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        web.download_metadata(seed, seed_df)
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 12: Objects folder is empty.
    if seed.Seed_ID == "2529681":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 13: No metadata folder.
    if seed.Seed_ID == "2529652":
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        make_warc_placeholder(seed.AIP_ID)
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # Error 14: Metadata folder is empty.
    if seed.Seed_ID == "2529634":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        make_warc_placeholder(seed.AIP_ID)
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # Error 15: Objects and metadata folders are missing.
    if seed.Seed_ID == "2454506":
        os.makedirs(seed.AIP_ID)
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # Error 16: Objects folder is missing; metadata folder is empty.
    if seed.Seed_ID == "2529642":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

# Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) and logs into the AIPs folder
# to keep everything together if another set is downloaded before these are deleted.
os.chdir(c.script_output)
to_move = ("aips-to-ingest", "errors", "fits-xml", "preservation-xml",
           "seeds.csv", "aip_log.csv", "completeness_check.csv")
for item in os.listdir("."):
    if item in to_move:
        os.replace(item, f"{aips_directory}/{item}")

# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT TESTS THE ACTUAL RESULTS AGAINST THE EXPECTED RESULTS.
# TESTS seed_df, aip_log.csv AND AIPS DIRECTORY STRUCTURE.
# COMPARISONS ARE SAVED AS TABS ON A SPREADSHEET IN THE SCRIPT OUTPUT FOLDER.
# ----------------------------------------------------------------------------------------------------------------

# Checks the values in seed_df against the expected values.
# Removes the timestamps from seed_df so the data can be compared to consistent expected values.
expected_seed_df = make_expected_seed_df()
seed_df["WARC_Fixity_Errors"] = seed_df["WARC_Fixity_Errors"].str.replace(" \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}", "")
compare_seed_df = seed_df.merge(expected_seed_df, indicator=True, how="outer")

# Checks the values in aip_log.csv against the expected values.
# Removes Time Started from aip_log.csv so the data can be compared to consistent expected values.
expected_aip_log_df = make_expected_aip_log_df()
aip_log_df = pd.read_csv(os.path.join(aips_directory, "aip_log.csv"))
aip_log_df.drop(["Time Started"], axis=1, inplace=True)
compare_aip_df = aip_log_df.merge(expected_aip_log_df, indicator=True, how="outer")

# Checks the directory structure against the expected directory structure.
# Makes a dataframe with the relative paths in the AIPs directory structure and compares to the expected paths.
directory_list = []
for root, dirs, files in os.walk("aips_2022-03-24"):
    for file in files:
        directory_list.append(os.path.join(root, file))
directory_df = pd.DataFrame({"File_Path": directory_list})
expected_directory_df = make_directory_structure_df()
compare_dir_df = directory_df.merge(expected_directory_df, indicator=True, how="outer")

# Saves the results to tabs on a spreadsheet (error_test_results.xlsx) in the script output folder.
with pd.ExcelWriter(os.path.join(c.script_output, "error_test_results.xlsx")) as results:
    compare_seed_df.to_excel(results, sheet_name="Seeds_CSV", index=False)
    compare_aip_df.to_excel(results, sheet_name="AIP_CSV", index=False)
    compare_dir_df.to_excel(results, sheet_name="Directory", index=False)

# Removes the rows that match from the dataframes and prints if there were mismatches or not.
compare_seed_df = compare_seed_df[compare_seed_df["_merge"] != "both"]
if len(compare_seed_df) == 0:
    print("\nTests passed for seeds.csv")
else:
    print("\nPossible errors with seeds.csv: check error_test_results.xlsx")
    print("Test passes if the difference is no login to redact in seed report. AIT data is not consistent.")
compare_aip_df = compare_aip_df[compare_aip_df["_merge"] != "both"]
if len(compare_aip_df) == 0:
    print("Tests passed for aip_log.csv")
else:
    print("Possible errors with aip_log: check error_test_results.xlsx")
compare_dir_df = compare_dir_df[compare_dir_df["_merge"] != "both"]
if len(compare_dir_df) == 0:
    print("Tests passed for directory structure.")
else:
    print("Possible errors with directory structure: check error_test_results.xlsx")

print('\nScript is complete.')
