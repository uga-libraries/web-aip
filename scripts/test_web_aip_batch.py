"""Purpose: This script generates every known error to use for testing the error handling of web_aip_batch.py.

Usage: python /path/test_web_aip_batch.py
Date values need to be 2022-03-20 and 2022-03-25 to test for predictable results,
so included in this script and not as arguments.

"""

import csv
import datetime
import os
import pandas as pd
import re
import requests
import shutil
import subprocess
import sys
import time

# Import functions and constant variables from other UGA scripts.
import aip_functions as a
import configuration as c
import web_functions as web


# ----------------------------------------------------------------------------------------------------------------
# ALTERNATIVE VERSIONS OF FUNCTIONS THAT GENERATE ERRORS.
# IF THERE IS MORE THAN ONE ERROR NEEDED, ADDS AN ARGUMENT FOR ERROR_TYPE TO SPECIFY.
# ----------------------------------------------------------------------------------------------------------------
def download_metadata(seed, seed_df, error_type):
    """Uses the Partner API to download six metadata reports to include in the AIPs for archived websites,
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

        # GENERATE ERROR 1
        # Changes the API status code for all metadata reports except crawl job.
        if error_type == "download" and not(report_type == "crawl_job"):
            metadata_report.status_code = 999
            print(f"Generated error with API status code when downloading {report_name}.")

        # GENERATE ERROR 2
        # Changes the API status code for crawl job.
        # Without that, crawl definition will also not download as part of standard error handling.
        if error_type == "crawl_job" and report_type == "crawl_job":
            metadata_report.status_code = 999
            print(f"Generated error with API status code when downloading {report_name}.")

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
            web.log(f"{report_type} API error {metadata_report.status_code}", seed_df, row_index, "Metadata_Report_Errors")
            return

        # Replaces the seed report with a redacted version of the file, removing login information if those columns
        # are present. Even if the columns are blank, replaces it with REDACTED. Since not all login information is
        # meaningful (some is from staff web browsers autofill information while viewing the metadata), knowing if
        # there was login information or not is misleading. """
        if report_type == "seed":
            report_df = pd.read_csv(f"{seed.AIP_ID}/metadata/{report_name}")
            if "login_password" in report_df.columns:
                report_df["login_username"] = "REDACTED"
                report_df["login_password"] = "REDACTED"
                report_df.to_csv(f"{seed.AIP_ID}/metadata/{report_name}")
            else:
                web.log("Seed report does not have login columns to redact", seed_df, row_index, "Metadata_Report_Info")

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # Downloads five of the six metadata reports from Archive-It needed to understand the context of the WARC.
    # These are reports where there is only one report per seed or collection.
    get_report("id", seed.Seed_ID, "seed", f"{seed.AIP_ID}_seed.csv")
    get_report("seed", seed.Seed_ID, "scope_rule", f"{seed.AIP_ID}_seedscope.csv")
    get_report("collection", seed.AIT_Collection, "scope_rule", f"{seed.AIP_ID}_collscope.csv")
    get_report("id", seed.AIT_Collection, "collection", f"{seed.AIP_ID}_coll.csv")
    get_report("id", seed.Job_ID, "crawl_job", f"{seed.AIP_ID}_{seed.Job_ID}_crawljob.csv")

    # Gets the crawl definition id from the crawl job report and downloads it.
    try:
        report_df = pd.read_csv(f"{seed.AIP_ID}/metadata/{seed.AIP_ID}_{seed.Job_ID}_crawljob.csv", dtype="object")
        crawl_def_id = report_df.loc[0, "crawl_definition"]
        get_report("id", crawl_def_id, "crawl_definition", f"{seed.Seed_ID}_{crawl_def_id}_crawldef.csv")
    except FileNotFoundError:
        web.log("Crawl job was not downloaded so can't get crawl definition id", seed_df, row_index, "Metadata_Report_Errors")

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
    for warc in warc_names:

        # Gets URL for downloading the WARC and WARC MD5 from Archive-It using WASAPI.
        warc_data = requests.get(f'{c.wasapi}?filename={warc}', auth=(c.username, c.password))

        # GENERATE ERROR 3: API error when getting WARC metadata.
        if error_type == "metadata":
            warc_data.status_code = 999
            print(f"Generated error with API status code when downloading metadata for {warc}.")

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
        if error_type == "fixity":
            warc_download = requests.get(f"{warc_url}", auth=(c.username, c.password))
            status_code = warc_download.status_code
        else:
            warc_md5 = "18c7f874cbf0b4de2dfb5dbeb46ac659"
            status_code = 200

        # GENERATE ERROR 4: API error when downloading WARC
        if error_type == "download":
            status_code = 999
            print(f"Generated error with API status code when downloading {warc}.")

        # If there was an error with the API call, starts the next WARC.
        if not status_code == 200:
            web.log(f"API error {status_code}: can't download {warc}",
                    seed_df, row_index, "WARC_API_Errors")
            continue
        else:
            web.log(f"Successfully downloaded {warc}", seed_df, row_index, "WARC_API_Errors")

        # Saves the zipped WARC in the objects folder, keeping the original filename.
        with open(warc_path, 'wb') as warc_file:
            if error_type == "fixity":
                warc_file.write(warc_download.content)
            else:
                warc_file.write(b"Testing Text")

        # Calculates the md5 for the downloaded zipped WARC with md5deep.
        md5deep_output = subprocess.run(f'"{c.MD5DEEP}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)

        # GENERATES ERROR 5: regular expression doesn't work on md5deep output.
        if error_type == "md5deep":
            md5deep_output.stdout = b"@Something#Unexpected"
            print(f"Generated error with MD5deep output format for {warc}.")

        # Calculates the md5 for the downloaded zipped WARC with md5deep. (continued)
        try:
            regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(md5deep_output.stdout))
            downloaded_warc_md5 = regex_md5.group(1)
        except AttributeError:
            web.log(f"Fixity for {warc} cannot be extracted from md5deep output: {md5deep_output.stdout}",
                    seed_df, row_index, "WARC_Fixity_Errors")
            continue

        # GENERATES ERROR 6: fixity in API is different from downloaded file.
        if error_type == "fixity":
            downloaded_warc_md5 = "abc123abc123abc123abc123"
            print(f"Generated error with downloaded file MD5 for {warc}.")

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

        # Extracts the WARC from the gzip file.
        unzip_output = subprocess.run(f'"C:/Program Files/7-Zip/7z.exe" x "{warc_path}" -o"{seed.AIP_ID}/objects"',
                                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True)

        # GENERATES ERROR 7: Unable to unzip.
        if error_type == "unzip":
            unzip_output.stderr = b'Error message stand-in.'
            print(f"Generated error with 7zip output for {warc}.")

        # Deletes the gzip file, unless 7zip had an error during unzipping.
        if unzip_output.stderr == b'':
            os.remove(warc_path)
            web.log(f"Successfully unzipped {warc}", seed_df, row_index, "WARC_Unzip_Errors")
        else:
            web.log(f"Error unzipping {warc}: {unzip_output.stderr.decode('utf-8')}",
                    seed_df, row_index, "WARC_Unzip_Errors")

        # Wait 15 second to give the API a rest.
        time.sleep(15)


# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT IS THE SAME AS web_aip_batch.py TO SET UP EVERYTHING CORRECTLY BEFORE THE DESIRED TESTS.
# ERROR HANDLING FOR SCRIPT ARGUMENTS AND THE CONFIGURATION FILE ARE TESTED BY GIVING THE WRONG INPUTS INSTEAD.
# Only difference is that I removed the code that lets the script keep restarting.
# ----------------------------------------------------------------------------------------------------------------

# The start and end dates that the test script requires to give predictable results.
# In the actual script, these are arguments.
date_start = "2021-10-17"
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

# Makes the output directories and log for the AIP part of the script.
a.make_output_directories()
a.log("header")

# Starts counters for tracking script progress.
current_seed = 0
total_seeds = len(seed_df)

# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT MAKES A DIFFERENT ERROR EVERY TIME IT STARTS A NEW SEED.
# FOR ERRORS GENERATING WITHIN FUNCTIONS, IT USES A DIFFERENT VERSION OF THE FUNCTION.
# PRINTS AN ERROR TO THE TERMINAL IF THE ERROR IS NOT CAUGHT AND STARTS THE LOOP WITH THE NEXT WARC.
# ----------------------------------------------------------------------------------------------------------------

# There should be 14 seeds total. Not all are needed for testing.
for seed in seed_df.itertuples():

    # Updates the current WARC number and displays the script progress.
    current_seed += 1
    # For building the tests: stop iterating once enough seeds have been used to do all the current tests.
    if current_seed == 8:
        print("\nTests are done!")
        break
    print(f"\nProcessing seed {current_seed} of {total_seeds}.")

    # Makes output directories.
    os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
    os.makedirs(os.path.join(seed.AIP_ID, "objects"))

    # Makes AIP instance. In production, this isn't done after downloading.
    # For testing, do it here so don't have to repeat the code for every seed.
    aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, version=1, to_zip=True)

    # ERROR 1: API error downloading metadata reports.
    if current_seed == 1:
        download_metadata(seed, seed_df, error_type="download")

    # ERROR 2: API error downloading crawl_job so can't download crawl_definition.
    if current_seed == 2:
        download_metadata(seed, seed_df, error_type="crawl_job")

    # ERROR 3: API error downloading WARC metadata.
    if current_seed == 3:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="metadata")

    # ERROR 4: API error downloading WARC.
    if current_seed == 4:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="download")

    # ERROR 5: Cannot extract fixity from MD5Deep output.
    if current_seed == 6:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="md5deep")

    # ERROR 6: WARC fixity after download doesn't match Archive-It record.
    if current_seed == 5:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="fixity")

    # ERROR 7: Error unzipping WARC
    if current_seed == 7:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="unzip")

    # # ERROR 8: No objects folder
    # if current_seed == 8:
    #     web.download_metadata(seed, seed_df)
    #     web.download_warcs(seed, date_end, seed_df)
    #     web.check_directory(aip)
    #
    # # ERROR 9: Objects folder is empty
    # if current_seed == 9:
    #     web.download_metadata(seed, seed_df)
    #     web.download_warcs(seed, date_end, seed_df)
    #     web.check_directory(aip)
    #
    # # ERROR 10: No metadata folder
    # if current_seed == 10:
    #     web.download_metadata(seed, seed_df)
    #     web.download_warcs(seed, date_end, seed_df)
    #     web.check_directory(aip)
    #
    # # Error 11: Metadata folder is empty
    # if current_seed == 11:
    #     web.download_metadata(seed, seed_df)
    #     web.download_warcs(seed, date_end, seed_df)
    #     web.check_directory(aip)

# Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) and logs into the AIPs folder
# to keep everything together if another set is downloaded before these are deleted.
os.chdir(c.script_output)
to_move = ("aips-to-ingest", "errors", "fits-xml", "preservation-xml",
           "seeds.csv", "aip_log.csv", "completeness_check.csv")
for item in os.listdir("."):
    if item in to_move:
        os.replace(item, f"{aips_directory}/{item}")
