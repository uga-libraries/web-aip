"""Purpose: This script generates every known error to use for testing the error handling of web_aip_batch.py.

Usage: python /path/test_web_function_errors.py
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
        # there was login information or not is misleading. """
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

# Makes the log for the AIP part of the script (used by check_directory).
a.log("header")

# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT MAKES A DIFFERENT ERROR EVERY TIME IT STARTS A NEW SEED.
# FOR ERRORS GENERATING WITHIN FUNCTIONS, IT USES A DIFFERENT VERSION OF THE FUNCTION.
# PRINTS AN ERROR TO THE TERMINAL IF THE ERROR IS NOT CAUGHT AND STARTS THE LOOP WITH THE NEXT WARC.
# ----------------------------------------------------------------------------------------------------------------

# There will be 14 seeds total.
# Errors are listed in script order. Seeds are processed out of order to get desired input for specific tests.
# Seeds are only processed through check_directory. After that, general aip functions take over.
for seed in seed_df.itertuples():

    # Displays the script progress.
    print(f"Processing seed {seed.Seed_ID} (AIP {seed.AIP_ID}).")

    # ERROR 1: API error downloading metadata reports.
    if seed.Seed_ID == "2529676":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        download_metadata(seed, seed_df, error_type="download")
        download_warcs(seed, date_end, seed_df, error_type="none")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 2: API error downloading crawl_job so can't download crawl_definition.
    if seed.Seed_ID == "2529652":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        download_metadata(seed, seed_df, error_type="crawl_job")
        download_warcs(seed, date_end, seed_df, error_type="none")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 3: No login columns in seed to redact.
    if seed.Seed_ID == "2529631":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        download_metadata(seed, seed_df, error_type="redact")
        download_warcs(seed, date_end, seed_df, error_type="none")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 4: API error downloading WARC metadata. AIP has 1 WARC.
    if seed.Seed_ID == "2529681":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="metadata")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 5: API error downloading WARC. AIP has 1 WARC.
    if seed.Seed_ID == "2529668":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="download")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 6: API error downloading first WARC. AIP has 2 WARCs.
    if seed.Seed_ID == "2529634":
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
    if seed.Seed_ID == "2454506":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="everything")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        web.check_directory(aip)
        a.log(aip.log)

    # The rest of the errors are for empty or missing objects and metadata folders.
    # Still makes the folders and downloads everything so the seeds.csv has expected log values.
    # Uses download_warcs with error_type of none to speed up the process by not really downloading.

    # ERROR 11: No objects folder.
    if seed.Seed_ID == "2184360":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="none")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        shutil.rmtree(f"{aip.directory}/{aip.id}/objects")
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 12: Objects folder is empty.
    if seed.Seed_ID == "2529629":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="none")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        shutil.rmtree(f"{aip.directory}/{aip.id}/objects")
        os.makedirs(f"{aip.directory}/{aip.id}/objects")
        web.check_directory(aip)
        a.log(aip.log)

    # ERROR 13: No metadata folder.
    if seed.Seed_ID == "2529642":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="none")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        shutil.rmtree(f"{aip.directory}/{aip.id}/metadata")
        web.check_directory(aip)
        a.log(aip.log)

    # Error 14: Metadata folder is empty.
    if seed.Seed_ID == "2739136":
        os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
        os.makedirs(os.path.join(seed.AIP_ID, "objects"))
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="none")
        aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, 1, True)
        shutil.rmtree(f"{aip.directory}/{aip.id}/metadata")
        os.makedirs(f"{aip.directory}/{aip.id}/metadata")
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
print("\nColumn types in seed_df in case those don't match.")
print(seed_df.dtypes)

# CHECKS VALUES IN: seed_df

# Makes a dataframe with the expected values for the seed dataframe.
data = {"Seed_ID": ["2529634", "2529660", "2529629", "2529642", "2529627", "2529652", "2529631",
                    "2529668", "2529681", "2529676", "2454507", "2454506", "2739136", "2184360"],
        "AIP_ID": ["magil-ggp-2529634-2022-03", "magil-ggp-2529660-2022-03", "magil-ggp-2529629-2022-03",
                   "magil-ggp-2529642-2022-03", "magil-ggp-2529627-2022-03", "magil-ggp-2529652-2022-03",
                   "magil-ggp-2529631-2022-03", "magil-ggp-2529668-2022-03", "magil-ggp-2529681-2022-03",
                   "magil-ggp-2529676-2022-03", "rbrl-246-web-202203-0001", "rbrl-246-web-202203-0002",
                   "rbrl-499-web-202203-0001", "harg-0000-web-202203-0011"],
        "Title": ["Georgia Office of Attorney General Consumer Protection Division",
                  "Georgia Governor's Office of Disability Services Ombudsman",
                  "Georgia Board of Pharmacy",
                  "Georgia Department of Community Supervision",
                  "Georgia Aviation Authority",
                  "Georgia Drugs and Narcotics Agency",
                  "Georgia Commission on Equal Opportunity",
                  "Georgia Office of the Child Advocate",
                  "Georgia State Properties Commission",
                  "Georgia State Board of Accountancy",
                  "SenatorIsakson - YouTube",
                  "SenatorIsakson - YouTube [channel page]",
                  "Savannah River Site",
                  "UGA Today"],
        "Department": ["magil", "magil", "magil", "magil", "magil", "magil", "magil",
                       "magil", "magil", "magil", "russell", "russell", "russell", "hargrett"],
        "UGA_Collection": ["0000", "0000", "0000", "0000", "0000", "0000", "0000",
                           "0000", "0000", "0000", "rbrl-246", "rbrl-246", "rbrl-499", "0000"],
        "AIT_Collection": ["15678", "15678", "15678", "15678", "15678", "15678", "15678",
                           "15678", "15678", "15678", "12265", "12265", "12263", "12912"],
        "Job_ID": ["1576505", "1573937", "1573937", "1573937", "1573937", "1576493", "1576492",
                   "1577241", "1577187", "1577186", "1542317", "1542317", "1539793", "1497166"],
        "Size_GB": [1.2, 0.1, 0.93, 0.94, 0.06, 0.19, 0.23,
                    0.39, 0.33, 0.19, 0.0, 8.04, 1.35999999999999, 34.7399999999999],
        "WARCs": ["2", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "9", "2", "35"],
        "WARC_Filenames": ["ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220318151112447-00000-h3.warc.gz,ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220320234830550-00001-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1573937-SEED2529660-20220314173634919-00000-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1573937-SEED2529629-20220314173634874-00000-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1573937-SEED2529642-20220314173635152-00000-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1573937-SEED2529627-20220314173635069-00000-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1576493-SEED2529652-20220318145745554-00000-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1576492-SEED2529631-20220318145704126-00000-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1577241-SEED2529668-20220320182657667-00000-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1577187-SEED2529681-20220320144922213-00000-h3.warc.gz",
                           "ARCHIVEIT-15678-TEST-JOB1577186-SEED2529676-20220320144814871-00000-h3.warc.gz",
                           "ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz",
                           "ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220114214053021-00000-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220115132006340-00001-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220115233148240-00002-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116063911435-00003-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116131550492-00004-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117010033395-00005-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117200343134-00008-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117064534470-00006-8dgxemts.warc.gz,ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117130422336-00007-8dgxemts.warc.gz",
                           "ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220110194710516-00000-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220111130848517-00001-h3.warc.gz",
                           "ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017105304659-00034-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211018211851376-00035-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103754760-00033-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102717487-00031-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103234346-00032-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101611745-00029-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102203466-00030-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101049993-00028-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017100311992-00027-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094753960-00025-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017095516699-00026-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094020636-00024-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017093516000-00023-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017092934974-00022-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091918474-00021-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091205263-00020-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090702655-00019-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090255676-00018-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085824384-00017-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085024644-00016-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017081603923-00015-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017074632435-00014-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073724465-00013-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073125245-00012-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017071639642-00011-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070834820-00010-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070215160-00009-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017065508860-00008-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017064416641-00007-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063723971-00006-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063009068-00005-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017061815778-00004-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017060858471-00003-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017020639822-00002-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211016171927511-00001-h3.warc.gz"],
        "Seed_Metadata_Errors": ["Successfully calculated seed metadata", "Successfully calculated seed metadata",
                                 "Successfully calculated seed metadata", "Successfully calculated seed metadata",
                                 "Successfully calculated seed metadata", "Successfully calculated seed metadata",
                                 "Successfully calculated seed metadata", "Successfully calculated seed metadata",
                                 "Successfully calculated seed metadata", "Successfully calculated seed metadata",
                                 "Successfully calculated seed metadata", "Successfully calculated seed metadata",
                                 "Successfully calculated seed metadata", "Successfully calculated seed metadata"],
        "Metadata_Report_Errors": ["Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports",
                                   "magil-ggp-2529652-2022-03_1576493_crawljob.csv API error 999; Crawl job was not downloaded so can't get crawl definition id",
                                   "Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports",
                                   "magil-ggp-2529676-2022-03_seed.csv API error 999; magil-ggp-2529676-2022-03_seedscope.csv API error 999; magil-ggp-2529676-2022-03_collscope.csv API error 999; magil-ggp-2529676-2022-03_coll.csv API error 999; 2529676_31104537579_crawldef.csv API error 999",
                                   "Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports",
                                   "Successfully downloaded all metadata reports"],
        "Metadata_Report_Info": ["Empty report magil-ggp-2529634-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529634-2022-03_collscope.csv not saved",
                                 "Empty report magil-ggp-2529660-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529660-2022-03_collscope.csv not saved",
                                 "Empty report magil-ggp-2529629-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529629-2022-03_collscope.csv not saved",
                                 "Empty report magil-ggp-2529642-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529642-2022-03_collscope.csv not saved",
                                 "Empty report magil-ggp-2529627-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529627-2022-03_collscope.csv not saved",
                                 "Empty report magil-ggp-2529652-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529652-2022-03_collscope.csv not saved",
                                 "Seed report does not have login columns to redact; Empty report magil-ggp-2529631-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529631-2022-03_collscope.csv not saved",
                                 "Empty report magil-ggp-2529668-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529668-2022-03_collscope.csv not saved",
                                 "Empty report magil-ggp-2529681-2022-03_seedscope.csv not saved; Empty report magil-ggp-2529681-2022-03_collscope.csv not saved",
                                 "No additional information",
                                 "No additional information",
                                 "No additional information",
                                 "Empty report rbrl-499-web-202203-0001_seedscope.csv not saved",
                                 "No additional information"],
        "WARC_API_Errors": ["API error 999: can't download ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220318151112447-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220320234830550-00001-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1573937-SEED2529660-20220314173634919-00000-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1573937-SEED2529629-20220314173634874-00000-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1573937-SEED2529642-20220314173635152-00000-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1573937-SEED2529627-20220314173635069-00000-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1576493-SEED2529652-20220318145745554-00000-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1576492-SEED2529631-20220318145704126-00000-h3.warc.gz",
                            "API error 999: can't download ARCHIVEIT-15678-TEST-JOB1577241-SEED2529668-20220320182657667-00000-h3.warc.gz",
                            "API error 999: can't get info about ARCHIVEIT-15678-TEST-JOB1577187-SEED2529681-20220320144922213-00000-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1577186-SEED2529676-20220320144814871-00000-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz",
                            "API error 999: can't get info about ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220114214053021-00000-8dgxemts.warc.gz; API error 999: can't download ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220115132006340-00001-8dgxemts.warc.gz; Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220115233148240-00002-8dgxemts.warc.gz; Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116063911435-00003-8dgxemts.warc.gz; Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116131550492-00004-8dgxemts.warc.gz; Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117010033395-00005-8dgxemts.warc.gz; Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117200343134-00008-8dgxemts.warc.gz; Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117064534470-00006-8dgxemts.warc.gz; Successfully downloaded ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117130422336-00007-8dgxemts.warc.gz",
                            "Successfully downloaded ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220110194710516-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220111130848517-00001-h3.warc.gz",
                            "Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017105304659-00034-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211018211851376-00035-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103754760-00033-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102717487-00031-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103234346-00032-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101611745-00029-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102203466-00030-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101049993-00028-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017100311992-00027-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094753960-00025-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017095516699-00026-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094020636-00024-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017093516000-00023-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017092934974-00022-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091918474-00021-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091205263-00020-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090702655-00019-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090255676-00018-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085824384-00017-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085024644-00016-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017081603923-00015-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017074632435-00014-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073724465-00013-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073125245-00012-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017071639642-00011-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070834820-00010-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070215160-00009-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017065508860-00008-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017064416641-00007-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063723971-00006-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063009068-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017061815778-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017060858471-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017020639822-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211016171927511-00001-h3.warc.gz"],
        "WARC_Fixity_Errors": ["Successfully verified ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220320234830550-00001-h3.warc.gz fixity on",
                               "Fixity for ARCHIVEIT-15678-TEST-JOB1573937-SEED2529660-20220314173634919-00000-h3.warc.gz cannot be extracted from md5deep output: b'@Something#Unexpected'",
                               "Successfully verified ARCHIVEIT-15678-TEST-JOB1573937-SEED2529629-20220314173634874-00000-h3.warc.gz fixity on",
                               "Successfully verified ARCHIVEIT-15678-TEST-JOB1573937-SEED2529642-20220314173635152-00000-h3.warc.gz fixity on",
                               "Fixity for ARCHIVEIT-15678-TEST-JOB1573937-SEED2529627-20220314173635069-00000-h3.warc.gz changed and it was deleted: 5074238e67149f99f5e8b3700196db97 before, abc123abc123abc123abc123 after",
                               "Successfully verified ARCHIVEIT-15678-TEST-JOB1576493-SEED2529652-20220318145745554-00000-h3.warc.gz fixity on",
                               "Successfully verified ARCHIVEIT-15678-TEST-JOB1576492-SEED2529631-20220318145704126-00000-h3.warc.gz fixity on",
                               "BLANK",
                               "BLANK",
                               "Successfully verified ARCHIVEIT-15678-TEST-JOB1577186-SEED2529676-20220320144814871-00000-h3.warc.gz fixity on 2022-07-25 16:13:18.742668",
                               "Successfully verified ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz fixity on 2022-07-25 16:13:39.473768",
                               "Fixity for ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220115233148240-00002-8dgxemts.warc.gz cannot be extracted from md5deep output: b'@Something#Unexpected'; Successfully verified ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116063911435-00003-8dgxemts.warc.gz fixity on 2022-07-25 16:13:58.072782; Successfully verified ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116131550492-00004-8dgxemts.warc.gz fixity on 2022-07-25 16:14:13.495123; Successfully verified ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117010033395-00005-8dgxemts.warc.gz fixity on 2022-07-25 16:14:28.962814; Fixity for ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117200343134-00008-8dgxemts.warc.gz changed and it was deleted: 0bc2fd86f2e1a8ea6e0f87701f60ef79 before, abc123abc123abc123abc123 after; Successfully verified ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117064534470-00006-8dgxemts.warc.gz fixity on; Successfully verified ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117130422336-00007-8dgxemts.warc.gz fixity on",
                               "Successfully verified ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220110194710516-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220111130848517-00001-h3.warc.gz fixity on",
                               "Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017105304659-00034-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211018211851376-00035-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103754760-00033-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102717487-00031-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103234346-00032-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101611745-00029-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102203466-00030-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101049993-00028-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017100311992-00027-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094753960-00025-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017095516699-00026-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094020636-00024-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017093516000-00023-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017092934974-00022-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091918474-00021-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091205263-00020-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090702655-00019-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090255676-00018-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085824384-00017-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085024644-00016-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017081603923-00015-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017074632435-00014-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073724465-00013-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073125245-00012-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017071639642-00011-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070834820-00010-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070215160-00009-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017065508860-00008-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017064416641-00007-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063723971-00006-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063009068-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017061815778-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017060858471-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017020639822-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211016171927511-00001-h3.warc.gz fixity on"],
        "WARC_Unzip_Errors": ["Successfully unzipped ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220320234830550-00001-h3.warc.gz",
                              "BLANK",
                              "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1573937-SEED2529629-20220314173634874-00000-h3.warc.gz",
                              "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1573937-SEED2529642-20220314173635152-00000-h3.warc.gz",
                              "BLANK",
                              "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1576493-SEED2529652-20220318145745554-00000-h3.warc.gz",
                              "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1576492-SEED2529631-20220318145704126-00000-h3.warc.gz",
                              "BLANK",
                              "BLANK",
                              "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1577186-SEED2529676-20220320144814871-00000-h3.warc.gz",
                              "Error unzipping ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz: Error message stand-in.",
                              "Error unzipping ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116063911435-00003-8dgxemts.warc.gz: Error message stand-in.; Successfully unzipped ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116131550492-00004-8dgxemts.warc.gz; Successfully unzipped ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117010033395-00005-8dgxemts.warc.gz; Successfully unzipped ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117064534470-00006-8dgxemts.warc.gz; Successfully unzipped ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117130422336-00007-8dgxemts.warc.gz",
                              "Successfully unzipped ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220110194710516-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220111130848517-00001-h3.warc.gz",
                              "Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017105304659-00034-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211018211851376-00035-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103754760-00033-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102717487-00031-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017103234346-00032-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101611745-00029-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017102203466-00030-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017101049993-00028-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017100311992-00027-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094753960-00025-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017095516699-00026-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017094020636-00024-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017093516000-00023-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017092934974-00022-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091918474-00021-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017091205263-00020-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090702655-00019-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017090255676-00018-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085824384-00017-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017085024644-00016-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017081603923-00015-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017074632435-00014-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073724465-00013-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017073125245-00012-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017071639642-00011-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070834820-00010-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017070215160-00009-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017065508860-00008-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017064416641-00007-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063723971-00006-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017063009068-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017061815778-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017060858471-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211017020639822-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1497166-SEED2184360-20211016171927511-00001-h3.warc.gz"]}
expected_seed_df = pd.DataFrame(data)

print("\nColumn types in expected_seed_df")
print(expected_seed_df.dtypes)

# Updates seeds_df created by the script to make comparison with consistent values possible.
# Removes time stamps from WARC fixity column and fills NaN with text.
seed_df["WARC_Fixity_Errors"] = seed_df["WARC_Fixity_Errors"].str.replace(" \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}", "")
seed_df.fillna("BLANK", inplace=True)

# Compares the actual and expected values of seed_df
compare_seed_df = seed_df.merge(expected_seed_df, indicator=True, how="outer")

# CHECKS VALUES IN: aip_log.csv

# Makes dataframe with expected values for aip_log.csv.
data = {"AIP ID": ["magil-ggp-2529634-2022-03", "magil-ggp-2529629-2022-03", "magil-ggp-2529629-2022-03",
                   "magil-ggp-2529642-2022-03", "magil-ggp-2529642-2022-03", "magil-ggp-2529627-2022-03",
                   "magil-ggp-2529627-2022-03", "magil-ggp-2529652-2022-03", "magil-ggp-2529631-2022-03",
                   "magil-ggp-2529668-2022-03", "magil-ggp-2529668-2022-03", "magil-ggp-2529681-2022-03",
                   "magil-ggp-2529681-2022-03", "magil-ggp-2529676-2022-03", "rbrl-246-web-202203-0001",
                   "rbrl-246-web-202203-0002", "rbrl-499-web-202203-0001", "rbrl-499-web-202203-0001",
                   "harg-0000-web-202203-0011", "harg-0000-web-202203-0011"],
        "Files Deleted": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                          "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
        "Objects Folder": ["Successfully created objects folder", "Objects folder is empty.",
                           "Objects folder is empty.", "Successfully created objects folder",
                           "Successfully created objects folder", "Objects folder is empty.",
                           "Objects folder is empty.", "Successfully created objects folder",
                           "Successfully created objects folder", "Objects folder is empty.",
                           "Objects folder is empty.", "Objects folder is empty.",
                           "Objects folder is empty.", "Successfully created objects folder",
                           "Successfully created objects folder", "Successfully created objects folder",
                           "Successfully created objects folder", "Successfully created objects folder",
                           "Objects folder is missing.", "Objects folder is missing."],
        "Metadata Folder": ["Successfully created metadata folder", "BLANK",
                            "BLANK", "Metadata folder is missing.",
                            "Metadata folder is missing.", "BLANK",
                            "BLANK", "Successfully created metadata folder",
                            "Successfully created metadata folder", "BLANK",
                            "BLANK", "BLANK",
                            "BLANK", "Successfully created metadata folder",
                            "Successfully created metadata folder", "Successfully created metadata folder",
                            "Metadata folder is empty.", "Metadata folder is empty.",
                            "BLANK", "BLANK"],
        "FITS Tool Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                             "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
        "FITS Combination Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                                    "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
        "Preservation.xml Made": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                                  "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
        "Preservation.xml Valid": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                                   "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
        "Bag Valid": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                      "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
        "Package Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                           "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
        "Manifest Errors": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                            "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
        "Processing Complete": ["BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK",
                                "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]}
expected_aip_log_df = pd.DataFrame(data)

# Reads aip_log.csv, made by the script, into a dataframe.
# Removes Time Started in aip_log.csv and fills NaN with text so the data is can be compared.
aip_log_df = pd.read_csv(os.path.join(c.script_output, aips_directory, "aip_log.csv"))
aip_log_df.drop(["Time Started"], axis=1, inplace=True)
aip_log_df.fillna("BLANK", inplace=True)
print("\nColumn types in aip_log_df in case those don't match.")
print(aip_log_df.dtypes)

# Compares the actual and expected values of aip_log.csv
compare_aip_df = aip_log_df.merge(expected_aip_log_df, indicator=True, how="outer")

# CHECKS VALUES IN: directory structure.

# Creates a list with the expected relative paths.
expected = [r"aips_2022-03-24\aip_log.csv",
            r"aips_2022-03-24\seeds.csv",
            r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\2184360_31104307871_crawldef.csv",
            r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_1497166_crawljob.csv",
            r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_coll.csv",
            r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_collscope.csv",
            r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_seed.csv",
            r"aips_2022-03-24\errors\incomplete_directory\harg-0000-web-202203-0011\metadata\harg-0000-web-202203-0011_seedscope.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529627-2022-03\metadata\2529627_31104535848_crawldef.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529627-2022-03\metadata\magil-ggp-2529627-2022-03_1573937_crawljob.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529627-2022-03\metadata\magil-ggp-2529627-2022-03_coll.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529627-2022-03\metadata\magil-ggp-2529627-2022-03_seed.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529629-2022-03\metadata\2529629_31104535848_crawldef.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529629-2022-03\metadata\magil-ggp-2529629-2022-03_1573937_crawljob.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529629-2022-03\metadata\magil-ggp-2529629-2022-03_coll.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529629-2022-03\metadata\magil-ggp-2529629-2022-03_seed.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529642-2022-03\objects\ARCHIVEIT-15678-TEST-JOB1573937-SEED2529642-20220314173635152-00000-h3.warc",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529668-2022-03\metadata\2529668_31104537585_crawldef.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529668-2022-03\metadata\magil-ggp-2529668-2022-03_1577241_crawljob.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529668-2022-03\metadata\magil-ggp-2529668-2022-03_coll.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529668-2022-03\metadata\magil-ggp-2529668-2022-03_seed.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529681-2022-03\metadata\2529681_31104537580_crawldef.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529681-2022-03\metadata\magil-ggp-2529681-2022-03_1577187_crawljob.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529681-2022-03\metadata\magil-ggp-2529681-2022-03_coll.csv",
            r"aips_2022-03-24\errors\incomplete_directory\magil-ggp-2529681-2022-03\metadata\magil-ggp-2529681-2022-03_seed.csv",
            r"aips_2022-03-24\errors\incomplete_directory\rbrl-499-web-202203-0001\objects\ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220110194710516-00000-h3.warc",
            r"aips_2022-03-24\errors\incomplete_directory\rbrl-499-web-202203-0001\objects\ARCHIVEIT-12263-TEST-JOB1539793-SEED2739136-20220111130848517-00001-h3.warc",
            r"aips_2022-03-24\magil-ggp-2529631-2022-03\metadata\2529631_31104537364_crawldef.csv",
            r"aips_2022-03-24\magil-ggp-2529631-2022-03\metadata\magil-ggp-2529631-2022-03_1576492_crawljob.csv",
            r"aips_2022-03-24\magil-ggp-2529631-2022-03\metadata\magil-ggp-2529631-2022-03_coll.csv",
            r"aips_2022-03-24\magil-ggp-2529631-2022-03\metadata\magil-ggp-2529631-2022-03_seed.csv",
            r"aips_2022-03-24\magil-ggp-2529631-2022-03\objects\ARCHIVEIT-15678-TEST-JOB1576492-SEED2529631-20220318145704126-00000-h3.warc",
            r"aips_2022-03-24\magil-ggp-2529634-2022-03\metadata\2529634_31104537372_crawldef.csv",
            r"aips_2022-03-24\magil-ggp-2529634-2022-03\metadata\magil-ggp-2529634-2022-03_1576505_crawljob.csv",
            r"aips_2022-03-24\magil-ggp-2529634-2022-03\metadata\magil-ggp-2529634-2022-03_coll.csv",
            r"aips_2022-03-24\magil-ggp-2529634-2022-03\metadata\magil-ggp-2529634-2022-03_seed.csv",
            r"aips_2022-03-24\magil-ggp-2529634-2022-03\objects\ARCHIVEIT-15678-TEST-JOB1576505-SEED2529634-20220320234830550-00001-h3.warc",
            r"aips_2022-03-24\magil-ggp-2529652-2022-03\metadata\magil-ggp-2529652-2022-03_coll.csv",
            r"aips_2022-03-24\magil-ggp-2529652-2022-03\metadata\magil-ggp-2529652-2022-03_seed.csv",
            r"aips_2022-03-24\magil-ggp-2529652-2022-03\objects\ARCHIVEIT-15678-TEST-JOB1576493-SEED2529652-20220318145745554-00000-h3.warc",
            r"aips_2022-03-24\magil-ggp-2529660-2022-03\metadata\2529660_31104535848_crawldef.csv",
            r"aips_2022-03-24\magil-ggp-2529660-2022-03\metadata\magil-ggp-2529660-2022-03_1573937_crawljob.csv",
            r"aips_2022-03-24\magil-ggp-2529660-2022-03\metadata\magil-ggp-2529660-2022-03_coll.csv",
            r"aips_2022-03-24\magil-ggp-2529660-2022-03\metadata\magil-ggp-2529660-2022-03_seed.csv",
            r"aips_2022-03-24\magil-ggp-2529660-2022-03\objects\ARCHIVEIT-15678-TEST-JOB1573937-SEED2529660-20220314173634919-00000-h3.warc.gz",
            r"aips_2022-03-24\magil-ggp-2529676-2022-03\metadata\magil-ggp-2529676-2022-03_1577186_crawljob.csv",
            r"aips_2022-03-24\magil-ggp-2529676-2022-03\objects\ARCHIVEIT-15678-TEST-JOB1577186-SEED2529676-20220320144814871-00000-h3.warc",
            r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\2454507_31104519079_crawldef.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_1542317_crawljob.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_coll.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_collscope.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_seed.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0001\metadata\rbrl-246-web-202203-0001_seedscope.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0001\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz",
            r"aips_2022-03-24\rbrl-246-web-202203-0001\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454507-20220114214056178-00000-hlvkp53o.warc.gz.open",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\metadata\2454506_31104519079_crawldef.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\metadata\rbrl-246-web-202203-0002_1542317_crawljob.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\metadata\rbrl-246-web-202203-0002_coll.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\metadata\rbrl-246-web-202203-0002_collscope.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\metadata\rbrl-246-web-202203-0002_seed.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\metadata\rbrl-246-web-202203-0002_seedscope.csv",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220115233148240-00002-8dgxemts.warc.gz",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116063911435-00003-8dgxemts.warc.gz",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220116131550492-00004-8dgxemts.warc",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117010033395-00005-8dgxemts.warc",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117064534470-00006-8dgxemts.warc",
            r"aips_2022-03-24\rbrl-246-web-202203-0002\objects\ARCHIVEIT-12265-TEST-JOB1542317-0-SEED2454506-20220117130422336-00007-8dgxemts.warc"]

# Creates a list with relative paths starting at the AIPs directory so paths are predictable.
directory_list = []
for root, dirs, files in os.walk(f"aips_{date_end}"):
    for file in files:
        directory_list.append(os.path.join(root, file))

# Makes dataframes of the expected and actual directory lists and compares them.
directory_df = pd.DataFrame({"File_Path": directory_list})
expected_directory_df = pd.DataFrame({"File_Path": expected})
compare_dir_df = directory_df.merge(expected_directory_df, indicator=True, how="outer")

# Saves the results to tabs on a spreadsheet (error_test_results.xlsx) in the script output folder.
with pd.ExcelWriter(os.path.join(c.script_output, "error_test_results.xlsx")) as results:
    compare_seed_df.to_excel(results, sheet_name="Seeds_CSV", index=False)
    compare_aip_df.to_excel(results, sheet_name="AIP_CSV", index=False)
    compare_dir_df.to_excel(results, sheet_name="Directory", index=False)

# Removes the rows that match from the dataframes and prints if there were mismatches or not.
compare_seed_df = compare_seed_df[compare_seed_df["_merge"] != "both"]
if len(compare_seed_df) == 0:
    print("Tests passed for seeds.csv")
else:
    print("Possible errors with seeds.csv: check error_test_results.xlsx")
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
