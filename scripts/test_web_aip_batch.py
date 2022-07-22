"""Purpose: This script generates every known error to use for testing the error handling of web_aip_batch.py.

Usage: python /path/test_web_aip_batch.py
Date values need to be 2022-03-20 and 2022-03-25 to test for predictable results,
so included in this script and not as arguments.

"""

import csv
import datetime
import os
import re
import requests
import shutil
import subprocess
import sys

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
    deletes any empty reports (meaning there was no data of that type for this seed), and redacts login information
    from the seed report. """

    def get_report(filter_type, filter_value, report_type, report_name):
        """Downloads a single metadata report and saves it as a csv in the AIP's metadata folder.
            filter_type and filter_value are used to filter the API call to the right AIP's report
            report_type is the Archive-It name for the report
            report_name is the name for the report saved in the AIP, including the ARCHive metadata code """

        # Checks if the report has already been downloaded and ends the function if so.
        # If there is more than one WARC for a seed, reports may already be in the metadata folder.
        report_path = f'{c.script_output}/aips_{date_end}/{aip_id}/metadata/{report_name}'
        if os.path.exists(report_path):
            return

        # Builds the API call to get the report as a csv.
        # Limit of -1 will return all matches. Default is only the first 100.
        filters = {'limit': -1, filter_type: filter_value, 'format': 'csv'}
        metadata_report = requests.get(f'{c.partner_api}/{report_type}', params=filters, auth=(c.username, c.password))

        # Generates errors by changing the API status of collection, collection scope, and crawl job.
        if "coll" in report_name or report_name.endswith("_crawljob.csv"):
            metadata_report.status_code = 999

        # Saves the metadata report if there were no errors with the API or logs the error.
        if metadata_report.status_code == 200:
            with open(f'{aip_id}/metadata/{report_name}', 'wb') as report_csv:
                report_csv.write(metadata_report.content)
        else:
            if log_data['report_download'] == "n/a":
                log_data['report_download'] = f'{report_type} API error {metadata_report.status_code}'
            else:
                log_data['report_download'] += f'; {report_type} API error {metadata_report.status_code}'

    def redact(metadata_report_path):
        """Replaces the seed report with a redacted version of the file, removing login information if those columns
        are present. Even if the columns are blank, replaces it with REDACTED. Since not all login information is
        meaningful (some is from staff web browsers autofill information while viewing the metadata), knowing if
        there was login information or not is misleading. """

        # Starts a list for storing the redacted rows that will be saved in the new version of the document.
        redacted_rows = []

        # Opens and reads the seed report.
        with open(metadata_report_path) as seed_csv:
            seed_read = csv.reader(seed_csv)

            # Adds the header row to the redacted_rows list without altering it.
            header = next(seed_read)
            redacted_rows.append(header)

            # Gets the index of the password and username columns. Since the report is inconsistent about if these
            # are included at all, also want to catch if columns are in a different order. If the login columns are not
            # present, exits the function and leaves the seed report as it was.
            try:
                password_index = header.index('login_password')
                username_index = header.index('login_username')
            except ValueError:
                if log_data['report_info'] == "n/a":
                    log_data['report_info'] = 'Seed report does not have login columns to redact.'
                else:
                    log_data['report_info'] += '; Seed report does not have login columns to redact.'
                return

            # Puts 'REDACTED' in the password and username columns for each non-header row and adds the updated
            # rows to the redacted_rows list.
            for row in seed_read:
                row[password_index] = 'REDACTED'
                row[username_index] = 'REDACTED'
                redacted_rows.append(row)

        # Opens the seed report again, but in write mode to replace the rows with the redacted rows.
        # Gets each row from the redacted_rows list and saves it to the csv.
        with open(metadata_report_path, 'w', newline='') as report_csv:
            report_write = csv.writer(report_csv)
            for row in redacted_rows:
                report_write.writerow(row)

    # Downloads five of the six metadata reports from Archive-It needed to understand the context of the WARC.
    # These are reports where there is only one report per seed or collection.
    get_report('id', seed_id, 'seed', f'{aip_id}_seed.csv')
    get_report('seed', seed_id, 'scope_rule', f'{aip_id}_seedscope.csv')
    get_report('collection', warc_collection, 'scope_rule', f'{aip_id}_collscope.csv')
    get_report('id', warc_collection, 'collection', f'{aip_id}_coll.csv')
    get_report('id', job_id, 'crawl_job', f'{aip_id}_{job_id}_crawljob.csv')

    # Downloads the crawl definition report for the job this WARC was part of.
    # The crawl definition id is obtained from the crawl job report using the job id.
    # There may be more than one crawl definition report per AIP.
    # Logs an error if there is no crawl job report to get the job id(s) from.
    try:
        with open(f'{aip_id}/metadata/{aip_id}_{job_id}_crawljob.csv', 'r') as crawljob_csv:
            crawljob_data = csv.DictReader(crawljob_csv)
            for job in crawljob_data:
                if job_id == job['id']:
                    crawl_def_id = job['crawl_definition']
                    get_report('id', crawl_def_id, 'crawl_definition', f'{aip_id}_{crawl_def_id}_crawldef.csv')
                    break
    except FileNotFoundError:
        log_data['report_download'] += f'; Crawl Job was not downloaded so cannot get Crawl Definition'

    # If there were no download errors (the log still has the default value), updates the log to show success.
    if log_data['report_download'] == "n/a":
        log_data['report_download'] = "Successfully downloaded all metadata reports."

    # Iterates over each report in the metadata folder to delete empty reports and redact login information from the
    # seed report.
    for report in os.listdir(f'{aip_id}/metadata'):

        # Saves the full file path of the report.
        report_path = f'{c.script_output}/aips_{date_end}/{aip_id}/metadata/{report}'

        # Deletes any empty metadata files (file size of 0) and begins processing the next file. A file is empty if
        # there is no metadata of that type, which is most common for collection and seed scope reports.
        if os.path.getsize(report_path) == 0:
            if log_data['report_info'] == "n/a":
                log_data['report_info'] = f'Deleted empty report {report}'
            else:
                log_data['report_info'] += f'; Deleted empty report {report}'
            os.remove(report_path)
            continue

        # Redacts login password and username from the seed report so we can share the seed report with researchers.
        if report.endswith('_seed.csv'):
            redact(report_path)


def download_warcs(seed, date_end, seed_df, error_type):
    """Downloads a warc file and verifies that fixity is unchanged after downloading.
    Since downloading is slow and no tests require a complete WARC, replaces the download code with making a text file.
    When error_type is fixity, no additional changes are needed since the text file MD5 won't match the WARC."""

    # The path for where the warc will be saved on the local machine (it is long and used twice in this script).
    warc_path = f'{c.script_output}/aips_{date_end}/{aip_id}/objects/{warc_filename}'

    # For testing, makes a status code error. Usually, requests would generate a status code during downloading.
    if error_type == "download":
        status_code = 999
    else:
        status_code = 200

    # If there was an error with the API call, quits the function.
    if not status_code == 200:
        log_data["warc_api"] = f'API error {status_code}'
        return
    else:
        log_data["warc_api"] = "Successfully downloaded WARC."

    # Saves the warc in the objects folder, keeping the original filename.
    # For testing, writing generic text instead of having real data.
    with open(warc_path, 'wb') as warc_file:
        warc_file.write(b"Testing Text")

    # Calculates the md5 for the downloaded WARC, using a regular expression to get the md5 from the md5deep output.
    # If the output is not formatted as expected, quits the function.
    # For testing, changes the md5deep output when needed.
    md5deep_output = subprocess.run(f'"{c.MD5DEEP}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)
    if error_type == "md5deep":
        md5deep_output.stdout = b"error"
    try:
        regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(md5deep_output.stdout))
        downloaded_warc_md5 = regex_md5.group(1)
    except AttributeError:
        log_data["warc_fixity"] = f"Fixity cannot be extracted from md5deep output: {md5deep_output.stdout}"
        return

    # Compares the md5 of the download warc to what Archive-It has for the warc (warc_md5). If the md5 has changed,
    # deletes the WARC so the check for AIP completeness will catch that there was a problem.
    if not warc_md5 == downloaded_warc_md5:
        os.remove(warc_path)
        log_data["warc_fixity"] = f"Fixity changed and WARC deleted. {warc_md5} before, {downloaded_warc_md5} after"


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
    print(f"Processing seed {current_seed} of {total_seeds}.")

    # Makes output directories
    os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
    os.makedirs(os.path.join(seed.AIP_ID, "objects"))

    # Makes AIP instance. In production, this isn't done after downloading.
    # For testing, do it here so don't have to repeat the code for every seed.
    aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, version=1, to_zip=True)

    # ERROR 1: API error downloading metadata reports.
    if current_seed == 1:
        download_metadata(seed, seed_df, error_type="download")

    # ERROR 2: No crawl_job so can't download crawl_definition.
    if current_seed == 2:
        download_metadata(seed, seed_df, error_type="crawldef")

    # ERROR 3: API error downloading WARC metadata.
    if current_seed == 3:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="metadata")

    # ERROR 4: API error downloading WARC.
    if current_seed == 4:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="download")

    # ERROR 5: Cannot extract fixity from MD5Deep output.
    if current_seed == 5:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="md5deep")

    # ERROR 6: WARC fixity after download doesn't match Archive-It record.
    if current_seed == 6:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="fixity")

    # ERROR 7: Error unzipping WARC
    if current_seed == 7:
        web.download_metadata(seed, seed_df)
        download_warcs(seed, date_end, seed_df, error_type="unzip")

    # ERROR 8: No objects folder
    if current_seed == 8:
        web.download_metadata(seed, seed_df)
        web.download_warcs(seed, date_end, seed_df)
        web.check_directory(aip)

    # ERROR 9: Objects folder is empty
    if current_seed == 9:
        web.download_metadata(seed, seed_df)
        web.download_warcs(seed, date_end, seed_df)
        web.check_directory(aip)

    # ERROR 10: No metadata folder
    if current_seed == 10:
        web.download_metadata(seed, seed_df)
        web.download_warcs(seed, date_end, seed_df)
        web.check_directory(aip)

    # Error 11: Metadata folder is empty
    if current_seed == 11:
        web.download_metadata(seed, seed_df)
        web.download_warcs(seed, date_end, seed_df)
        web.check_directory(aip)
