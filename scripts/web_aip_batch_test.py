"""Purpose: This script generates every known error to use for testing the error handling of web_aip_batch.py.

Usage: python /path/web_aip_batch_test.py date_start date_end
To get 16 WARCs, for date_start use 2022-03-20 and for date_end use 2022-03-25

"""

import csv
import os
import re
import requests
import subprocess
import sys

# Import functions and constant variables from other UGA scripts.
import aip_functions as aip
import configuration as c
import web_functions as web


# ----------------------------------------------------------------------------------------------------------------
# ALTERNATIVE VERSIONS OF FUNCTIONS THAT GENERATE ERRORS.
# IF THERE IS MORE THAN ONE ERROR NEEDED, ADDS AN ARGUMENT FOR ERROR_TYPE TO SPECIFY.
# ----------------------------------------------------------------------------------------------------------------
def download_metadata(aip_id, warc_collection, job_id, seed_id, date_end, log_data):
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

        # Generates errors by changing the API status of collection and collection scope.
        if "coll" in report_name:
            metadata_report.status_code = 999

        # Saves the metadata report if there were no errors with the API or logs the error.
        if metadata_report.status_code == 200:
            with open(f'{aip_id}/metadata/{report_name}', 'wb') as report_csv:
                report_csv.write(metadata_report.content)
        else:
            if log_data['report_download'] == "n/a":
                log_data['report_download'] = f'{report_type} API error {metadata_report.status_code}'
            else:
                log_data['report_download'] = log_data['report_download'] + f'; {report_type} API error {metadata_report.status_code}'

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
                    log_data['report_info'] = log_data['report_info'] + '; Seed report does not have login columns to redact.'
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
    with open(f'{aip_id}/metadata/{aip_id}_{job_id}_crawljob.csv', 'r') as crawljob_csv:
        crawljob_data = csv.DictReader(crawljob_csv)
        for job in crawljob_data:
            if job_id == job['id']:
                crawl_def_id = job['crawl_definition']
                get_report('id', crawl_def_id, 'crawl_definition', f'{aip_id}_{crawl_def_id}_crawldef.csv')
                break

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
                log_data['report_info'] = log_data['report_info'] + f'; Deleted empty report {report}'
            os.remove(report_path)
            continue

        # Redacts login password and username from the seed report so we can share the seed report with researchers.
        if report.endswith('_seed.csv'):
            redact(report_path)


def download_warc(aip_id, warc_filename, warc_url, warc_md5, date_end, log_data, error_type):
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
# ----------------------------------------------------------------------------------------------------------------

# Gets the start and end dates from script arguments and verify their formatting.
try:
    date_start, date_end = sys.argv[1:]
except IndexError:
    print("Exiting script: must provide exactly two arguments, the start and end date of the quarter.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_start):
    print(f"Exiting script: start date '{date_start}' must be formatted YYYY-MM-DD.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_end):
    print(f"Exiting script: end date '{date_end}' must be formatted YYYY-MM-DD.")
    exit()

# Tests the paths in the configuration file.
valid_errors = aip.check_paths()
if not valid_errors == "no errors":
    print('The following path(s) in the configuration file are not correct:')
    for error in valid_errors:
        print(error)
    print('Correct the configuration file and run the script again.')
    sys.exit()

# Makes a folder for AIPs within the script_output folder and makes it the current directory.
aips_directory = f'aips_{date_end}'
if not os.path.exists(f'{c.script_output}/{aips_directory}'):
    os.makedirs(f'{c.script_output}/{aips_directory}')
os.chdir(f'{c.script_output}/{aips_directory}')

# Downloads information from APIs, starts variables for tracking information, and starts the warc log.
warc_metadata = web.warc_data(date_start, date_end)
seed_metadata = web.seed_data(warc_metadata, date_end)
current_warc = 0
total_warcs = warc_metadata['count']
seed_to_aip = {}
aip_to_title = {}
web.warc_log("header")

# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT MAKES A DIFFERENT ERROR EVERY TIME IT STARTS A NEW WARC.
# FOR ERRORS GENERATING WITHIN FUNCTIONS, IT USES A DIFFERENT VERSION OF THE FUNCTION.
# PRINTS AN ERROR TO THE TERMINAL IF THE ERROR IS NOT CAUGHT AND STARTS THE LOOP WITH THE NEXT WARC.
# ----------------------------------------------------------------------------------------------------------------

for warc in warc_metadata['files']:

    # Starts a dictionary of information for the log.
    log_data = {"filename": "TBD", "warc_json": "n/a", "seed_id": "n/a", "job_id": "n/a",
                "seed_metadata": "n/a", "report_download": "n/a", "report_info": "n/a", "warc_api": "n/a",
                "warc_fixity": "n/a", "complete": "Errors during WARC processing."}

    # Updates the current WARC number and displays the script progress.
    current_warc += 1
    print(f"Processing WARC {current_warc} of {total_warcs}.")

    # ERROR 1: Cannot find expected values in WARC JSON (KeyError).
    if current_warc == 1:

        # Generate the error by removing two fields needed for variables.
        warc["checksums"].pop("md5")
        warc.pop("collection")

        # Saves relevant information about the WARC in variables for future use.
        try:
            warc_filename = warc['filename']
            warc_url = warc['locations'][0]
            warc_md5 = warc['checksums']['md5']
            warc_collection = warc['collection']
        except KeyError:
            log_data["warc_json"] = f"Could not find at least one expected value in JSON: {warc}"
            web.warc_log(log_data)
            continue
        except IndexError:
            log_data["warc_json"] = f"Could not find URL in JSON: {warc}"
            web.warc_log(log_data)
            continue

        # Should catch the error in the previous step and this should not run.
        print("Test did not catch Error 1 correctly.")
        continue

    # ERROR 2: Cannot find expected values in WARC JSON (IndexError).
    if current_warc == 2:

        # Saves relevant information about the WARC in variables for future use.
        # Generate error by using the wrong index number for warc_url
        try:
            warc_filename = warc['filename']
            warc_url = warc['locations'][5]
            warc_md5 = warc['checksums']['md5']
            warc_collection = warc['collection']
        except KeyError:
            log_data["warc_json"] = f"Could not find at least one expected value in JSON: {warc}"
            web.warc_log(log_data)
            continue
        except IndexError:
            log_data["warc_json"] = f"Could not find URL in JSON: {warc}"
            web.warc_log(log_data)
            continue

        # Should catch the error in the previous step and this should not run.
        print("Test did not catch Error 2 correctly.")
        continue

    # ERROR 3: Cannot extract seed_id from the WARC filename.
    if current_warc == 3:

        # Previous step. Generate the error by assigning the wrong value to warc_filename.
        warc_filename = "warc-seed-error.warc.gz"
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."

        # Calculates seed id, which is a portion of the WARC filename between "-SEED" and "-".
        try:
            regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
            seed_id = regex_seed_id.group(1)
            log_data["seed_id"] = "Successfully calculated seed id."
        except AttributeError:
            log_data["seed_id"] = "Could not calculate seed id from the WARC filename."
            web.warc_log(log_data)
            continue

        # Should catch the error in the previous step and this should not run.
        print("Test did not catch Error 3 correctly.")
        continue

    # ERROR 4: Cannot extract job_id from the WARC filename.
    if current_warc == 4:

        # Previous steps. Generate the error by assigning the wrong value to warc_filename.
        warc_filename = "warc-SEED123456-job-error.warc.gz"
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."

        # Calculates the job id from the WARC filename, which are the numbers after "-JOB".
        try:
            regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
            job_id = regex_job_id.group(1)
            log_data["job_id"] = "Successfully calculated job id."
        except AttributeError:
            log_data["job_id"] = "Could not calculate job id from the WARC filename."
            web.warc_log(log_data)
            continue

        # Should catch the error in the previous step and this should not run.
        print("Test did not catch Error 4 correctly.")
        continue

    # ERROR 5: Cannot find expected values in seed JSON (KeyError).
    if current_warc == 5:

        # Previous steps.
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."
        regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
        job_id = regex_job_id.group(1)
        log_data["job_id"] = "Successfully calculated job id."

        # Generates error by removing an expected value.
        # Saves the value so it can be put back after the error for testing future warcs from this seed.
        seed_info = seed_metadata.pop(seed_id)

        # Saves relevant information the WARC's seed in variables for future use.
        try:
            aip_id = seed_metadata[seed_id][0]
            aip_title = seed_metadata[seed_id][1]
            log_data["seed_metadata"] = "Successfully got seed metadata."
        except KeyError:
            log_data["seed_metadata"] = "Seed id is not in seed JSON."
            web.warc_log(log_data)
            seed_metadata[seed_id] = seed_info
            continue
        except IndexError:
            log_data["seed_metadata"] = f"At least one value missing from JSON for this seed: {seed_metadata[seed_id]}"
            web.warc_log(log_data)
            seed_metadata[seed_id] = seed_info
            continue

        # Should catch the error in the previous step and this should not run.
        print("Test did not catch Error 5 correctly.")
        continue

    # ERROR 6: Cannot find expected values in seed JSON (IndexError).
    if current_warc == 6:

        # Previous steps.
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."
        regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
        job_id = regex_job_id.group(1)
        log_data["job_id"] = "Successfully calculated job id."

        # Generates error by removing an expected value.
        # Saves the value so it can be put back after the error for testing future warcs from this seed.
        seed_info = seed_metadata[seed_id].pop(1)

        # Saves relevant information the WARC's seed in variables for future use.
        try:
            aip_id = seed_metadata[seed_id][0]
            aip_title = seed_metadata[seed_id][1]
            log_data["seed_metadata"] = "Successfully got seed metadata."
        except KeyError:
            log_data["seed_metadata"] = "Seed id is not in seed JSON."
            web.warc_log(log_data)
            seed_metadata[seed_id].append(seed_info)
            continue
        except IndexError:
            log_data["seed_metadata"] = f"At least one value missing from JSON for this seed: {seed_metadata[seed_id]}"
            web.warc_log(log_data)
            seed_metadata[seed_id].append(seed_info)
            continue

        # Should catch the error in the previous step and this should not run.
        print("Test did not catch Error 6 correctly.")
        continue

    # ERROR 7: API error downloading metadata reports.
    if current_warc == 7:

        # Previous steps.
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."
        regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
        job_id = regex_job_id.group(1)
        log_data["job_id"] = "Successfully calculated job id."
        aip_id = seed_metadata[seed_id][0]
        aip_title = seed_metadata[seed_id][1]
        log_data["seed_metadata"] = "Successfully got seed metadata."
        seed_to_aip[seed_id] = aip_id
        aip_to_title[aip_id] = aip_title
        web.make_aip_directory(aip_id)

        # Downloads the seed metadata from Archive-It into the seed's metadata folder.
        download_metadata(aip_id, warc_collection, job_id, seed_id, date_end, log_data)

        # Last step for this test, so saves the log.
        web.warc_log(log_data)

    # ERROR 8: API error downloading WARC.
    if current_warc == 8:

        # Previous steps.
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."
        regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
        job_id = regex_job_id.group(1)
        log_data["job_id"] = "Successfully calculated job id."
        aip_id = seed_metadata[seed_id][0]
        aip_title = seed_metadata[seed_id][1]
        log_data["seed_metadata"] = "Successfully got seed metadata."
        seed_to_aip[seed_id] = aip_id
        aip_to_title[aip_id] = aip_title
        web.make_aip_directory(aip_id)
        web.download_metadata(aip_id, warc_collection, job_id, seed_id, date_end, log_data)

        # Downloads the WARC from Archive-It into the seed's objects folder.
        # There are multiple errors for this function, so indicates the error type.
        download_warc(aip_id, warc_filename, warc_url, warc_md5, date_end, log_data, "download")

        # Last step for this test, so saves the log.
        web.warc_log(log_data)

    # ERROR 9: Cannot extract fixity from MD5Deep output.
    if current_warc == 9:

        # Previous steps.
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."
        regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
        job_id = regex_job_id.group(1)
        log_data["job_id"] = "Successfully calculated job id."
        aip_id = seed_metadata[seed_id][0]
        aip_title = seed_metadata[seed_id][1]
        log_data["seed_metadata"] = "Successfully got seed metadata."
        seed_to_aip[seed_id] = aip_id
        aip_to_title[aip_id] = aip_title
        web.make_aip_directory(aip_id)
        web.download_metadata(aip_id, warc_collection, job_id, seed_id, date_end, log_data)

        # Downloads the WARC from Archive-It into the seed's objects folder.
        # There are multiple errors for this function, so indicates the error type.
        download_warc(aip_id, warc_filename, warc_url, warc_md5, date_end, log_data, "md5deep")

        # Last step for this test, so saves the log.
        web.warc_log(log_data)

    # ERROR 10: WARC fixity after download doesn't match Archive-It record.
    if current_warc == 10:

        # Previous steps.
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."
        regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
        job_id = regex_job_id.group(1)
        log_data["job_id"] = "Successfully calculated job id."
        aip_id = seed_metadata[seed_id][0]
        aip_title = seed_metadata[seed_id][1]
        log_data["seed_metadata"] = "Successfully got seed metadata."
        seed_to_aip[seed_id] = aip_id
        aip_to_title[aip_id] = aip_title
        web.make_aip_directory(aip_id)
        web.download_metadata(aip_id, warc_collection, job_id, seed_id, date_end, log_data)

        # Downloads the WARC from Archive-It into the seed's objects folder.
        # There are multiple errors for this function, so indicates the error type.
        download_warc(aip_id, warc_filename, warc_url, warc_md5, date_end, log_data, "fixity")

        # Last step for this test, so saves the log.
        web.warc_log(log_data)

print("\nStarting empty directory tests.")
# For testing, create two AIPs with empty directories.
# Some of the previous tests also have empty objects folders and will be moved too.
os.makedirs("test-999-web-metadata-empty/metadata")
os.makedirs("test-999-web-metadata-empty/objects")
with open("test-999-web-metadata-empty/objects/file.txt", "w") as new_file:
    new_file.write("Sample text.")

os.makedirs("test-000-web-objects-empty/metadata")
os.makedirs("test-000-web-objects-empty/objects")
with open("test-000-web-objects-empty/metadata/file.txt", "w") as new_file:
    new_file.write("Sample text.")

# Checks for empty metadata or objects folders in the AIPs.
# Should catch the two test AIPs just created.
log_path = "../aip_log.txt"
web.find_empty_directory(log_path)

# ----------------------------------------------------------------------------------------------------------------
# THIS REPLACES THE PART OF THE SCRIPT THAT MAKES THE AIPS.
# INSTEAD, IT CREATES A SET OF TO USE FOR TESTING check_aips.
# ----------------------------------------------------------------------------------------------------------------
print("\nStarting completeness tests.")
log_path = "../aip_log.txt"
aip.log(log_path, "\nStarting Completeness Tests")

# 2529629 is already in the AIPs directory from earlier testing but not structured right for this test.
# Delete what is there and make a new one.
import shutil
shutil.rmtree("magil-ggp-2529629-2022-03")

# Seeds that are part of the download if use 2022-03-20 and 2022-03-25 as date boundaries.

seed_to_aip = {"2529671": "magil-ggp-2529671-2022-03", "2529669": "magil-ggp-2529669-2022-03",
               "2529633": "magil-ggp-2529633-2022-03", "2529665": "magil-ggp-2529665-2022-03",
               "2529634": "magil-ggp-2529634-2022-03", "2529660": "magil-ggp-2529660-2022-03",
               "2529642": "magil-ggp-2529642-2022-03", "2529627": "magil-ggp-2529627-2022-03",
               "2529652": "magil-ggp-2529652-2022-03", "2529631": "magil-ggp-2529631-2022-03",
               "2529668": "magil-ggp-2529668-2022-03", "2529681": "magil-ggp-2529681-2022-03",
               "2529676": "magil-ggp-2529676-2022-03", "2529629": "magil-ggp-2529629-2022-03"}

aip_to_title = {"magil-ggp-2529671-2022-03": "Title", "magil-ggp-2529669-2022-03": "Title",
               "magil-ggp-2529633-2022-03": "Title", "magil-ggp-2529665-2022-03": "Title",
               "magil-ggp-2529634-2022-03": "Title", "magil-ggp-2529660-2022-03": "Title",
               "magil-ggp-2529642-2022-03": "Title", "magil-ggp-2529627-2022-03": "Title",
               "magil-ggp-2529652-2022-03": "Title", "magil-ggp-2529631-2022-03": "Title",
               "magil-ggp-2529668-2022-03": "Title", "magil-ggp-2529681-2022-03": "Title",
               "magil-ggp-2529676-2022-03": "Title", "magil-ggp-2529629-2022-03": "Title"}


# # Make one AIP folder for each of the seeds with fake metadata files, warcs, and bagging.
# # Just has to have everything that the completeness check looks for.
for seed in seed_to_aip:

    aip_id = seed_to_aip[seed]
    aip_folder = aip_id + "_bag"

    # Make error of missing AIP folder by skipping this AIP.
    if seed == "2529627":
        continue

    # Metadata folder and its contents.
    os.makedirs(f"{aip_folder}/data/metadata")
    metadata_ext = ["seed.csv", "seedscope.csv", "collscope.csv", "coll.csv", "crawljob.csv", "crawldef.csv",
                    "fits.xml", "preservation.xml"]

    # Make error by not including the Archive-It metadata reports for this AIP.
    if seed == "2529629":
        with open(f"{aip_folder}/data/metadata/{aip_id}_fits.xml", "w") as new_file:
            new_file.write("Test")
        with open(f"{aip_folder}/data/metadata/{aip_id}_preservation.xml", "w") as new_file:
            new_file.write("Test")

    # Make error by not including the fits or preservation.xml files for this AIP.
    elif seed == "2529642":
        for ext in ("seed.csv", "seedscope.csv", "collscope.csv", "coll.csv", "crawljob.csv", "crawldef.csv"):
            with open(f"{aip_folder}/data/metadata/{aip_id}_{ext}", "w") as new_file:
                new_file.write("Test")

    # For the rest of the AIPs, add all expected metadata files.
    else:
        for ext in metadata_ext:
            # Add one each of all of the expected metadata files.
            with open(f"{aip_folder}/data/metadata/{aip_id}_{ext}", "w") as new_file:
                new_file.write("Test")

    # Test counting by adding extra crawldef and crawljob to this AIP.
    if seed == "2529631":
        with open(f"{aip_folder}/data/metadata/{aip_id}_2_crawldef.csv", "w") as new_file:
            new_file.write("Test")
        with open(f"{aip_folder}/data/metadata/{aip_id}_2_crawljob.csv", "w") as new_file:
            new_file.write("Test")
        with open(f"{aip_folder}/data/metadata/{aip_id}_3_crawljob.csv", "w") as new_file:
            new_file.write("Test")

    # Test fits count by adding extra FITS for one AIP with 2 WARCs (correct) and one with 1 WARC (error).
    if seed in ("2529634", "2529668"):
        with open(f"{aip_folder}/data/metadata/{aip_id}_2_fits.xml", "w") as new_file:
            new_file.write("Test")

    # Make error by making a metadata file that isn't an expected type for this AIP.
    if seed == "2529669":
        with open(f"{aip_folder}/data/metadata/{aip_id}_error.txt", "w") as new_file:
            new_file.write("Test")

    # Objects folder and its contents. All by one seed have 1 WARC.
    # Making files with a WARC extension but they are not really WARCs.
    os.makedirs(f"{aip_folder}/data/objects")

    # Make error by not making a WARC for this AIP.
    if seed == "2529660":
        continue
    # For all other AIPs, make one test WARC.
    else:
        with open(f"{aip_folder}/data/objects/test.warc.gz", "w") as new_file:
            new_file.write("Test")

    # Test WARC count by adding extra WARC for one AIP with 2 WARCs (correct) and one with 1 WARC (error).
    if seed in ("2529634", "2529652"):
        with open(f"{aip_folder}/data/objects/test2.warc.gz", "w") as new_file:
            new_file.write("Test")

    # Make error by adding something other than a WARC to the objects folder for this AIP.
    if seed == "2529665":
        with open(f"{aip_folder}/data/objects/error.txt", "w") as new_file:
            new_file.write("Test")

# Makes an error by adding an AIP to the AIPs directory that is not expected.
os.makedirs("magil-error-000000-2022-03_bag")

# Verifies the AIPs are complete and no extra AIPs were created. Does not look at the errors folder, so any AIPs with
# errors will show as missing. Saves the result as a csv in the folder with the downloaded AIPs.
web.check_aips(date_end, date_start, seed_to_aip, log_path)
