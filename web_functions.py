"""Purpose: functions used by the preservation scripts for web content in Archive-It.

Dependencies:
    * Python library: requests
    * Tool: md5deep
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

# Import constant variables and functions from another UGA preservation script.
import configuration as c


def check_config():
    """
    Checks that all required variables are in the configuration file and correct.
    If there are any errors, prints an explanation and quits the script.
    """
    errors = []

    # Checks if the path in script_output exists on the local machine.
    try:
        if not os.path.exists(c.script_output):
            errors.append(f"Variable path '{c.script_output}' is not correct.")
    except AttributeError:
        errors.append("Variable 'script_output' is missing from the configuration file.")

    # Checks that the API URLs, which are consistent values, are correct.
    try:
        if c.partner_api != 'https://partner.archive-it.org/api':
            errors.append("Partner API path is not correct.")
    except AttributeError:
        errors.append("Variable 'partner_api' is missing from the configuration file.")
    try:
        if c.wasapi != 'https://warcs.archive-it.org/wasapi/v1/webdata':
            errors.append("WASAPI path is not correct.")
    except AttributeError:
        errors.append("Variable 'wasapi' is missing from the configuration file.")

    # Checks that the institution page exists.
    try:
        response = requests.get(c.inst_page)
        if response.status_code != 200:
            errors.append("Institution Page URL is not correct.")
    except AttributeError:
        errors.append("Variable 'inst_page' is missing from the configuration file.")

    # Checks that the username and password are present.
    try:
        c.username
    except AttributeError:
        errors.append("Variable 'username' is missing from the configuration file.")
    try:
        c.password
    except AttributeError:
        errors.append("Variable 'password' is missing from the configuration file.")

    # Checks that the Archive-It username and password are correct by using them with an API call.
    # This only works if the partner_api variable is in the configuration file.
    try:
        response = requests.get(f'{c.partner_api}/seed?limit=5', auth=(c.username, c.password))
        if response.status_code != 200:
            errors.append("Could not access Partner API with provided credentials. "
                          "Check if the partner_api, username, and/or password variables have errors.")
    except AttributeError:
        errors.append("Variables 'partner_api', 'username', and/or 'password' are missing from the configuration file.")

    # If there were errors, prints them and exits the script.
    if len(errors) > 0:
        print("\nProblems detected with configuration.py.")
        for error in errors:
            print("    *", error)
        print("\nCorrect the configuration file using configuration_template.py as a model")
        sys.exit()


def seed_data(date_start, date_end):
    """Uses WASAPI to get information about each WARC and seed to include in the download.
    Returns the data as a dataframe and also saves it to a CSV in the script output folder to use for the log
    and for splitting big downloads or restarting jobs if the script breaks."""

    # Uses WASAPI to get information about all WARCs in this download, based on the date limits.
    # WASAPI is the only API that allows limiting by date.
    filters = {"store-time-after": date_start, "store-time-before": date_end, "page_size": 10000}
    warcs = requests.get(c.wasapi, params=filters, auth=(c.username, c.password))

    # If there was an error with the API call, quits the script.
    if not warcs.status_code == 200:
        print(f"\nAPI error {warcs.status_code} when getting WARC data.")
        print(f"Ending script (this information is required). Try script again later.")
        exit()

    # Saves WARC data from WASAPI (which downloads as a dictionary) to a dataframe and reorganizes it by seed.
    # For each seed: Archive-It collection, seed id, job, size in GB, number of WARCs, and all the WARC filenames.
    rows = []
    for file in warcs.json()['files']:
        rows.append([file['collection'], file['crawl'], file['size'], file['filename']])
    warc_df = pd.DataFrame(rows, columns=["AIT_Collection", "Job_ID", "Size", "WARC_Filename"])
    warc_df['Seed_ID'] = warc_df['WARC_Filename'].str.extract(r"^.*-SEED(\d+)-")

    coll_df = warc_df[['Seed_ID', 'AIT_Collection']].copy()
    coll_df = coll_df.drop_duplicates()
    coll_df['AIT_Collection'] = coll_df['AIT_Collection'].astype(str)
    coll_by_seed = coll_df.groupby(['Seed_ID'])['AIT_Collection'].apply(';'.join)

    job_df = warc_df[['Seed_ID', 'Job_ID']].copy()
    job_df = job_df.drop_duplicates()
    job_df['Job_ID'] = job_df['Job_ID'].astype(str)
    jobs_by_seed = job_df.groupby(['Seed_ID'])['Job_ID'].apply(';'.join)

    warc_df['Size_GB'] = warc_df['Size']/1000000000
    gb_by_seed = warc_df.groupby(['Seed_ID'])['Size_GB'].sum().round(3)

    count_by_seed = warc_df.groupby('Seed_ID')['Seed_ID'].count()

    warc_names = warc_df.groupby(['Seed_ID'])['WARC_Filename'].apply(';'.join)

    seed_df = pd.concat([coll_by_seed, jobs_by_seed, gb_by_seed, count_by_seed, warc_names], axis=1)
    seed_df.columns = ["AIT_Collection", "Job_ID", "Size_GB", 'WARCs', "WARC_Filenames"]
    seed_df = seed_df.reset_index()

    # Adds columns for logging the workflow steps.
    log_columns = ["Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction", "WARC_API_Errors",
                   "WARC_Fixity_Errors", "WARC_Unzip_Errors"]
    seed_df = seed_df.reindex(columns=seed_df.columns.tolist() + log_columns)

    # Saves the dataframe as a CSV in the script output folder for splitting or restarting a batch.
    # Returns the dataframe for when the entire group will be downloaded as one batch.
    seed_df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)
    return seed_df


def log(message, df, row, column):
    """Adds log information to the seeds dataframe and saves an updated version of seeds_log.csv."""

    # Updates the dataframe. Separates messages with a a semicolon if there is more than one.
    if pd.isnull(df.at[row, column]):
        df.loc[row, column] = message
    else:
        df.loc[row, column] += "; " + message

    # Saves a new version of seeds_log.csv with the updated information.
    # The previous version of the file is overwritten.
    df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)


def reset_aip(seed_id, df):
    """Deletes the directories and log information for a seed
    that was partially completed when the script broke so it can be remade."""

    # Deletes the seed folder and all its contents.
    shutil.rmtree(seed_id)

    # Clears data in the seed dataframe related to successfully completing metadata and WARC downloading
    # from the failed attempt.
    row_index = df.index[df['Seed_ID'] == seed_id].tolist()[0]
    df.loc[row_index, 'Metadata_Report_Errors'] = None
    df.loc[row_index, 'Metadata_Report_Empty'] = None
    df.loc[row_index, 'Seed_Report_Redaction'] = None
    df.loc[row_index, 'WARC_API_Errors'] = None
    df.loc[row_index, 'WARC_Fixity_Errors'] = None
    df.loc[row_index, 'WARC_Unzip_Errors'] = None

    # Saves a new version of seeds_log.csv with the updated information.
    # The previous version of the file is overwritten.
    df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)


def get_report(seed, seed_df, row_index, filter_type, filter_value, report_type, report_name):
    """
    Downloads a single metadata report and saves it as a csv in the seed's folder.
    Only saves if there is data of that type.

    Parameters:
    seed, seed_df, and row_index are used to update the log
    filter_type and filter_value are used to filter the API call to the right AIP's report
    report_type is the Archive-It name for the report
    report_name is the name for the report saved in the AIP, including the ARCHive metadata code
    """

    # Builds the API call to get the report as a csv.
    # Limit of -1 will return all matches. Default is only the first 100.
    filters = {"limit": -1, filter_type: filter_value, "format": "csv"}
    metadata_report = requests.get(f"{c.partner_api}/{report_type}", params=filters, auth=(c.username, c.password))

    # Saves the metadata report if there were no API errors and there was data of this type (content isn't empty).
    # For scope rules, it is common for one or both to not have data since these aren't required.
    if metadata_report.status_code == 200:
        if metadata_report.content == b"":
            log(report_name, seed_df, row_index, "Metadata_Report_Empty")
            return
        else:
            with open(f"{seed.Seed_ID}/{report_name}", "wb") as report_csv:
                report_csv.write(metadata_report.content)
    else:
        log(f"{report_name} API Error {metadata_report.status_code}", seed_df, row_index, "Metadata_Report_Errors")
        return


def redact_seed_report(seed_id, seed_df, row_index):
    """
    Replaces the seed report with a redacted version of the file, removing login information if those columns
    are present. Even if the columns are blank, replaces it with REDACTED. Since not all login information is
    meaningful (some is from staff web browsers autofill information while viewing the metadata), knowing if
    there was login information or not is misleading.
    """

    report_df = pd.read_csv(f"{seed_id}/{seed_id}_seed.csv")
    if "login_password" in report_df.columns:
        report_df["login_username"] = "REDACTED"
        report_df["login_password"] = "REDACTED"
        report_df.to_csv(f"{seed_id}/{seed_id}_seed.csv", index=False)
        log("Successfully redacted", seed_df, row_index, "Seed_Report_Redaction")
    else:
        log("No login columns to redact", seed_df, row_index, "Seed_Report_Redaction")


def download_job_and_definition(seed, seed_df, row_index):
    """
    Downloads each of the crawl job reports and its corresponding crawl definition report (if new).
    """

    # If a seed has more than one job, Job_ID has a comma-separated string of the IDs.
    job_list = seed.Job_ID.split(";")

    for job in job_list:

        # Downloads the crawl job for report.
        get_report(seed, seed_df, row_index, "id", job, "crawl_job", f"{seed.Seed_ID}_{job}_crawljob.csv")

        # Reads the crawl job to get the crawl definition ID.
        # If reading the id is successful and the report isn't downloaded yet, downloads the crawl definition report.
        # If the crawl job report wasn't downloaded, logs the error instead.
        try:
            report_df = pd.read_csv(f"{seed.Seed_ID}/{seed.Seed_ID}_{job}_crawljob.csv", dtype="object")
            crawl_def_id = report_df.loc[0, "crawl_definition"]
            crawL_def_report_name = f"{seed.Seed_ID}_{crawl_def_id}_crawldef.csv"
            if not os.path.exists(f"{seed.Seed_ID}/{crawL_def_report_name}"):
                get_report(seed, seed_df, row_index, "id", crawl_def_id, "crawl_definition", crawL_def_report_name)
        except FileNotFoundError:
            log("Crawl job was not downloaded so can't get crawl definition id",
                seed_df, row_index, "Metadata_Report_Errors")


def download_metadata(seed, seed_df):
    """
    Uses the Partner API to download six metadata reports to include in the AIPs for archived websites,
    deletes any empty reports (meaning there was no data of that type for this seed),
    and redacts login information from the seed report.
    Any errors are added to the seed dataframe and saved to the script log at the end of the function.
    """

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # Downloads four of the six metadata reports from Archive-It needed to understand the context of the WARC.
    # These are reports where there is only one report per seed or collection.
    get_report(seed, seed_df, row_index, "id", seed.Seed_ID, "seed", f"{seed.Seed_ID}_seed.csv")
    get_report(seed, seed_df, row_index, "seed", seed.Seed_ID, "scope_rule", f"{seed.Seed_ID}_seedscope.csv")
    get_report(seed, seed_df, row_index, "collection", seed.AIT_Collection, "scope_rule", f"{seed.Seed_ID}_collscope.csv")
    get_report(seed, seed_df, row_index, "id", seed.AIT_Collection, "collection", f"{seed.Seed_ID}_coll.csv")

    # Redacts login information from the seed report.
    redact_seed_report(seed.Seed_ID, seed_df, row_index)

    # Downloads each of the crawl job reports and its corresponding crawl definition report (if new).
    download_job_and_definition(seed, seed_df, row_index)

    # If there were no download errors (the dataframe still has no value in that cell), updates the log to show success.
    if pd.isnull(seed_df.at[row_index, "Metadata_Report_Errors"]):
        seed_df.loc[row_index, "Metadata_Report_Errors"] = "Successfully downloaded all metadata reports"
        seed_df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)

    # If there were no deleted empty reports (the dataframe still has not value in that cell), updates the log.
    if pd.isnull(seed_df.at[row_index, "Metadata_Report_Empty"]):
        seed_df.loc[row_index, "Metadata_Report_Empty"] = "No empty reports"
        seed_df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)


def get_warc_info(warc, seed_df, row_index):
    """
    Gets and returns the URL for downloading the WARC and WARC MD5 from Archive-It using WASAPI.
    """

    # WASAPI call to get all data related to this WARC.
    warc_data = requests.get(f'{c.wasapi}?filename={warc}', auth=(c.username, c.password))

    # If there is an API error, updates the log and raises an error to skip the rest of the steps for this WARC.
    if not warc_data.status_code == 200:
        log(f"API error {warc_data.status_code}: can't get info about {warc}",
            seed_df, row_index, "WARC_API_Errors")
        raise ValueError

    # Gets and returns the two data points needed from the WASAPI results, unless there is an error.
    py_warc = warc_data.json()
    try:
        warc_url = py_warc["files"][0]["locations"][0]
        warc_md5 = py_warc["files"][0]["checksums"]["md5"]
        return warc_url, warc_md5
    except IndexError:
        log(f"Index Error: cannot get the WARC URL or MD5 for {warc}",
            seed_df, row_index, "WARC_API_Errors")
        raise IndexError


def get_warc(seed_df, row_index, warc_url, warc, warc_path):
    """
    Downloads the WARC and saves it to the seed folder.
    """

    # Downloads the WARC, which will be zipped.
    warc_download = requests.get(f"{warc_url}", auth=(c.username, c.password))

    # Updates the log with the success or error from the download.
    # If there was an error, raises an error to skip the rest of the steps for this WARC.
    if warc_download.status_code == 200:
        log(f"Successfully downloaded {warc}", seed_df, row_index, "WARC_API_Errors")
    else:
        log(f"API error {warc_download.status_code}: can't download {warc}",
            seed_df, row_index, "WARC_API_Errors")
        raise ValueError

    # Saves the zipped WARC in the seed folder, keeping the original filename.
    with open(warc_path, 'wb') as warc_file:
        warc_file.write(warc_download.content)


def verify_warc_fixity(seed_df, row_index, warc_path, warc, warc_md5):
    """
    Compares the fixity of the downloaded zipped WARC to the fixity in Archive-It
    and deletes the file if it does not match.
    """

    # Calculates the md5 for the downloaded zipped WARC with md5deep.
    md5deep_output = subprocess.run(f'"{c.md5deep}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)
    try:
        regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(md5deep_output.stdout))
        downloaded_warc_md5 = regex_md5.group(1)
    except AttributeError:
        log(f"Fixity for {warc} cannot be extracted from md5deep output: {md5deep_output.stdout}",
            seed_df, row_index, "WARC_Fixity_Errors")
        raise AttributeError

    # Compares the md5 of the downloaded zipped WARC to Archive-It metadata.
    # If the md5 has changed, deletes the WARC.
    if warc_md5 == downloaded_warc_md5:
        log(f"Successfully verified {warc} fixity on {datetime.datetime.now()}",
            seed_df, row_index, "WARC_Fixity_Errors")
    else:
        os.remove(warc_path)
        log(f"Fixity for {warc} changed and it was deleted: {warc_md5} before, {downloaded_warc_md5} after",
            seed_df, row_index, "WARC_Fixity_Errors")
        raise ValueError


def unzip_warc(seed_df, row_index, warc_path, warc, seed_id, date_end):
    """
    Unzips the WARC, which is downloaded as a gzip file.
    If it unzipped correctly, deletes the zip file.
    If there was an unzip error, deletes the unzipped file and leaves the zip.
    """

    # Extracts the WARC from the gzip file.
    unzip_output = subprocess.run(f'"C:/Program Files/7-Zip/7z.exe" x "{warc_path}" -o"{seed_id}"',
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True)

    # Checks for and logs any 7-Zip errors.
    if unzip_output.stderr == b'':
        # A filename (warc.open) is an error from a known gzip bug. Deletes the erroneous unzipped file.
        if os.path.exists(f'{c.script_output}/aips_{date_end}/{seed_id}/{warc}.open'):
            os.remove(f'{c.script_output}/aips_{date_end}/{seed_id}/{warc}.open')
            log(f"Error unzipping {warc}: unzipped to '.gz.open' file", seed_df, row_index, "WARC_Unzip_Errors")
        # If it unzipped correctly, deletes the zip file.
        else:
            os.remove(warc_path)
            log(f"Successfully unzipped {warc}", seed_df, row_index, "WARC_Unzip_Errors")
    else:
        log(f"Error unzipping {warc}: {unzip_output.stderr.decode('utf-8')}",
            seed_df, row_index, "WARC_Unzip_Errors")


def download_warcs(seed, date_end, seed_df):
    """Downloads every WARC file and verifies that fixity is unchanged after downloading.
    Unzips each WARC."""

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # Makes a list of the filenames for all WARCs for this seed.
    warc_names = seed.WARC_Filenames.split(";")

    # Downloads and validates every WARC.
    # If an error is caught at any point, logs the error and starts the next WARC.
    for warc in warc_names:

        # The path for where the WARC will be saved on the local machine.
        warc_path = f'{c.script_output}/aips_{date_end}/{seed.Seed_ID}/{warc}'

        # Gets URL for downloading the WARC and WARC MD5 from Archive-It using WASAPI.
        # If there was an API error, stops processing this WARC and starts the next.
        try:
            warc_url, warc_md5 = get_warc_info(warc, seed_df, row_index)
        except (ValueError, IndexError):
            continue

        # Downloads the WARC from Archive-It.
        # If there is an API error, stops processing this WARC and starts the next.
        try:
            get_warc(seed_df, row_index, warc_url, warc, warc_path)
        except ValueError:
            continue

        # Verifies that the WARC fixity after download is correct, and deletes it if not.
        try:
            verify_warc_fixity(seed_df, row_index, warc_path, warc, warc_md5)
        except (AttributeError, ValueError):
            continue

        # Unzips the WARC and handles any errors.
        unzip_warc(seed_df, row_index, warc_path, warc, seed.Seed_ID, date_end)

        # Waits 15 second to give the API a rest.
        time.sleep(15)


def check_aips(date_end, date_start, seed_df, aips_directory):
    """Verifies that all the expected seed folders for the download are complete
    and no unexpected seed folders were created.
    Produces a csv named completeness_check with the results in the AIPs directory. """

    def aip_dictionary():
        """Uses the Archive-It APIs and Python filters to gather information about the expected AIPs. Using Python
        instead of the API to filter the results for a more independent analysis of expected AIPs. All WARC
        information is downloaded, filtered with Python to those expected in this preservation download, and the WARC
        information is aggregated into a dictionary organized by seed/AIP. The key is the seed id and the values are
        the AIP id, warc count, and url. """

        # Downloads the entire WARC list.
        filters = {'page_size': 10000}
        warcs = requests.get(c.wasapi, params=filters, auth=(c.username, c.password))

        # If there was an API error, ends the function.
        if warcs.status_code != 200:
            print("WASAPI error, status code: ", warcs.status_code)
            raise ValueError

        # Converts json from API to a python object.
        py_warcs = warcs.json()

        # Starts variables used to verify that the script processes the right number of WARCs. The total number of WARCs
        # that are either part of this download (include) or not part of it (exclude) are compared to the total
        # number of WARCs expected from the API data.
        warcs_expected = py_warcs['count']
        warcs_include = 0
        warcs_exclude = 0

        # Starts the dictionary for the AIP metadata generated from the WARC metadata.
        aip_info = {}

        # Iterates over the metadata for each WARC.
        for warc_info in py_warcs['files']:

            # Gets the seed id from the WARC filename.
            try:
                regex_seed = re.match(r".*-SEED(\d+)-.*", warc_info['filename'])
                seed_identifier = regex_seed.group(1)
            except AttributeError:
                print(f"Unable to get seed ID for {warc_info['filename']}")
                raise ValueError

            # Filter one: only includes the WARC in the dictionary if it was created since the last download and
            # before the current download. Store time is used so test crawls are evaluated based on the date they
            # were saved. Simplifies the date format to YYYY-MM-DD by removing the time information before comparing
            # it to the last download date.
            try:
                regex_crawl_date = re.match(r"(\d{4}-\d{2}-\d{2})T.*", warc_info['store-time'])
                crawl_date = regex_crawl_date.group(1)
            except AttributeError:
                print(f"Unable to reformat date {warc_info['store-time']} for {warc_info['filename']}")
                raise ValueError

            # With WASAPI the start date is inclusive but the end date is not.
            if crawl_date < date_start or crawl_date >= date_end:
                warcs_exclude += 1
                continue

            # Checks if another WARC from this seed has been processed, meaning there is data in the aip_info
            # dictionary. If so, updates the WARC count in the dictionary and starts processing the next WARC. If
            # not, continues processing this WARC.
            try:
                aip_info[seed_identifier][1] += 1
                warcs_include += 1

            # Filter two: only includes the WARC in the dictionary if the repository is Hargrett, MAGIL, or Russell.
            # The repository is in the seed report.
            except (KeyError, IndexError):

                # Gets the seed report for this seed.
                seed_report = requests.get(f'{c.partner_api}/seed?id={seed_identifier}', auth=(c.username, c.password))
                json_seed = seed_report.json()

                # If there was an API error, ends the function.
                if seed_report.status_code != 200:
                    print(f"Unable to get seed report for seed {seed_identifier}. Status {seed_report.status_code}.")
                    raise ValueError

                # Gets the repository from the seed report, if present. If not, this WARC is not included.
                try:
                    repository = json_seed[0]['metadata']['Collector'][0]['value']
                except (KeyError, IndexError):
                    warcs_exclude += 1
                    continue

                # Does not include the WARC in the dictionary if the repository is not Hargrett, MAGIL, or Russell.
                if not repository.startswith(('Hargrett', 'Map', 'Richard B. Russell')):
                    warcs_exclude += 1
                    continue

                # Saves data about the WARC to the dictionary (AIP id, WARC count, URL). If the seed is not in
                # seed_df, it is an unexpected seed and cannot be added to the dictionary.
                try:
                    aip_info[seed_identifier] = [seed_df.loc[seed_df["Seed_ID"] == seed_identifier]["AIP_ID"].item(),
                                                 1, json_seed[0]['url']]
                except (KeyError, ValueError, IndexError):
                    print(f"Seed {seed_identifier} is not in seeds_df")
                    warcs_exclude += 1
                    continue

                warcs_include += 1

        # Checks that the right number of WARCs were evaluated.
        if warcs_expected != warcs_include + warcs_exclude:
            print("Check AIPs did not review the expected number of WARCs.")
            raise ValueError

        return aip_info

    def check_completeness(aip_id, warc_total, website_url):
        """Verifies a single AIP is complete, checking the contents of the metadata and objects folders. Returns a
        list with the results ready to be added as a row to the completeness check csv. """

        # Starts a list for the results, with the AIP id and website url to use for identification of the AIP.
        result = [aip_id, website_url]

        # Tests if there is a folder for this seed in the AIPs directory.
        # If not, returns the result for this AIP and does not run the rest of the function's tests.
        if aip_id in os.listdir(aips_directory):
            result.append(True)
        else:
            result.extend([False, 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a'])
            return result

        # Tests if each of the four Archive-It metadata reports that never repeat are present.
        # os.path.exists() returns True/False.
        result.append(os.path.exists(f'{aips_directory}/{aip_id}/{aip_id}_coll.csv'))
        result.append(os.path.exists(f'{aips_directory}/{aip_id}/{aip_id}_collscope.csv'))
        result.append(os.path.exists(f'{aips_directory}/{aip_id}/{aip_id}_seed.csv'))
        result.append(os.path.exists(f'{aips_directory}/{aip_id}/{aip_id}_seedscope.csv'))

        # Counts the number of instances of the two Archive-It metadata reports than can repeat.
        # Compare to expected results in the WARC inventory.
        result.append(len([file for file in os.listdir(f'{aips_directory}/{aip_id}') if file.endswith('_crawldef.csv')]))
        result.append(len([file for file in os.listdir(f'{aips_directory}/{aip_id}') if file.endswith('_crawljob.csv')]))

        # Tests if the number of WARCs is correct. Compares the number of WARCs in the objects folder, calculated
        # with len(), to the number of WARCs expected from the API (warc_total).
        warcs = len([file for file in os.listdir(f'{aips_directory}/{aip_id}') if file.endswith('.warc')])
        if warcs == warc_total:
            result.append(True)
        else:
            result.append(False)

        # Tests if everything in the seed folder is an expected metadata file or a WARC.
        # Starts with a value of True and if there is a file of another type,
        # based on the end of the filename, it updates the value to False.
        result.append(True)
        expected_endings = ('_coll.csv', '_collscope.csv', '_crawldef.csv', '_crawljob.csv', '_seed.csv',
                            '_seedscope.csv', '.warc')
        for file in os.listdir(f'{aips_directory}/{aip_id}'):
            if not file.endswith(expected_endings):
                result[-1] = False

        return result

    def check_for_extra_aips():
        """Looks for seed folders that were created but were not expected based on the API data.
        If any are found, returns a list with the results ready to be added as a row to the results csv."""

        # Starts a list for the results. The list elements will be one list per unexpected seed.
        extras = []

        # Iterates through the folder with the AIPs.
        for aip_directory in os.listdir(aips_directory):

            # Creates a tuple of the expected AIPs, which are the values in the AIP_ID row in the seed dataframe.
            # Does not include blanks from any seeds where the AIP ID was not calculated.
            expected_aip_ids = tuple(seed_df[seed_df["AIP_ID"].notnull()]["AIP_ID"].to_list())

            # If there is an AIP that does not start with one of the expected AIP ids, adds a list with the values
            # for that AIP's row in the completeness check csv to the extras list.
            if not aip_directory.startswith(expected_aip_ids):
                extras.append([aip_directory, 'n/a', 'Not expected', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a',
                               'n/a', 'n/a'])

        # Only returns the extras list if at least one unexpected AIP was found.
        if len(extras) > 0:
            return extras

    try:
        aips_metadata = aip_dictionary()
    except (ValueError, IndexError, KeyError):
        print("Unable to make aip dictionary and cannot check for completeness.")
        return

    # Starts a csv for the results of the quality review.
    csv_path = f'{c.script_output}/completeness_check.csv'
    with open(csv_path, 'w', newline='') as complete_csv:
        complete_write = csv.writer(complete_csv)

        # Adds a header row to the csv.
        complete_write.writerow(
            ['AIP', 'URL', 'AIP Folder Made', 'coll.csv', 'collscope.csv', 'seed.csv',
             'seedscope.csv', 'crawldef.csv count', 'crawljob.csv count', 'WARC Count Correct',
             'All Expected File Types'])

        # Tests each AIP for completeness and saves the results.
        for seed in aips_metadata:
            aip_identifier, warc_count, website = aips_metadata[seed]
            row = check_completeness(aip_identifier, warc_count, website)
            complete_write.writerow(row)

        # Tests if there are folders in the AIP's directory that were not expected, and if so adds them to the
        # completeness check csv. The function only returns a value if there is at least one unexpected AIP.
        extra_aips = check_for_extra_aips()
        if extra_aips:
            for extra in extra_aips:
                complete_write.writerow(extra)
