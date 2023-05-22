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


def add_completeness(row_index, seed_df):
    """
    Uses the information already in the seed dataframe to log the types of errors for a seed
    or if it completed downloading without detecting any errors.
    This will eventually be updated to include a summary of the error types.
    """
    # Adds errors from Metadata_Report_Errors.
    if "Error" in seed_df.at[row_index, 'Metadata_Report_Errors']:
        log("Metadata_Report_Errors", seed_df, row_index, "Complete")

    # Adds errors from WARC_API_Errors.
    if "Error" in seed_df.at[row_index, 'WARC_API_Errors']:
        log("WARC_API_Errors", seed_df, row_index, "Complete")

    # Adds errors from WARC_Fixity_Errors.
    if "Error" in seed_df.at[row_index, 'WARC_Fixity_Errors']:
        log("WARC_Fixity_Errors", seed_df, row_index, "Complete")

    # Adds errors from WARC_Unzip_Errors.
    if "Error" in seed_df.at[row_index, 'WARC_Unzip_Errors']:
        log("WARC_Unzip_Errors", seed_df, row_index, "Complete")

    # If none of the previous columns had errors, so the Complete column does not have a string with the error types,
    # adds default text for no errors.
    if type(seed_df.at[row_index, 'Complete']) is not str:
        log("Successfully completed", seed_df, row_index, "Complete")


def check_aips(date_end, date_start, seed_df, seeds_directory):
    """Verifies that all the expected seed folders for the download are complete
    and no unexpected seed folders were created.
    Produces a csv named completeness_check with the results in the AIPs directory. """

    def seed_dictionary():
        """Uses the Archive-It APIs and Python filters to gather information about the expected seeds.
        Using Python instead of the API to filter the results for a more independent analysis of expected AIPs.
        All WARC information is downloaded, filtered with Python to those expected in this preservation download,
        and the WARC information is aggregated into a dictionary organized by seed.
        The key is the seed id and the values are the AIP id and warc count. """

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
        seed_info = {}

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

            # Checks if another WARC from this seed has been processed (there is data in seed_info. 
            # If so, updates the WARC count in the dictionary and starts processing the next WARC. 
            # If not, adds the seed to the dictionary.
            try:
                seed_info[seed_identifier][1] += 1
                warcs_include += 1
            except (KeyError, IndexError):
                try:
                    seed_info[seed_identifier] = [seed_df.loc[seed_df["Seed_ID"] == seed_identifier]["AIP_ID"].item(), 1]
                except (KeyError, ValueError, IndexError):
                    print(f"Seed {seed_identifier} is not in seeds_df")
                    warcs_exclude += 1
                    continue

                warcs_include += 1

        # Checks that the right number of WARCs were evaluated and prints a warning if not.
        # It still returns because this happens when downloads are in batches and still get some useful info.
        if warcs_expected != warcs_include + warcs_exclude:
            print("Check AIPs did not review the expected number of WARCs.")

        return seed_info

    def check_completeness(seed_id, aip_id, warc_total):
        """Verifies a single AIP is complete, checking the contents of the metadata and objects folders. Returns a
        list with the results ready to be added as a row to the completeness check csv. """

        # Starts a list for the results, with the Seed ID and AIP ID.
        result = [seed_id, aip_id]

        # Tests if there is a folder for this seed in the AIPs directory.
        # If not, returns the result for this AIP and does not run the rest of the function's tests.
        if seed_id in os.listdir(seeds_directory):
            result.append(True)
        else:
            result.extend([False, 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a'])
            return result

        # Tests if each of the four Archive-It metadata reports that never repeat are present.
        # os.path.exists() returns True/False.
        result.append(os.path.exists(f'{seeds_directory}/{seed_id}/{aip_id}_coll.csv'))
        result.append(os.path.exists(f'{seeds_directory}/{seed_id}/{aip_id}_collscope.csv'))
        result.append(os.path.exists(f'{seeds_directory}/{seed_id}/{aip_id}_seed.csv'))
        result.append(os.path.exists(f'{seeds_directory}/{seed_id}/{aip_id}_seedscope.csv'))

        # Counts the number of instances of the two Archive-It metadata reports than can repeat.
        # Compare to expected results in the WARC inventory.
        result.append(
            len([file for file in os.listdir(f'{seeds_directory}/{seed_id}') if file.endswith('_crawldef.csv')]))
        result.append(
            len([file for file in os.listdir(f'{seeds_directory}/{seed_id}') if file.endswith('_crawljob.csv')]))

        # Tests if the number of WARCs is correct. Compares the number of WARCs in the objects folder, calculated
        # with len(), to the number of WARCs expected from the API (warc_total).
        warcs = len([file for file in os.listdir(f'{seeds_directory}/{seed_id}') if file.endswith('.warc')])
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
        for file in os.listdir(f'{seeds_directory}/{seed_id}'):
            if not file.endswith(expected_endings):
                result[-1] = False

        return result

    def check_for_extra_seeds():
        """Looks for seed folders that were created but were not expected based on the API data.
        If any are found, returns a list with the results ready to be added as a row to the results csv."""

        # Starts a list for the results. The list elements will be one list per unexpected seed.
        extras = []

        # Iterates through the folder with the seeds.
        for seed_folder in os.listdir(seeds_directory):

            # Creates a tuple of the expected seeds, which are the values in the Seed_ID row in the seed dataframe.
            # and adds metadata.csv to the list, which will also be in the folder.
            expected_seed_ids = seed_df["Seed_ID"].values.tolist()
            expected_seed_ids.append("metadata.csv")

            # If there is a seed folder that is not named with one of the expected seed ids,
            # adds a list with the values for that seed's row in the completeness check csv to the extras list.
            if seed_folder not in expected_seed_ids:
                extras.append([seed_folder, 'n/a', 'Not expected', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a'])

        # Only returns the extras list if at least one unexpected seed was found.
        if len(extras) > 0:
            return extras

    try:
        seeds_metadata = seed_dictionary()
    except (ValueError, IndexError, KeyError):
        print("Unable to make seed dictionary and cannot check for completeness.")
        return

    # Starts a csv for the results of the quality review.
    csv_path = f'{c.script_output}/completeness_check.csv'
    with open(csv_path, 'w', newline='') as complete_csv:
        complete_write = csv.writer(complete_csv)

        # Adds a header row to the csv.
        complete_write.writerow(
            ['Seed', 'AIP', 'Seed Folder Made', 'coll.csv', 'collscope.csv', 'seed.csv',
             'seedscope.csv', 'crawldef.csv count', 'crawljob.csv count', 'WARC Count Correct',
             'All Expected File Types'])

        # Tests each AIP for completeness and saves the results.
        for seed in seeds_metadata:
            aip_identifier, warc_count = seeds_metadata[seed]
            row = check_completeness(seed, aip_identifier, warc_count)
            complete_write.writerow(row)

        # Tests if there are folders in the AIP's directory that were not expected, and if so adds them to the
        # completeness check csv. The function only returns a value if there is at least one unexpected AIP.
        extra_seeds = check_for_extra_seeds()
        if extra_seeds:
            for extra in extra_seeds:
                complete_write.writerow(extra)


def check_config():
    """
    Checks that all required variables are in the configuration file and correct.
    If there are any errors, prints an explanation and quits the script.
    """
    errors = []

    # Checks that the path in script_output exists on the local machine.
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

    # Checks that the path in md5deep exists on the local machine
    # and uses the correct direction of slashes (\).
    try:
        if not os.path.exists(c.md5deep):
            errors.append(f"Variable path '{c.md5deep}' is not correct.")
        elif "/" in c.md5deep:
            errors.append(f"Path '{c.md5deep}' must use \ for md5deep to work correctly.")
    except AttributeError:
        errors.append("Variable 'md5deep' is missing from the configuration file.")

    # If there were errors, prints them and exits the script.
    if len(errors) > 0:
        print("\nProblems detected with configuration.py.")
        for error in errors:
            print("    *", error)
        print("\nCorrect the configuration file using configuration_template.py as a model")
        sys.exit()


def download_crawl_definition(job_id, seed, seed_df, row_index):
    """
    Gets the crawl definition ID from the previously downloaded crawl job report
    and downloads the crawl definition report, if it was not already downloaded.
    """

    try:
        # If the crawl job report is present, reads it for the crawl definition id.
        job_df = pd.read_csv(f"{seed.Seed_ID}/{seed.AIP_ID}_{job_id}_crawljob.csv", dtype="object")
        crawl_def = job_df.loc[0, "crawl_definition"]

        # If the crawl definition report hasn't been downloaded yet, downloads the report.
        # Multiple jobs can have the same crawl definition, so it could already be downloaded.
        report_name = f"{seed.AIP_ID}_{crawl_def}_crawldef.csv"
        if not os.path.exists(f"{seed.Seed_ID}/{report_name}"):
            get_report(seed, seed_df, row_index, "id", crawl_def, "crawl_definition", report_name)

    # If the crawl job report wasn't downloaded due to an error, logs the error instead.
    except FileNotFoundError:
        log(f"Error: crawl job {job_id} was not downloaded so can't get crawl definition id",
            seed_df, row_index, "Metadata_Report_Errors")


def download_metadata(seed, row_index, seed_df):
    """
    Uses the Partner API to download six metadata reports to include in the AIPs for archived websites,
    deletes any empty reports (meaning there was no data of that type for this seed),
    and redacts login information from the seed report.
    Any errors are added to the seed dataframe and saved to the script log at the end of the function.
    """

    # Downloads four of the six metadata reports from Archive-It needed to understand the context of the WARC.
    # These are reports where there is only one report per seed or collection.
    get_report(seed, seed_df, row_index, "id", seed.Seed_ID, "seed", f"{seed.AIP_ID}_seed.csv")
    get_report(seed, seed_df, row_index, "seed", seed.Seed_ID, "scope_rule", f"{seed.AIP_ID}_seedscope.csv")
    get_report(seed, seed_df, row_index, "collection", seed.AIT_Collection, "scope_rule", f"{seed.AIP_ID}_collscope.csv")
    get_report(seed, seed_df, row_index, "id", seed.AIT_Collection, "collection", f"{seed.AIP_ID}_coll.csv")

    # Redacts login information from the seed report.
    redact_seed_report(seed.Seed_ID, seed.AIP_ID, seed_df, row_index)

    # Downloads each of the crawl job reports and its corresponding crawl definition report (if new).
    job_list = seed.Job_ID.split("|")
    for job in job_list:
        get_report(seed, seed_df, row_index, "id", job, "crawl_job", f"{seed.AIP_ID}_{job}_crawljob.csv")
        download_crawl_definition(job, seed, seed_df, row_index)

    # If there were no download errors (the dataframe still has no value in that cell), updates the log to show success.
    if pd.isnull(seed_df.at[row_index, "Metadata_Report_Errors"]):
        seed_df.loc[row_index, "Metadata_Report_Errors"] = "Successfully downloaded all metadata reports"
        seed_df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)

    # If there were no deleted empty reports (the dataframe still has not value in that cell), updates the log.
    if pd.isnull(seed_df.at[row_index, "Metadata_Report_Empty"]):
        seed_df.loc[row_index, "Metadata_Report_Empty"] = "No empty reports"
        seed_df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)


def download_warcs(seed, row_index, seed_df):
    """Downloads every WARC file and verifies that fixity is unchanged after downloading.
    Unzips each WARC."""

    # Makes a list of the filenames for all WARCs for this seed.
    warc_names = seed.WARC_Filenames.split("|")

    # Downloads and validates every WARC.
    # If an error is caught at any point, logs the error and starts the next WARC.
    for warc in warc_names:

        # The path for where the WARC will be saved on the local machine.
        warc_path = f'{c.script_output}/preservation_download/{seed.Seed_ID}/{warc}'

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
        unzip_warc(seed_df, row_index, warc_path, warc, seed.Seed_ID)

        # Waits 15 second to give the API a rest.
        time.sleep(15)


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
        log(f"API Error {warc_download.status_code}: can't download {warc}",
            seed_df, row_index, "WARC_API_Errors")
        raise ValueError

    # Saves the zipped WARC in the seed folder, keeping the original filename.
    with open(warc_path, 'wb') as warc_file:
        warc_file.write(warc_download.content)


def get_warc_info(warc, seed_df, row_index):
    """
    Gets and returns the URL for downloading the WARC and WARC MD5 from Archive-It using WASAPI.
    """

    # WASAPI call to get all data related to this WARC.
    warc_data = requests.get(f'{c.wasapi}?filename={warc}', auth=(c.username, c.password))

    # If there is an API error, updates the log and raises an error to skip the rest of the steps for this WARC.
    if not warc_data.status_code == 200:
        log(f"API Error {warc_data.status_code}: can't get info about {warc}",
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


def metadata_csv(seeds_list, date_end):
    """
    Makes a spreadsheet named metadata.csv in the preservation_download folder
    using the Partner API to get the seed reports.
    This spreadsheet is required by the general-aip.py script.
    """

    # Makes a dataframe for storing all the seed data.
    df = pd.DataFrame(columns=["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"])

    # Gets the data from the Archive-It seed report for each seed on the list.
    # Each seed will be one row in the df and CSV.
    for seed_id in seeds_list:

        row_list = []

        # Uses the Partner API to get the seed report.
        # If the connection fails, logs an error and adds a row to the df so it is clear more work is needed.
        api_result = requests.get(f"{c.partner_api}/seed?id={seed_id}", auth=(c.username, c.password))
        if not api_result.status_code == 200:
            row_list = [f"TBD: API error {api_result.status_code}", "TBD", seed_id, "TBD", "TBD", 1]
            df.loc[len(df)] = row_list
            continue
        seed_report = api_result.json()

        # Adds the department code, which is based on Collector from the seed report.
        # Supplies a default value if the collector is not an expected value so it is clear more work is needed.
        try:
            collector = seed_report[0]['metadata']['Collector'][0]['value']
            collector_to_dept = {"Hargrett Rare Book & Manuscript Library": "hargrett",
                                 "Map and Government Information Library": "magil",
                                 "Richard B. Russell Library for Political Research and Studies": "russell"}
            department = collector_to_dept.get(collector, "TBD: unexpected collector value")
        except (KeyError, IndexError):
            department = "TBD: no collector in Archive-It"
        row_list.append(department)

        # Adds the related archival collection number, which is based on Relation from the seed report.
        # Regular expressions are used to extract the number from the relation information if present,
        # and otherwise a department-specific default value is supplied.
        if department == "hargrett":
            try:
                relation = seed_report[0]['metadata']['Relation'][0]['value']
                collection_id = re.match("^Hargrett (.*):", relation)[1]
                collection = f"harg-{collection_id}"
            except (KeyError, AttributeError):
                collection = "harg-0000"
        elif department == "magil":
            collection = "magil-0000"
        elif department == "russell":
            try:
                relation = seed_report[0]['metadata']['Relation'][0]['value']
                collection_id = re.match("^RBRL/(\d{3})", relation)[1]
                collection = f"rbrl-{collection_id}"
            except (KeyError, AttributeError):
                collection = "rbrl-000"
        else:
            collection = "TBD: unexpected department value"
        row_list.append(collection)

        # Adds the folder with the contents to be made into AIPs, which is the seed_id.
        row_list.append(seed_id)

        # Adds a placeholder for the AIP_ID, which will be made once all the seed report data is in the dataframe.
        row_list.append("AIP_ID TBD")

        # Adds the title, which is Title from the seed report, unless that fields is missing.
        try:
            row_list.append(seed_report[0]['metadata']['Title'][0]['value'])
        except (KeyError, IndexError):
            row_list.append("TBD: could not get title from Archive-It")

        # Adds the version number, which is always 1 for web preservation downloads.
        # Each download is considered a new AIP, even if other WARCs were downloaded for that seed previously,
        # since the WARCs are new.
        row_list.append("1")

        # Adds the completed row of information available in the seed report (everything by AIP ID)
        # to the end of the dataframe.
        df.loc[len(df)] = row_list

    # Calculates the AIP_ID for each seed and adds it to the dataframe.
    # Identifiers are department-specific and may use collection, download date, and a sequential number.
    # The sequential number (number of seeds in a collection) is temporarily added to the dataframe.
    # IDs with different patterns are made in one dataframe per pattern, before combining them back together.

    year, month, day = date_end.split("-")
    df['Sequential'] = df.groupby('Collection').cumcount() + 1
    df['Sequential'] = df['Sequential'].astype(str).str.zfill(4)

    df_magil = df[df['Department'] == 'magil'].copy()
    df_magil['AIP_ID'] = "magil-ggp-" + df_magil['Folder'] + "-" + year + "-" + month

    df_harg_rbrl = df[df['Department'].isin(['hargrett', 'russell'])].copy()
    df_harg_rbrl['AIP_ID'] = df_harg_rbrl['Collection'] + "-web-" + year + month + "-" + df_harg_rbrl['Sequential']

    df_tbd = df[df['Department'].str.startswith("TBD")].copy()
    df_tbd['AIP_ID'] = "TBD"

    # Combines the department dataframes, removes the temporary column Sequential,
    # and saves the completed dataframe to a CSV.
    df = pd.concat([df_magil, df_harg_rbrl, df_tbd])
    df = df.drop(['Sequential'], axis=1)
    df.to_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"), index=False)

    # Returns a dataframe with the Seed ID (Folder) and AIP ID so the AIP ID can be added to seed_df.
    aip_df = df[['Folder', 'AIP_ID']].copy()
    aip_df.rename(columns={"Folder": "Seed_ID"}, inplace=True)
    return aip_df


def redact_seed_report(seed_id, aip_id, seed_df, row_index):
    """
    Replaces the seed report with a redacted version of the file, removing login information if those columns
    are present. Even if the columns are blank, replaces it with REDACTED. Since not all login information is
    meaningful (some is from staff web browsers autofill information while viewing the metadata), knowing if
    there was login information or not is misleading.
    """

    # Reads the seeds.csv into a dataframe for editing.
    # If it is not present, logs the error and ends this function.
    try:
        report_df = pd.read_csv(f"{seed_id}/{aip_id}_seed.csv")
    except FileNotFoundError:
        log("No seeds.csv to redact", seed_df, row_index, "Seed_Report_Redaction")
        return

    # If the login columns exist, replaces the values with REDACTED and updates the log.
    # If they do not exist, just updates the log.
    if "login_password" in report_df.columns:
        report_df["login_username"] = "REDACTED"
        report_df["login_password"] = "REDACTED"
        report_df.to_csv(f"{seed_id}/{aip_id}_seed.csv", index=False)
        log("Successfully redacted", seed_df, row_index, "Seed_Report_Redaction")
    else:
        log("No login columns to redact", seed_df, row_index, "Seed_Report_Redaction")


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
    coll_by_seed = coll_df.groupby(['Seed_ID'])['AIT_Collection'].apply('|'.join)

    job_df = warc_df[['Seed_ID', 'Job_ID']].copy()
    job_df = job_df.drop_duplicates()
    job_df['Job_ID'] = job_df['Job_ID'].astype(str)
    jobs_by_seed = job_df.groupby(['Seed_ID'])['Job_ID'].apply('|'.join)

    warc_df['Size_GB'] = warc_df['Size']/1000000000
    gb_by_seed = warc_df.groupby(['Seed_ID'])['Size_GB'].sum().round(3)

    count_by_seed = warc_df.groupby('Seed_ID')['Seed_ID'].count()

    warc_names = warc_df.groupby(['Seed_ID'])['WARC_Filename'].apply('|'.join)

    seed_df = pd.concat([coll_by_seed, jobs_by_seed, gb_by_seed, count_by_seed, warc_names], axis=1)
    seed_df.columns = ["AIT_Collection", "Job_ID", "Size_GB", 'WARCs', "WARC_Filenames"]
    seed_df = seed_df.reset_index()

    # Adds columns for logging the workflow steps.
    log_columns = ["Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction", "WARC_API_Errors",
                   "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"]
    seed_df = seed_df.reindex(columns=seed_df.columns.tolist() + log_columns)

    # Saves the dataframe as a CSV in the script output folder for splitting or restarting a batch.
    # Returns the dataframe for when the entire group will be downloaded as one batch.
    seed_df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)
    return seed_df


def unzip_warc(seed_df, row_index, warc_path, warc, seed_id):
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
        if os.path.exists(f'{c.script_output}/preservation_download/{seed_id}/{warc}.open'):
            os.remove(f'{c.script_output}/preservation_download/{seed_id}/{warc}.open')
            log(f"Error unzipping {warc}: unzipped to '.gz.open' file", seed_df, row_index, "WARC_Unzip_Errors")
        # If it unzipped correctly, deletes the zip file.
        else:
            os.remove(warc_path)
            log(f"Successfully unzipped {warc}", seed_df, row_index, "WARC_Unzip_Errors")
    else:
        log(f"Error unzipping {warc}: {unzip_output.stderr.decode('utf-8')}",
            seed_df, row_index, "WARC_Unzip_Errors")


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
        log(f"Error: fixity for {warc} cannot be extracted from md5deep output: {md5deep_output.stdout}",
            seed_df, row_index, "WARC_Fixity_Errors")
        raise AttributeError

    # Compares the md5 of the downloaded zipped WARC to Archive-It metadata.
    # If the md5 has changed, deletes the WARC.
    if warc_md5 == downloaded_warc_md5:
        log(f"Successfully verified {warc} fixity on {datetime.datetime.now()}",
            seed_df, row_index, "WARC_Fixity_Errors")
    else:
        os.remove(warc_path)
        log(f"Error: fixity for {warc} changed and it was deleted: {warc_md5} before, {downloaded_warc_md5} after",
            seed_df, row_index, "WARC_Fixity_Errors")
        raise ValueError
