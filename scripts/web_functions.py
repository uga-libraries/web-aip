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
import subprocess

# Import constant variables and functions from another UGA preservation script.
import aip_functions as a
import configuration as c


def seed_data(date_start, date_end):
    """Uses WASAPI and the Partner API to get information about each seed to include in the download.
    Returns the data as a dataframe and also saves it to a CSV in the script output folder to use for the log
    and for splitting big downloads or restarting jobs if the script breaks."""

    # Starts a dataframe for storing seed level data about the WARCs in this download.
    # Includes columns that will be used for logging steps prior to using the general-aip script functions.
    seed_df = pd.DataFrame(columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection",
                                    "Job_ID", "Size_GB", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                    "Metadata_Report_Errors", "Metadata_Report_Info",
                                    "WARC_API_Errors", "WARC_Fixity_Errors"])

    # Uses WASAPI to get information about all WARCs in this download, based on the date limits.
    # WASAPI is the only API that allows limiting by date.
    filters = {"store-time-after": date_start, "store-time-before": date_end, "page_size": 1000}
    warcs = requests.get(c.wasapi, params=filters, auth=(c.username, c.password))

    # If there was an error with the API call, quits the script.
    if not warcs.status_code == 200:
        print(f"\nAPI error {warcs.status_code} when getting WARC data.")
        print(f"Ending script (this information is required). Try script again later.")
        exit()

    # Converts the WARC data from json to a Python object and organizes it in the dataframe by seed.
    # Includes seed, Archive-It collection, job, size in GB, number of WARCs, and list of WARC filenames.
    py_warcs = warcs.json()
    for warc_info in py_warcs["files"]:

        # Calculates the seed id, which is a portion of the WARC filename.
        regex_seed = re.match(r"^.*-SEED(\d+)-", warc_info["filename"])
        seed_identifier = regex_seed.group(1)

        # If the seed is already in the dataframe, adds to the size, WARC count, and WARC filenames.
        # If the seed is new, gets the data needed about the seed and adds it to the dataframe.
        if seed_identifier in seed_df.Seed_ID.values:
            seed_df.loc[seed_df.Seed_ID == seed_identifier, "Size_GB"] += round(warc_info["size"]/1000000000, 2)
            seed_df.loc[seed_df.Seed_ID == seed_identifier, "WARCs"] += 1
            seed_df.loc[seed_df.Seed_ID == seed_identifier, "WARC_Filenames"] += f',{warc_info["filename"]}'
        else:
            seed_info = {"Seed_ID": seed_identifier, "AIT_Collection": warc_info["collection"],
                         "Job_ID": warc_info["crawl"], "Size_GB": round(warc_info["size"]/1000000000, 2),
                         "WARCs": 1, "WARC_Filenames": warc_info["filename"]}
            seed_df = seed_df.append(seed_info, ignore_index=True)

    # Gets a list of the seeds and uses the Partner API (seed report) to get additional information.
    # Includes: seed title, related archival collection, and department (all part of the metadata)
    # and constructs the AIP ID. Adds these fields to the dataframe.
    seed_list = seed_df["Seed_ID"].to_list()
    for seed in seed_list:

        # Row index is used to save the data to the correct row in the dataframe.
        row_index = seed_df.index[seed_df["Seed_ID"] == seed].tolist()[0]

        # Gets the seed metadata from the Archive-It Partner API and logs an error if the connection fails.
        seed_report = requests.get(f"{c.partner_api}/seed?id={seed}", auth=(c.username, c.password))
        if not seed_report.status_code == 200:
            log(f"API error {seed_report.status_code}", seed_df, row_index, "Seed_Metadata_Errors")
            continue
        py_seed_report = seed_report.json()

        # Gets the AIP title from the seed metadata.
        seed_df.loc[row_index, "Title"] = py_seed_report[0]["metadata"]["Title"][0]["value"]

        # Gets the department name from the seed metadata and translates it to the ARCHive group code.
        dept_to_group = {"Hargrett Rare Book & Manuscript Library": "hargrett",
                         "Map and Government Information Library": "magil",
                         "Richard B. Russell Library for Political Research and Studies": "russell"}
        department = dept_to_group[py_seed_report[0]["metadata"]["Collector"][0]["value"]]
        seed_df.loc[row_index, "Department"] = department

        # Gets the related archival collection from the seed metadata, reformatting if necessary.
        if department == "hargrett":
            try:
                regex_collection = re.match("^Hargrett (.*):", py_seed_report[0]["metadata"]["Relation"][0]["value"])
                seed_df.loc[row_index, "UGA_Collection"] = regex_collection.group(1)
            except (KeyError, AttributeError):
                seed_df.loc[row_index, "UGA_Collection"] = "0000"
        elif department == "magil":
            seed_df.loc[row_index, "UGA_Collection"] = "0000"
        elif department == "russell":
            try:
                regex_collection = re.match(r"^RBRL/(\d{3})", py_seed_report[0]["metadata"]["Relation"][0]["value"])
                seed_df.loc[row_index, "UGA_Collection"] = "rbrl-" + regex_collection.group(1)
            except (KeyError, AttributeError):
                seed_df.loc[row_index, "UGA_Collection"] = "rbrl-000"

        # Constructs the AIP ID according to department specifications.
        # Also temporarily adds a column to the dataframe which counts the number of seeds per collection,
        # and calculates the year and month of the download, both of which are needed for AIP IDs.
        seed_df["coll_sequence"] = seed_df.groupby(["UGA_Collection"]).cumcount() + 1
        year, month, day = date_end.split("-")
        if department == "hargrett":
            seed_df.loc[row_index, "AIP_ID"] = f'harg-{seed_df.at[row_index, "UGA_Collection"]}-web-{year}{month}-{format(seed_df.at[row_index, "coll_sequence"], "04d")}'
        elif department == "magil":
            seed_df.loc[row_index, "AIP_ID"] = f"magil-ggp-{seed}-{year}-{month}"
        elif department == "russell":
            seed_df.loc[row_index, "AIP_ID"] = f'{seed_df.at[row_index, "UGA_Collection"]}-web-{year}{month}-{format(seed_df.at[row_index, "coll_sequence"], "04d")}'
        seed_df.drop(["coll_sequence"], axis=1, inplace=True)

    # Saves the dataframe as a CSV in the script output folder for splitting or restarting a batch.
    # Returns the dataframe for when the entire group will be downloaded as one batch.
    seed_df.to_csv(os.path.join(c.script_output, "seeds.csv"), index=False)
    return seed_df


def log(message, df, row, column):
    """Adds log information to the seeds dataframe and saves an updated version of seeds.csv."""

    # Updates the dataframe. Separates messages with a a semicolon if there is more than one.
    if pd.isnull(df.at[row, column]):
        df.loc[row, column] = message
    else:
        df.loc[row, column] += "; " + message

    # Saves a new version of seeds.csv with the updated information.
    # The previous version of the file is overwritten.
    df.to_csv(os.path.join(c.script_output, "seeds.csv"), index=False)


def download_metadata(seed, date_end, seed_df):
    """Uses the Partner API to download six metadata reports to include in the AIPs for archived websites,
    deletes any empty reports (meaning there was no data of that type for this seed),
    and redacts login information from the seed report.
    Any errors are added to the seed dataframe and saved to the script log at the end of the function."""

    def get_report(filter_type, filter_value, report_type, report_name):
        """Downloads a single metadata report and saves it as a csv in the AIP's metadata folder.
            filter_type and filter_value are used to filter the API call to the right AIP's report
            report_type is the Archive-It name for the report
            report_name is the name for the report saved in the AIP, including the ARCHive metadata code """

        # Builds the API call to get the report as a csv.
        # Limit of -1 will return all matches. Default is only the first 100.
        filters = {'limit': -1, filter_type: filter_value, 'format': 'csv'}
        metadata_report = requests.get(f'{c.partner_api}/{report_type}', params=filters, auth=(c.username, c.password))

        # Saves the metadata report if there were no errors with the API or logs the error.
        if metadata_report.status_code == 200:
            with open(f'{seed.Seed_ID}/metadata/{report_name}', 'wb') as report_csv:
                report_csv.write(metadata_report.content)
        else:
            log(f"{report_type} API error {metadata_report.status_code}", seed_df, row_index, "Metadata_Report_Errors")

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
                log("Seed report does not have login columns to redact.", seed_df, row_index, "Metadata_Report_Info")
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

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # Downloads five of the six metadata reports from Archive-It needed to understand the context of the WARC.
    # These are reports where there is only one report per seed or collection.
    get_report('id', seed.Seed_ID, 'seed', f'{seed.Seed_ID}_seed.csv')
    get_report('seed', seed.Seed_ID, 'scope_rule', f'{seed.Seed_ID}_seedscope.csv')
    get_report('collection', seed.AIT_Collection, 'scope_rule', f'{seed.Seed_ID}_collscope.csv')
    get_report('id', seed.AIT_Collection, 'collection', f'{seed.Seed_ID}_coll.csv')
    get_report('id', seed.Job_ID, 'crawl_job', f'{seed.Seed_ID}_{seed.Job_ID}_crawljob.csv')

    # Downloads the crawl definition report for the job this WARC was part of.
    # The crawl definition id is obtained from the crawl job report using the job id.
    # There may be more than one crawl definition report per AIP.
    # Logs an error if there is no crawl job report to get the job id(s) from.
    try:
        with open(f'{seed.Seed_ID}/metadata/{seed.Seed_ID}_{seed.Job_ID}_crawljob.csv', 'r') as crawljob_csv:
            crawljob_data = csv.DictReader(crawljob_csv)
            for job in crawljob_data:
                if str(seed.Job_ID) == job['id']:
                    crawl_def_id = job['crawl_definition']
                    get_report('id', crawl_def_id, 'crawl_definition', f'{seed.Seed_ID}_{crawl_def_id}_crawldef.csv')
                    break
    except FileNotFoundError:
        log("Crawl Job was not downloaded so cannot get Crawl Definition.", seed_df, row_index, "Metadata_Report_Errors")

    # If there were no download errors (the dataframe still has no value in that cell), updates the log to show success.
    if pd.isnull(seed_df.at[row_index, "Metadata_Report_Errors"]):
        seed_df.loc[row_index, "Metadata_Report_Errors"] = "Successfully downloaded all metadata reports."
        seed_df.to_csv(os.path.join(c.script_output, "seeds.csv"), index=False)

    # Iterates over each report in the metadata folder to delete empty reports and redact login information from the
    # seed report.
    for report in os.listdir(f'{seed.Seed_ID}/metadata'):

        # Saves the full file path of the report.
        report_path = f'{c.script_output}/aips_{date_end}/{seed.Seed_ID}/metadata/{report}'

        # Deletes any empty metadata files (file size of 0) and begins processing the next file. A file is empty if
        # there is no metadata of that type, which is most common for collection and seed scope reports.
        if os.path.getsize(report_path) == 0:
            os.remove(report_path)
            log(f"Deleted empty report {report}", seed_df, row_index, "Metadata_Report_Info")
            continue

        # Redacts login password and username from the seed report so we can share the seed report with researchers.
        if report.endswith('_seed.csv'):
            redact(report_path)


def download_warcs(seed, date_end, seed_df):
    """Downloads every WARC file and verifies that fixity is unchanged after downloading."""

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # Makes a list of the filenames for all WARCs for this seed.
    warc_names = seed.WARC_Filenames.split(",")

    # Downloads and validates every WARC.
    for warc in warc_names:

        # Gets URL for downloading the WARC and WARC MD5 from Archive-It using WASAPI.
        warc_data = requests.get(f'{c.wasapi}?filename={warc}', auth=(c.username, c.password))
        if not warc_data.status_code == 200:
            log(f"API error {warc_data.status_code}: can't get info about warc {warc}.", seed_df, row_index, "WARC_API_Errors")
            return
        py_warc = warc_data.json()
        warc_url = py_warc["files"][0]["locations"][0]
        warc_md5 = py_warc["files"][0]["checksums"]["md5"]

        # The path for where the warc will be saved on the local machine (it is long and used twice in this script).
        warc_path = f'{c.script_output}/aips_{date_end}/{seed.Seed_ID}/objects/{warc}'

        # TEMPORARY CODE TO SPEED UP TESTING
        # This will make a file of the correct name in the objects folder instead of downloading.
        with open(warc_path, "w") as file:
            file.write("Text")

        # # Downloads the warc.
        # warc_download = requests.get(f"{warc_url}", auth=(c.username, c.password))
        #
        # # If there was an error with the API call, quits the function.
        # if not warc_download.status_code == 200:
        #     log(f"API error {warc_download.status_code}: can't download warc {warc}", seed_df, row_index, "WARC_API_Errors")
        #     return
        # else:
        #     log(f"WARC {warc} successfully downloaded", seed_df, row_index, "WARC_API_Errors")
        #
        # # Saves the warc in the objects folder, keeping the original filename.
        # with open(warc_path, 'wb') as warc_file:
        #     warc_file.write(warc_download.content)
        #
        # # Calculates the md5 for the downloaded WARC, using a regular expression to get the md5 from the md5deep output.
        # # If the output is not formatted as expected, quits the function.
        # md5deep_output = subprocess.run(f'"{c.MD5DEEP}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)
        # try:
        #     regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(md5deep_output.stdout))
        #     downloaded_warc_md5 = regex_md5.group(1)
        # except AttributeError:
        #     log(f"Fixity for {warc} cannot be extracted from md5deep output: {md5deep_output.stdout}",
        #         seed_df, row_index, "WARC_Fixity_Errors")
        #     return
        #
        # # Compares the md5 of the download warc to what Archive-It has for the warc (warc_md5). If the md5 has changed,
        # # deletes the WARC so the check for AIP completeness will catch that there was a problem.
        # if not warc_md5 == downloaded_warc_md5:
        #     os.remove(warc_path)
        #     log(f"Fixity for WARC {warc} changed and it deleted: {warc_md5} before, {downloaded_warc_md5} after",
        #         seed_df, row_index, "WARC_Fixity_Errors")
        # else:
        #     log(f"Successfully verified WARC {warc} fixity on {datetime.datetime.now()}", seed_df, row_index, "WARC_Fixity_Errors")


def check_directory(aip):
    """Identifies any AIPs with missing or empty objects or metadata folders and moves them to an error folder."""

    # Checks if the objects folder doesn't exist or is empty.
    # Moves to an error folder if either issue is detected and logs the result.
    if not os.path.exists(f"{aip.directory}/{aip.id}/objects"):
        aip.log["ObjectsError"] = "Objects folder is missing."
        a.log(aip.log)
        a.move_error('incomplete_directory', aip.id)
        return
    elif len(os.listdir(f"{aip.directory}/{aip.id}/objects")) == 0:
        aip.log["ObjectsError"] = "Objects folder is empty."
        a.log(aip.log)
        a.move_error('incomplete_directory', aip.id)
        return
    else:
        aip.log["ObjectsError"] = "Successfully created objects folder"

    # Checks if the metadata folder doesn't exist or is empty.
    # Moves to an error folder if either issue is detected and logs the result.
    if not os.path.exists(f"{aip.directory}/{aip.id}/metadata"):
        aip.log["MetadataError"] = "Metadata folder is missing."
        a.log(aip.log)
        a.move_error('incomplete_directory', aip.id)
        return
    elif len(os.listdir(f"{aip.directory}/{aip.id}/metadata")) == 0:
        aip.log["MetadataError"] = "Metadata folder is empty."
        a.log(aip.log)
        a.move_error('incomplete_directory', aip.id)
        return
    else:
        aip.log["MetadataError"] = "Successfully created metadata folder"


def check_aips(date_end, date_start, seed_to_aip, aips_directory):
    """Verifies that all the expected AIPs for the download are complete and no unexpected AIPs were created.
    Produces a csv named completeness_check with the results in the AIPs directory. """

    def aip_dictionary():
        """Uses the Archive-It APIs and Python filters to gather information about the expected AIPs. Using Python
        instead of the API to filter the results for a more independent analysis of expected AIPs. All WARC
        information is downloaded, filtered with Python to those expected in this preservation download, and the WARC
        information is aggregated into a dictionary organized by seed/AIP. The key is the seed id and the values are
        the AIP id, warc count, and url. """

        # Downloads the entire WARC list.
        filters = {'page_size': 1000}
        warcs = requests.get(c.wasapi, params=filters, auth=(c.username, c.password))

        # If there was an API error, ends the function.
        if warcs.status_code != 200:
            print("WASAPI Status code:", warcs.status_code)
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
                raise ValueError

            # Filter one: only includes the WARC in the dictionary if it was created since the last download and
            # before the current download. Store time is used so test crawls are evaluated based on the date they
            # were saved. Simplifies the date format to YYYY-MM-DD by removing the time information before comparing
            # it to the last download date.
            try:
                regex_crawl_date = re.match(r"(\d{4}-\d{2}-\d{2})T.*", warc_info['store-time'])
                crawl_date = regex_crawl_date.group(1)
            except AttributeError:
                raise ValueError

            if crawl_date < date_start or crawl_date > date_end:
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
                    print(f'API error {seed_report.status_code} getting seed report for seed {seed_identifier}.')
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
                # seed_to_aip, it is an unexpected seed and cannot be added to the dictionary.
                try:
                    aip_info[seed_identifier] = [seed_to_aip[seed_identifier], 1, json_seed[0]['url']]
                #TODO: do something with the errors.
                except (KeyError, IndexError):
                    pass

                warcs_include += 1

        # Checks that the right number of WARCs were evaluated.
        if warcs_expected != warcs_include + warcs_exclude:
            print('Check AIPs did not review the expected number of WARCs.')
            raise ValueError

        return aip_info

    def check_completeness(aip_id, warc_total, website_url):
        """Verifies a single AIP is complete, checking the contents of the metadata and objects folders. Returns a
        list with the results ready to be added as a row to the completeness check csv. """

        # Starts a list for the results, with the AIP id and website url to use for identification of the AIP.
        result = [aip_id, website_url]

        # Tests if there is a folder for this AIP in the AIPs directory. If not, returns the result for this AIP and
        # does not run the rest of the function's tests since there is no directory to check for completeness.
        if any(folder.startswith(aip_id) for folder in os.listdir(aips_directory)):
            result.append(True)
        else:
            result.extend([False, 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a'])
            return result

        # Saves the file paths to the metadata and objects folders to variables, since they are long and reused.
        objects = f'{aips_directory}/{aip_id}_bag/data/objects'
        metadata = f'{aips_directory}/{aip_id}_bag/data/metadata'

        # Tests if each of the four Archive-It metadata reports that never repeat are present.
        # os.path.exists() returns True/False.
        result.append(os.path.exists(f'{metadata}/{aip_id}_coll.csv'))
        result.append(os.path.exists(f'{metadata}/{aip_id}_collscope.csv'))
        result.append(os.path.exists(f'{metadata}/{aip_id}_seed.csv'))
        result.append(os.path.exists(f'{metadata}/{aip_id}_seedscope.csv'))

        # Counts the number of instances of the two Archive-It metadata reports than can repeat.
        # Compare to expected results in the WARC inventory.
        result.append(len([file for file in os.listdir(metadata) if file.endswith('_crawldef.csv')]))
        result.append(len([file for file in os.listdir(metadata) if file.endswith('_crawljob.csv')]))

        # Tests if the preservation.xml file is present.
        result.append(os.path.exists(f'{metadata}/{aip_id}_preservation.xml'))

        # Tests if the number of WARCs is correct. Compares the number of WARCs in the objects folder, calculated
        # with len(), to the number of WARCs expected from the API (warc_count).
        warcs = len([file for file in os.listdir(objects) if file.endswith('.warc.gz')])
        if warcs == warc_total:
            result.append(True)
        else:
            result.append(False)

        # Tests if everything in the AIP's objects folder is a WARC. Starts with a value of True. If there is a file
        # of a different format, based on the file extension, it updates the value to False.
        result.append(True)
        for file in os.listdir(objects):
            if not file.endswith('.warc.gz'):
                result[-1] = False

        # Tests if the number of FITS files is correct (one for each WARC). Compares the number of FITS files in the
        # metadata folder, calculated with len(), to the number of WARCs in the objects folder, which was calculated
        # earlier in this function.
        fits = len([file for file in os.listdir(metadata) if file.endswith('_fits.xml')])
        if fits == warcs:
            result.append(True)
        else:
            result.append(False)

        # Tests if everything in the AIP's metadata folder is an expected file type. Starts with a value of True. If
        # there is a file of a different type, based on the end of the filename, it updates the value to False.
        result.append(True)
        expected_endings = ('_coll.csv', '_collscope.csv', '_crawldef.csv', '_crawljob.csv', '_seed.csv',
                            '_seedscope.csv', '_preservation.xml', '_fits.xml', '_del.csv')
        for file in os.listdir(metadata):
            if not file.endswith(expected_endings):
                result[-1] = False

        return result

    def check_for_extra_aips():
        """Looks for AIPs that were created during the last download but were not expected based on the API data. If
        any are found, returns a list with the results ready to be added as a row to the results csv."""

        # Starts a list for the results. The list elements will be one list per unexpected AIP.
        extras = []

        # Iterates through the folder with the AIPs.
        for aip_directory in os.listdir(aips_directory):

            # Skips the metadata.csv used to make the AIPs.
            if aip_directory == "metadata.csv":
                continue

            # Creates a tuple of the expected AIPs, which are the values in the seed_to_aip dictionary generated
            # earlier in the script.
            expected_aip_ids = tuple(seed_to_aip.values())

            # If there is an AIP that does not start with one of the expected AIP ids, adds a list with the values
            # for that AIP's row in the completeness check csv to the extras list.
            if not aip_directory.startswith(expected_aip_ids):
                extras.append([aip_directory, 'n/a', 'Not expected', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a',
                               'n/a', 'n/a', 'n/a', 'n/a'])

        # Only returns the extras list if at least one unexpected AIP was found.
        if len(extras) > 0:
            return extras

    # Makes a dictionary with information about expected AIPs for this download. The key is the seed id,
    # and the value is a list with the AIP id, WARC count, and URL. If there were errors when calculating the
    # dictionary, ends the function.
    try:
        aips_metadata = aip_dictionary()
    except ValueError:
        print('\nUnable to check AIPs for completeness. AIP dictionary not generated.')
        return

    # Starts a csv for the results of the quality review.
    csv_path = f'{c.script_output}/completeness_check.csv'
    with open(csv_path, 'w', newline='') as complete_csv:
        complete_write = csv.writer(complete_csv)

        # Adds a header row to the csv.
        complete_write.writerow(
            ['AIP', 'URL', 'AIP Folder Made', 'coll.csv', 'collscope.csv', 'seed.csv',
             'seedscope.csv', 'crawldef.csv count', 'crawljob.csv count', 'preservation.xml', 'WARC Count Correct',
             'Objects is all WARCs', 'fits.xml Count Correct', 'No Extra Metadata'])

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
