"""Purpose: functions used by the preservation scripts for web content in Archive-It.

Dependencies:
    * Python library: requests
    * Tool: md5deep
"""

import csv
import datetime
import os
import re
import requests
import subprocess

# Import constant variables and functions from another UGA preservation script.
import aip_functions as a
import configuration as c


def warc_log(log_data):
    """Saves information about each step done on a WARC to a CSV file, warc_log.csv, in the script output folder.
    Information is saved to a dictionary after each step and saved to the log after the end of WARC processing
    or if there is an error. The AIP steps use a different log structure."""

    # Formats the information from log_data as a list for adding to the CSV.
    # If it is header, it uses default values. Otherwise, log_data is a dictionary with known keys.
    if log_data == "header":
        log_row = ["WARC Filename", "WARC JSON Error", "Seed ID Error", "JOB ID Error", "Seed Metadata Error",
                   "Metadata Report Errors", "Metadata Report Information", "WARC Download API Error",
                   "WARC Fixity Error", "Processing Complete"]
    else:
        log_row = [log_data["filename"], log_data["warc_json"], log_data["seed_id"], log_data["job_id"],
                   log_data["seed_metadata"], log_data["report_download"], log_data["report_info"],
                   log_data["warc_api"], log_data["warc_fixity"], log_data["complete"]]

    # Saves the log_row information to a row in the WARC log CSV.
    with open("../warc_log.csv", "a", newline="") as log_file:
        log_writer = csv.writer(log_file)
        log_writer.writerow(log_row)


def warc_data(date_start, date_end, collections=None):
    """Gets data about WARCs to include in this download using WASAPI. A WARC is included if it was saved in the 3
    months since the last preservation download date and is part of a relevant collection. The relevant collection
    list is either provided as an argument or the function will calculate a list of departments who regularly use
    this script.

    Returns json converted to a Python object with all the WASAPI data on the included WARCs."""

    def collection_list():
        """Makes a list of Hargrett, MAGIL, and Russell Archive-It collections. Skips test collections and
        collections of other departments (e.g., BMA) who occasionally crawl but don't save to ARCHive."""

        # Starts lists to store the collections to include and exclude, depending on the department.
        collections_include = []
        collections_exclude = []

        # Uses the Partner API to get the data from the seed report, which has repository (department) information.
        # Limit of -1 means it will get data on all seeds, no matter how many there are.
        seed_reports = requests.get(f'{c.partner_api}/seed?limit=-1', auth=(c.username, c.password))

        # If there was an error with the API call, quits the script.
        if not seed_reports.status_code == 200:
            print(f"API error {seed_reports.status_code} when making collection list.")
            print(f"Ending script (this information is required). Try script again later.")
            exit()

        # Converts the seed data from json to a Python object.
        py_seeds = seed_reports.json()

        # Iterates over data about each seed.
        for seed in py_seeds:

            # Gets the collection id. If it has already been evaluated, stops processing this seed and starts the next.
            # Collections may have multiple seeds.
            collection_id = seed['collection']
            if collection_id in collections_include or collection_id in collections_exclude:
                continue

            # Gets the department name from the Collector field. If there is no information in the field, adds it to
            # the excluded list and to the log (so staff can verify it was correctly excluded) and starts the next seed.
            try:
                department_name = seed['metadata']['Collector'][0]['value']
            except (KeyError, IndexError):
                collections_exclude.append(collection_id)
                continue

            # If the department is Hargrett, MAGIL, or Russell, adds the collection id to the collections list.
            # Otherwise, adds it to the excluded list.
            if department_name.startswith(('Hargrett', 'Map', 'Richard B. Russell')):
                collections_include.append(seed['collection'])
            else:
                collections_exclude.append(collection_id)

        return collections_include

    # Gets data about WARCs to include in this download using WASAPI. Explanation of filters:
    #   * store time is when the WARC was saved, so test crawls saved in the quarter after they were run aren't missed.
    #   * collections are the Archive-It ids for collections to include in the download.
    #   * page size is the maximum number of WARCs the API call will return.
    if not collections:
        collections = collection_list()

    filters = {'store-time-after': date_start, 'store-time-before': date_end,
               'collection': collections, 'page_size': 1000}
    warcs = requests.get(c.wasapi, params=filters, auth=(c.username, c.password))

    # If there was an error with the API call, quits the script.
    if not warcs.status_code == 200:
        print(f'\nAPI error {warcs.status_code} when getting WARC data.')
        print(f"Ending script (this information is required). Try script again later.")
        exit()

    # Converts the WARC data from json to a Python object and returns that Python object.
    py_warcs = warcs.json()
    return py_warcs


def seed_data(py_warcs, date_end):
    """Extracts information from the warc and seed data to define the AIP id, AIP title, and crawl definition id.
    Returns this data in a dictionary with the seed id as the key and makes a metadata.csv file with seed metadata
    needed for making AIPs later."""

    # Starts a dictionary for the number of seeds per collection, which is used in the AIP id.
    seed_count = {}

    # Starts a dictionary for data about seeds included in this download.
    seeds_include = {}

    # Starts a list of seed ids for seeds that will not be included in this download so the script doesn't have to
    # check them again. There are often multiple warcs per seed.
    seeds_exclude = []

    # Gets the year, month, and day of the date_end. Year and month are used as part of AIP IDs.
    # date_end is the end of the preservation download period and formatted YYYY-MM-DD.
    year, month, day = date_end.split("-")

    # Makes a file named metadata.csv in the AIPs directory, with a header.
    metadata_open = open("metadata.csv", "a", newline="")
    metadata_csv = csv.writer(metadata_open)
    metadata_csv.writerow(["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"])

    # Iterates through data for each warc to get the seed ids which are included in this download. Those seed ids are
    # then used to look up information via the Partner API needed to generate the desired AIP information.
    for warc_info in py_warcs['files']:

        # Calculates the seed id, which is a portion of the warc filename.
        # Stops processing this warc and starts the next one if the filename doesn't match the expected pattern.
        try:
            regex_seed = re.match(r'^.*-SEED(\d+)-', warc_info['filename'])
            seed_identifier = regex_seed.group(1)
        except AttributeError:
            continue

        # Stops processing this warc and starts the next one if the script has already assigned an AIP id to this seed.
        # This happens when a seed has multiple warcs.
        if seed_identifier in seeds_include or int(seed_identifier) in seeds_exclude:
            continue

        # Uses the Partner API to get data about this seed.
        seed_report = requests.get(f'{c.partner_api}/seed?id={seed_identifier}', auth=(c.username, c.password))

        # If there was an error with the API call, quits the script.
        if not seed_report.status_code == 200:
            print(f"\nAPI error {seed_report.status_code} for seed report.")
            print(f"Ending script (this information is required). Try script again later.")
            exit()

        # Converts the seed data from json to a Python object.
        py_seed_report = seed_report.json()

        # Constructs the AIP id from the seed data. If at any stage a piece of the AIP id cannot be calculated,
        # the seed is added to the exclude list and the next warc is processed.
        for seed_info in py_seed_report:

            # Gets the title for the seed from the Title field.
            # Stops processing this seed if the title is missing. It is required.
            try:
                title = seed_info['metadata']['Title'][0]['value']
            except (KeyError, IndexError):
                seeds_exclude.append(seed_info['id'])
                continue

            # Gets the department from the Collector field.
            # Stops processing this seed if there is no department. It is required.
            try:
                department_name = seed_info['metadata']['Collector'][0]['value']
            except (KeyError, IndexError):
                seeds_exclude.append(seed_info['id'])
                continue

            # Constructs a Hargrett AIP ID: harg-collection-web-download_yearmonth-sequential_number.
            if department_name.startswith('Hargrett'):

                # Gets the related archival collection from Archive-it metadata.
                # If there is none, collection is '0000'.
                try:
                    regex_collection = re.match('^Hargrett (.*):', seed_info['metadata']['Relation'][0]['value'])
                    related_collection = regex_collection.group(1)
                except (KeyError, AttributeError):
                    related_collection = '0000'

                # Adds or updates the count for the number of AIPs from this collection in the seed_count dictionary.
                # Then gets the current count and formats it as a 4 digit number.
                seed_count[related_collection] = seed_count.get(related_collection, 0) + 1
                sequential_number = format(seed_count[related_collection], '04d')

                # Constructs the AIP id for the seed.
                identifier = f'harg-{related_collection}-web-{year}{month}-{sequential_number}'

            # Constructs a MAGIL AIP ID: magil-ggp-seed_id-download_year-download_month.
            elif department_name.startswith('Map'):
                identifier = f'magil-ggp-{seed_info["id"]}-{year}-{month}'
                related_collection = "0000"

            # Constructs a Russell AIP ID: rbrl-collection-web-download_yearmonth-sequential_number.
            elif department_name.startswith('Richard B. Russell'):

                # Gets the related archival collection from Archive-it metadata.
                # If there is none, collection is '000'.
                try:
                    regex_collection = re.match(r'^RBRL/(\d{3})', seed_info['metadata']['Relation'][0]['value'])
                    related_collection = regex_collection.group(1)
                except (KeyError, AttributeError):
                    related_collection = '000'

                # Adds or updates the count for the number of AIPs from this collection in the seed_count dictionary.
                # Then gets the current count and formats it as a 4 digit number.
                seed_count[related_collection] = seed_count.get(related_collection, 0) + 1
                sequential_number = format(seed_count[related_collection], '04d')

                # Constructs the AIP id for the seed.
                identifier = f'rbrl-{related_collection}-web-{year}{month}-{sequential_number}'

            # Stops processing this seed if the department isn't Hargrett, MAGIL, or Russell.
            # This shouldn't happen since the script is only processing seeds from these,
            # but there could have been an error in making the collections list.
            else:
                seeds_exclude.append(seed_info['id'])
                continue

            # Saves AIP id and AIP title to the seeds_include dictionary.
            # This only contains information about seeds that had no errors and were fully processed.
            seeds_include[seed_identifier] = [identifier, title]

            # Gets the ARCHive group for the department and saves AIp information to the metadata.csv
            dept_to_group = {"Hargrett Rare Book & Manuscript Library": "hargrett",
                             "Map and Government Information Library": "magil",
                             "Richard B. Russell Library for Political Research and Studies": "russell"}
            group = dept_to_group[department_name]
            metadata_csv.writerow([group, related_collection, identifier, identifier, title, 1])

    # Closes the metadata file and returns the dictionary with AIP ID and title.
    metadata_open.close()
    return seeds_include


def make_aip_directory(aip_folder):
    """Makes the AIP directory structure: a folder named with the AIP ID that contains folders named "metadata" and
    "objects", provided they are not already present from processing a previous WARC. """

    if not os.path.exists(f'{aip_folder}/metadata'):
        os.makedirs(f'{aip_folder}/metadata')

    if not os.path.exists(f'{aip_folder}/objects'):
        os.makedirs(f'{aip_folder}/objects')


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


def download_warc(aip_id, warc_filename, warc_url, warc_md5, date_end, log_data):
    """Downloads a warc file and verifies that fixity is unchanged after downloading."""

    # The path for where the warc will be saved on the local machine (it is long and used twice in this script).
    warc_path = f'{c.script_output}/aips_{date_end}/{aip_id}/objects/{warc_filename}'

    # Downloads the warc.
    warc_download = requests.get(f"{warc_url}", auth=(c.username, c.password))

    # If there was an error with the API call, quits the function.
    if not warc_download.status_code == 200:
        log_data["warc_api"] = f'API error {warc_download.status_code}'
        return
    else:
        log_data["warc_api"] = "Successfully downloaded WARC."

    # Saves the warc in the objects folder, keeping the original filename.
    with open(warc_path, 'wb') as warc_file:
        warc_file.write(warc_download.content)

    # Calculates the md5 for the downloaded WARC, using a regular expression to get the md5 from the md5deep output.
    # If the output is not formatted as expected, quits the function.
    md5deep_output = subprocess.run(f'"{c.MD5DEEP}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)
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
    else:
        log_data["warc_fixity"] = f"Successfully verified WARC fixity on {datetime.datetime.now()}"


def find_empty_directory():
    """Identifies any AIPs with empty objects or metadata folders and moves them to an error folder."""

    # Iterates through the aips directory.
    for root, folders, files in os.walk('.'):

        # Looks for empty metadata folders.
        if root.endswith('metadata') and len(os.listdir(root)) == 0:

            # Calculates the path of the parent folder (the aip folder).
            aip_path = os.path.dirname(root)

            # Prints the error and moves the aip to an error folder.
            print(f"\n{aip_path} metadata folder is empty. Moved to incomplete_directory error folder.")
            a.move_error('incomplete_directory', aip_path)

        # Looks for empty objects folders.
        if root.endswith('objects') and len(os.listdir(root)) == 0:

            # Calculates the path of the parent folder (the aip folder).
            aip_path = os.path.dirname(root)

            # Prints the error and moves the aip to an error folder.
            print(f"\n{aip_path} objects folder is empty. Moved to incomplete_directory error folder.")
            a.move_error('incomplete_directory', aip_path)


def check_aips(date_end, date_start, seed_to_aip):
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
        if any(folder.startswith(aip_id) for folder in os.listdir(f'{c.script_output}/aips_{date_end}')):
            result.append(True)
        else:
            result.extend([False, 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a'])
            return result

        # Saves the file paths to the metadata and objects folders to variables, since they are long and reused.
        objects = f'{c.script_output}/aips_{date_end}/{aip_id}_bag/data/objects'
        metadata = f'{c.script_output}/aips_{date_end}/{aip_id}_bag/data/metadata'

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
                            '_seedscope.csv', '_preservation.xml', '_fits.xml')
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
        for aip_directory in os.listdir(f'{c.script_output}/aips_{date_end}'):

            # Skips the csv made by the check_aips function.
            if aip_directory.startswith('completeness_check'):
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
    csv_path = f'{c.script_output}/completeness_check_{date_end}.csv'
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
