"""Purpose: functions and constants used by preservation scripts for web content in Archive-It.

Dependencies:
    * Python library: requests
    * Tool: md5deep
"""

import csv
import os
import re
import requests
import subprocess

# Import functions and constant variables from another UGA script.
import aip_functions as aip
import configuration as c


def warc_data(last_download, log_path, collections=None):
    """Gets data about WARCs to include in this download using WASAPI. A WARC is included if it was made since the
    last preservation download date and is part of a relevant collection. The collection list is either provided as
    an argument or the function will calculate a list of current Hargrett and Russell collections.

    Returns json converted to a Python object with all the WASAPI data on the included WARCs."""

    def collection_list():
        """Makes a list of Hargrett and Russell Archive-It collections. Used to skip collections by other departments
        (e.g. BMAC or DLG) or collections without metadata (test collections). """

        # Starts lists to store the collections to include (Hargrett and Russell) and exclude (everything else).
        collections_include = []
        collections_exclude = []

        # Uses the Partner API to get the data from the seed report, which has repository information.
        # Limit of -1 means it will get data on all seeds, no matter how many there are.
        seed_reports = requests.get(f'{c.partner_api}/seed?limit=-1', auth=(c.username, c.password))

        # If there was an error with the API call, quits the script.
        if not seed_reports.status_code == 200:
            aip.log(log_path, f'\nAPI error {seed_reports.status_code} for collection list.')
            print("API error, ending script. See log for details.")
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
                aip.log(log_path, f'Collection {collection_id} not included. No metadata.')
                continue

            # If the department is Hargrett or Russell, adds the collection id to the collections list. Otherwise,
            # adds it to the excluded list.
            if department_name.startswith('Hargrett') or department_name.startswith('Richard B. Russell'):
                collections_include.append(seed['collection'])
            else:
                collections_exclude.append(collection_id)
                aip.log(log_path, f'Collection {collection_id} department is {department_name}. Do not include.')

        return collections_include

    # Gets data about WARCs to include in this download using WASAPI. Explanation of filters:
    #   * crawl-start-after is inclusive so crawls done on the last download date will be included.
    #   * collections are the Archive-It ids for the relevant collections.
    #   * page size is the maximum number of WARCs the API call will return.
    if not collections:
        collections = collection_list()
    filters = {'crawl-start-after': last_download, 'collection': collections, 'page_size': 1000}
    warcs = requests.get(c.wasapi, params=filters, auth=(c.username, c.password))

    # If there was an error with the API call, quits the script.
    if not warcs.status_code == 200:
        aip.log(log_path, f'\nAPI error {warcs.status_code} when getting warc data.')
        print("API error, ending script. See log for details.")
        exit()

    # Converts the WARC data from json to a Python object and returns that Python object.
    py_warcs = warcs.json()
    return py_warcs


def seed_data(py_warcs, current_download, log_path):
    """Extracts information from the warc and seed data to define the AIP id, AIP title, and crawl definition id.
    Returns this data in a dictionary with the seed id as the key."""

    # Starts a dictionary for the number of seeds per collection, which is used in the AIP id.
    seed_count = {}

    # Starts a dictionary for data about seeds included in this download.
    seeds_include = {}

    # Starts a list of seed ids for seeds that will not be included in this download so the script doesn't have to
    # check them again. There are often multiple warcs per seed.
    seeds_exclude = []

    # Reformats the date of the current download to YYYYMM, which is used in the AIP id.
    download_date_code = str(current_download.year) + str(format(current_download.month, '02d'))

    # Iterates through data for each warc to get the seed ids which are included in this download. Those seed ids are
    # then used to look up information via the Partner API needed to generate the desired AIP information.
    for warc_info in py_warcs['files']:

        # Calculates the seed id, which is a portion of the warc filename.
        # Stops processing this warc and starts the next one if the filename doesn't match the expected pattern.
        try:
            regex_seed = re.match(r'^.*-SEED(\d+)-', warc_info['filename'])
            seed_identifier = regex_seed.group(1)
        except AttributeError:
            aip.log(log_path, f'Cannot calculate seed id for {warc_info["filename"]}.')
            continue

        # Stops processing this warc and starts the next one if the script has already assigned an AIP id to this seed.
        # This happens when a seed has multiple warcs.
        if seed_identifier in seeds_include or int(seed_identifier) in seeds_exclude:
            continue

        # Uses the Partner API to get data about this seed.
        seed_report = requests.get(f'{c.partner_api}/seed?id={seed_identifier}', auth=(c.username, c.password))

        # If there was an error with the API call, quits the script.
        if not seed_report.status_code == 200:
            aip.log(log_path, f'\nAPI error {seed_report.status_code} for seed report.')
            print("API error, ending script. See log for details.")
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
                aip.log(log_path, f'Seed {seed_info["id"]} has no title.')
                continue

            # Gets the department from the Collector field.
            # Stops processing this seed if there is no department. It is required.
            try:
                department_name = seed_info['metadata']['Collector'][0]['value']
            except (KeyError, IndexError):
                seeds_exclude.append(seed_info['id'])
                aip.log(log_path, f'Seed {seed_info["id"]} has no collector metadata.')
                continue

            # Assigns the Hargrett department code and collection number.
            # A default collection number 0000 is used if there is no relation information.
            if department_name.startswith('Hargrett'):
                department_code = 'harg'
                try:
                    regex_collection = re.match('^Hargrett (.*):', seed_info['metadata']['Relation'][0]['value'])
                    related_collection = regex_collection.group(1)
                except (KeyError, AttributeError):
                    related_collection = '0000'

            # Assigns the Russell department code and collection number.
            # A default collection number 000 if used if there is no relation information.
            elif department_name.startswith('Richard B. Russell'):
                department_code = 'rbrl'
                try:
                    regex_collection = re.match(r'^RBRL/(\d{3})', seed_info['metadata']['Relation'][0]['value'])
                    related_collection = regex_collection.group(1)
                except (KeyError, AttributeError):
                    related_collection = '000'

            # Stops processing this seed if the department isn't Hargrett or Russell. This shouldn't happen since
            # the script is only processing seeds from Hargrett or Russell collections, but there could have been an
            # error in making the collections list.
            else:
                seeds_exclude.append(seed_info['id'])
                aip.log(log_path, f'Seed {seed_info["id"]} is not Hargrett or Russell.')
                continue

            # Updates the count for the number of seeds from this collection in the seed_count dictionary.
            #   If the collection isn't there, adds it with a count of 0 and immediately adds 1.
            #   If the collection is there, adds one to the current count.
            # Then gets the current count and formats it as a 4 digit number.
            seed_count[related_collection] = seed_count.get(related_collection, 0) + 1
            sequential_number = format(seed_count[related_collection], '04d')

            # Constructs the AIP id for the seed.
            identifier = f'{department_code}-{related_collection}-web-{download_date_code}-{sequential_number}'

            # Saves AIP id, AIP title, and crawl definition id to the seeds_include dictionary.
            # This only contains information about seeds that had no errors and were fully processed.
            seeds_include[seed_identifier] = [identifier, title, seed_info['crawl_definition']]

    return seeds_include


def make_aip_directory(aip_folder):
    """Makes the AIP directory structure: a folder named aip-id_AIP Title with subfolders metadata and objects.
    Checks that the folders do not already exist prior to making them. """

    if not os.path.exists(f'{aip_folder}/metadata'):
        os.makedirs(f'{aip_folder}/metadata')

    if not os.path.exists(f'{aip_folder}/objects'):
        os.makedirs(f'{aip_folder}/objects')


def download_metadata(aip_id, aip_folder, warc_collection, crawl_definition, seed_id, current_download, log_path):
    """Uses the Partner API to download six metadata reports to include in the AIPs for archived websites,
    deletes any empty reports (meaning there was no data of that type for this seed), and redacts login information
    from the seed report. """

    def get_report(filter_type, filter_value, report_type, code):
        """Downloads a single metadata report and saves it as a csv in the AIP's metadata folder.
            filter_type and filter_value are used to filter the API call to the right AIP's report
            report_type is the Archive-It name for the report
            code is the ARCHive metadata code for the report"""

        # Builds the API call to get the report as a csv.
        # Limit of -1 will return all matches. Default is only the first 100.
        filters = {'limit': -1, filter_type: filter_value, 'format': 'csv'}
        metadata_report = requests.get(f'{c.partner_api}/{report_type}', params=filters, auth=(c.username, c.password))

        # Saves the metadata report if there were no errors with the API.
        if metadata_report.status_code == 200:
            with open(f'{aip_folder}/metadata/{aip_id}_{code}.csv', 'wb') as report_csv:
                report_csv.write(metadata_report.content)
        else:
            aip.log(log_path, f'Could not download {code} report. Error: {metadata_report.status_code}')

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
                aip.log(log_path, 'Seed report does not have login columns to redact.')
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

    # Downloads the six metadata reports from Archive-It needed to understand the context of the WARC.
    get_report('id', seed_id, 'seed', 'seed')
    get_report('seed', seed_id, 'scope_rule', 'seedscope')
    get_report('collection', warc_collection, 'scope_rule', 'collscope')
    get_report('id', warc_collection, 'collection', 'coll')
    get_report('collection', warc_collection, 'crawl_job', 'crawljob')
    get_report('id', crawl_definition, 'crawl_definition', 'crawldef')

    # Iterates over each report in the metadata folder to delete empty reports and redact login information from the
    # seed report.
    for report in os.listdir(f'{aip_folder}/metadata'):

        # Saves the full file path of the report.
        report_path = f'{c.script_output}/aips_{current_download}/{aip_folder}/metadata/{report}'

        # Deletes any empty metadata files (file size of 0) and begins processing the next file. A file is empty if
        # there is no metadata of that type, which is most common for collection and seed scope reports.
        if os.path.getsize(report_path) == 0:
            aip.log(log_path, f'Empty file deleted: {report}')
            os.remove(report_path)
            continue

        # Redacts login password and username from the seed report so we can share the seed report with researchers.
        if report.endswith('_seed.csv'):
            redact(report_path)


def download_warc(aip_folder, warc_filename, warc_url, warc_md5, current_download, log_path):
    """Downloads a warc file and verifies that fixity is unchanged after downloading."""

    # The path for where the warc will be saved on the local machine (it is long and used twice in this script).
    warc_path = f'{c.script_output}/aips_{current_download}/{aip_folder}/objects/{warc_filename}'

    # Downloads the warc.
    warc_download = requests.get(f"{warc_url}", auth=(c.username, c.password))

    # If there was an error with the API call, quits the function.
    if not warc_download.status_code == 200:
        aip.log(log_path, f'API error {warc_download.status_code} when downloading a WARC.')
        return

    # Saves the warc in the objects folder, keeping the original filename.
    with open(warc_path, 'wb') as warc_file:
        warc_file.write(warc_download.content)

    # Calculates the md5 for the downloaded WARC, using a regular expression to get the md5 from the md5deep output.
    # If the output is not formatted as expected, quits the function.
    md5deep_output = subprocess.run(f'"{aip.md5deep}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)
    try:
        regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(md5deep_output.stdout))
        downloaded_warc_md5 = regex_md5.group(1)
    except AttributeError:
        aip.log(log_path, f'Fixity cannot be extracted from md5deep output. \n{md5deep_output.stdout}')
        return

    # Compares the md5 of the download warc to what Archive-It has for the warc (warc_md5). If the md5 has changed,
    # deletes the WARC so the check for AIP completeness will catch that there was a problem.
    if not warc_md5 == downloaded_warc_md5:
        os.remove(warc_path)
        aip.log(log_path, f'Fixity changed on WARC after download. WARC was deleted. \n{md5deep_output.stdout}')


def find_empty_directory(log_path):
    """Identifies any AIPs with empty objects or metadata folders and moves them to an error folder."""

    # Iterates through the aips directory.
    for root, folders, files in os.walk('.'):

        # Looks for empty metadata folders.
        if root.endswith('metadata') and len(os.listdir(root)) == 0:

            # Calculates the path of the parent folder (the aip folder).
            aip_path = os.path.dirname(root)

            # Saves the error to the log and moves the aip to an error folder.
            aip.log(log_path, 'Stop processing. Empty metadata folder.')
            aip.move_error('incomplete_directory', aip_path)

        # Looks for empty objects folders.
        if root.endswith('objects') and len(os.listdir(root)) == 0:

            # Calculates the path of the parent folder (the aip folder).
            aip_path = os.path.dirname(root)

            # Saves the error to the log and moves the aip to an error folder.
            aip.log(log_path, 'Stop processing. Empty objects folder.')
            aip.move_error('incomplete_directory', aip_path)


def check_aips(current_download, last_download, seed_to_aip, log_path):
    """Verifies that all the expected AIPs for the download are complete and no unexpected AIPs were created.
    Produces a csv named completeness_check with the results in the AIPs directory. """

    def aip_dictionary():
        """Uses the Archive-It APIs and Python filters to gather information about the expected AIPs. Using Python
        instead of the API to filter the results for a more independent analysis of expected AIPs. All WARC
        information is downloaded, filtered with Python to those expected in this preservation download, and the WARC
        information is aggregated into a dictionary organized by seed/AIP. The key is the seed id and the values are
        the AIP id, warc count, and url. """

        # Downloads the entire WARC list, in json, and converts it to a python object.
        warcs = requests.get(c.wasapi, params={'page_size': 1000}, auth=(c.username, c.password))
        py_warcs = warcs.json()

        # If there was an API error, ends the function.
        if warcs.status_code != 200:
            aip.log(log_path, f'WASAPI error: {warcs.status_code}.')
            raise ValueError

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
                aip.log(log_path, f'No seed for {warc_info["warc_filename"]}.')
                raise ValueError

            # Filter one: only includes the WARC in the dictionary if it was created since the last download.
            # Simplifies the date format to YYYY-MM-DD by removing the time information before comparing it to the
            # last download date.
            try:
                regex_crawl_date = re.match(r"(\d{4}-\d{2}-\d{2})T.*", warc_info['crawl-start'])
                crawl_date = regex_crawl_date.group(1)
            except AttributeError:
                aip.log(log_path, f'No date for {warc_info["warc_filename"]}.')
                raise ValueError

            if crawl_date < last_download:
                warcs_exclude += 1
                continue

            # Checks if another WARC from this seed has been processed, meaning there is data in the aip_info
            # dictionary. If so, updates the WARC count in the dictionary and starts processing the next WARC. If
            # not, continues processing this WARC.
            try:
                aip_info[seed_identifier][1] += 1
                warcs_include += 1

            # Filter two: only includes the WARC in the dictionary if the repository is Hargrett or Russell. The
            # repository is in the seed report.
            except (KeyError, IndexError):

                # Gets the seed report for this seed.
                seed_report = requests.get(f'{c.partner_api}/seed?id={seed_identifier}', auth=(c.username, c.password))
                json_seed = seed_report.json()

                # If there was an API error, ends the function.
                if seed_report.status_code != 200:
                    aip.log(log_path, f'API error: {seed_report.status_code}.')
                    raise ValueError

                # Gets the repository from the seed report, if present. If not, this WARC is not included.
                try:
                    repository = json_seed[0]['metadata']['Collector'][0]['value']
                except (KeyError, IndexError):
                    warcs_exclude += 1
                    continue

                # Does not include the WARC in the dictionary if the repository is not Hargrett or Russell.
                if not repository.startswith('Hargrett') or repository.startswith('Richard B. Russell'):
                    warcs_exclude += 1
                    continue

                # Saves data about the WARC to the dictionary (AIP id, WARC count, URL). If the seed is not in
                # seed_to_aip, it is an unexpected seed and cannot be added to the dictionary.
                try:
                    aip_info[seed_identifier] = [seed_to_aip[seed_identifier], 1, json_seed[0]['url']]
                except (KeyError, IndexError):
                    aip.log(log_path, f'Unexpected seed: {seed_identifier}.')
                    raise ValueError

                warcs_include += 1

        # Checks that the right number of WARCs were evaluated.
        if warcs_expected != warcs_include + warcs_exclude:
            aip.log(log_path, 'Script did not review expected number of WARCs.')
            raise ValueError

        return aip_info

    def check_completeness(aip_id, warc_total, website_url):
        """Verifies a single AIP is complete, checking the contents of the metadata and objects folders. Returns a
        list with the results ready to be added as a row to the completeness check csv. """

        # Starts a list for the results, with the AIP id and website url to use for identification of the AIP.
        result = [aip_id, website_url]

        # Tests if there is a folder for this AIP in the AIPs directory. If not, returns the result for this AIP and
        # does not run the rest of the function's tests since there is no directory to check for completeness.
        if any(folder.startswith(aip_id) for folder in os.listdir(f'{c.script_output}/aips_{current_download}')):
            result.append(True)
        else:
            result.extend([False, 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a'])
            return result

        # Saves the file paths to the metadata and objects folders to variables, since they are long and reused.
        objects = f'{c.script_output}/aips_{current_download}/{aip_id}_bag/data/objects'
        metadata = f'{c.script_output}/aips_{current_download}/{aip_id}_bag/data/metadata'

        # Tests if each of the six Archive-It metadata reports is present. os.path.exists() returns True/False.
        result.append(os.path.exists(f'{metadata}/{aip_id}_coll.csv'))
        result.append(os.path.exists(f'{metadata}/{aip_id}_collscope.csv'))
        result.append(os.path.exists(f'{metadata}/{aip_id}_crawldef.csv'))
        result.append(os.path.exists(f'{metadata}/{aip_id}_crawljob.csv'))
        result.append(os.path.exists(f'{metadata}/{aip_id}_seed.csv'))
        result.append(os.path.exists(f'{metadata}/{aip_id}_seedscope.csv'))

        # Tests if the master.xml file is present.
        result.append(os.path.exists(f'{metadata}/{aip_id}_master.xml'))

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
                            '_seedscope.csv', '_master.xml', '_fits.xml')
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
        for aip_directory in os.listdir(f'{c.script_output}/aips_{current_download}'):

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
        aip.log(log_path, 'Unable to check AIPs for completeness. AIP dictionary not generated.')
        return

    # Starts a csv for the results of the quality review.
    csv_path = f'{c.script_output}/aips_{current_download}/completeness_check_{current_download}.csv'
    with open(csv_path, 'w', newline='') as complete_csv:
        complete_write = csv.writer(complete_csv)

        # Adds a header row to the csv.
        complete_write.writerow(
            ['AIP', 'URL', 'AIP Folder Made', 'coll.csv', 'collscope.csv', 'crawldef.csv', 'crawljob.csv', 'seed.csv',
             'seedscope.csv', 'master.xml', 'WARC Count Correct', 'Objects is all WARCs', 'fits.xml Count Correct',
             'No Extra Metadata'])

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
