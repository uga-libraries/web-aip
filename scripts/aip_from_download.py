# The WARCs and metadata files for a group of web AIPs were downloaded by the AIPs still need to be made.
# Using a separate script for ease of customization, but basically it is the second half of web_aip_batch.
# Extras: creates metadata dictionaries from a csv with the information and extra checking.

# TODO: once the AIPs are made, run the completeness check
#  OR adapt the web_aip_single.py completeness check for what is in the folder.

import csv
import datetime
import os
import re
import requests

# Import functions and constant variables from other UGA scripts.
import aip_functions as aip
import configuration as c


def check_aip(aip_id, seed_id):
    """Verifies a single AIP is complete, checking the contents of the metadata and objects folders.
     Saves the results to a CSV.

    This is different from the check_aips() function used by web_aip_batch.py since it has to
    filter by seed id and is only checking one AIP. It is different from the function of the same name in
    web_aip_single.py because it appends the results to a spreadsheet so the end result is one spreadsheet for all the
    AIPs across multiple batches."""

    def aip_warcs_count():
        """Uses the Archive-It APIs and Python filters to determine how many warcs should be in the AIP. Using Python
        instead of the API to filter the results for a more independent analysis."""

        # Downloads the entire WARC list, in json, and converts it to a python object.
        warcs = requests.get(c.wasapi, params={'page_size': 1000}, auth=(c.username, c.password))
        py_warcs = warcs.json()

        # If there was an API error, ends the function.
        if warcs.status_code != 200:
            aip.log(log_path, f"WASAPI error: {warcs.status_code}.")
            raise ValueError

        # Starts variables used to verify that the script processes the right number of WARCs. The total number of WARCs
        # that are either part of this download (include) or not part of it (exclude) are compared to the total
        # number of WARCs expected from the API data. warcs_include is also the number of WARCs for this AIP.
        warcs_from_api = py_warcs['count']
        warcs_include = 0
        warcs_exclude = 0

        # Iterates over the metadata for each WARC.
        for warc_info in py_warcs['files']:

            # Gets the seed id from the WARC filename.
            try:
                regex_seed = re.match(r".*-SEED(\d+)-.*", warc_info['filename'])
                warc_seed_identifier = regex_seed.group(1)
            except AttributeError:
                aip.log(log_path, f"No seed for {warc_info['warc_filename']}.")
                raise ValueError

            # Filter one: do not count the WARC if it is from a different seed.
            if not seed_id == warc_seed_identifier:
                warcs_exclude += 1
                continue

            # Filter two: do not count the WARC if it was created before the last download. Store time is used so
            # test crawls are evaluated based on the date they were saved. Simplifies the date format to YYYY-MM-DD
            # by removing the time information before comparing it to the last download date.
            try:
                regex_crawl_date = re.match(r"(\d{4}-\d{2}-\d{2})T.*", warc_info['store-time'])
                crawl_date = regex_crawl_date.group(1)
            except AttributeError:
                aip.log(log_path, f"No date for {warc_info['warc_filename']}.")
                raise ValueError

            if crawl_date < last_download or crawl_date > current_download:
                warcs_exclude += 1
                continue

            # If the WARC was not excluded by the previous two filters, it should be included in the AIP.
            warcs_include += 1

        # Checks that the right number of WARCs were evaluated.
        if warcs_from_api != warcs_include + warcs_exclude:
            aip.log(log_path, "Script did not review expected number of WARCs.")
            raise ValueError

        return warcs_include

    # Starts a list to store the results of the completeness check.
    completeness = [aip_id]

    # Saves the file paths to the metadata and objects folders to variables, since they are long and reused.
    objects = f"{c.script_output}/aips_{current_download}/{aip_id}_bag/data/objects"
    metadata = f"{c.script_output}/aips_{current_download}/{aip_id}_bag/data/metadata"

    # Calculates the number of WARCs that should be in this AIP. Exits the function if it is not calculated since
    # multiple tests depend on this.
    try:
        warcs_expected = aip_warcs_count()
    except ValueError:
        aip.log(log_path, 'Cannot check AIP for completeness. WARC count was not correct.')
        return

    # Tests if there is a folder for this AIP in the AIPs directory.
    completeness.append(os.path.exists(f'{c.script_output}/aips_{current_download}/{aip_id}_bag'))

    # Tests if each of the expected metadata reports that has either 0 or 1 instances present.
    completeness.append(os.path.exists(f'{metadata}/{aip_id}_coll.csv'))
    completeness.append(os.path.exists(f'{metadata}/{aip_id}_collscope.csv'))
    completeness.append(os.path.exists(f'{metadata}/{aip_id}_seed.csv'))
    completeness.append(os.path.exists(f'{metadata}/{aip_id}_seedscope.csv'))
    completeness.append(os.path.exists(f'{metadata}/{aip_id}_preservation.xml'))

    # Counts each of the expected metadata reports than can have more than one.
    completeness.append(len([file for file in os.listdir(metadata) if file.endswith("_crawldef.csv")]))
    completeness.append(len([file for file in os.listdir(metadata) if file.endswith("_crawljob.csv")]))

    # Tests if the number of FITS files is correct (one for each WARC).
    fits_count = len([file for file in os.listdir(metadata) if file.endswith("_fits.xml")])
    completeness.append(fits_count == warcs_expected)

    # Tests if everything in the AIP's metadata folder is an expected file type.
    expected_endings = ('coll.csv', 'collscope.csv', 'crawldef.csv', 'crawljob.csv', 'seed.csv',
                        'seedscope.csv', 'preservation.xml', 'fits.xml')
    completeness.append(any(not file.endswith(expected_endings) for file in os.listdir(metadata)))

    # Tests if the number of WARCs is correct.
    warcs_in_objects = len([file for file in os.listdir(objects) if file.endswith(".warc.gz")])
    completeness.append(warcs_expected == warcs_in_objects)

    # Tests if everything in the AIP's objects folder is a WARC.
    completeness.append(any(not file.endswith('.warc.gz') for file in os.listdir(objects)))

    # Returns the results to be saved in a CSV.
    return completeness


# SUBSTITUTES FOR PART ONE: ARGUMENTS, VARIABLES CALCULATED, SETTING CURRENT DIRECTORY, STARTING LOG.
last_download = "2021-02-01"
current_download = "2021-05-01"
aips_directory = f'aips_{current_download}'

os.chdir(f'{c.script_output}/{aips_directory}')

log_path = "../conversion_to_aips_log.txt"
aip.log(log_path, f'\n\nCreating AIPs from a batch of downloaded files using the aip_from_download.py script. '
                  f'Script started running at {datetime.datetime.today()}.')


seed_to_aip = {}
aip_to_title = {}

# Read data from CSV about the AIPs which would usually be calculated from the API.
# For every row, other than the header, get the seed id, title, and aip id, and add to the two dictionaries.
with open('C:/Users/amhan/Desktop/russell_aip_metadata.csv', 'r') as data_file:
    data_read = csv.reader(data_file)
    next(data_read)
    for row in data_read:
        seed = row[1]
        title = row[2]
        aip_id = row[4]
        seed_to_aip[seed] = aip_id
        aip_to_title[aip_id] = title


# PART TWO: CREATE AIPS THAT ARE READY FOR INGEST INTO ARCHIVE

# Makes a CSV for the completeness check, with a header, if it does not already exist.
# If it does exist, will just add to the existing CSV.
csv_path = f'{c.script_output}/completeness_check_{current_download}.csv'
if not os.path.exists(csv_path):
    with open(csv_path, 'w', newline='') as complete_csv:
        complete_write = csv.writer(complete_csv)
        complete_write.writerow(
            ['AIP', 'AIP Folder Made', 'Coll', 'Collscope', 'Seed', 'Seedscope', 'Preservation XML',
             'CrawlDef Count', 'CrawlJob Count', 'FITS Count', 'Unexpected Metadata',
             'WARC Count Correct', 'Unexpected Objects'])

# Makes directories used to store script outputs, if they aren't already there.
aip.make_output_directories()

# Starts counts for tracking script progress. Some processes are slow, so this shows the script is still working.
current_aip = 0
total_aips = len(os.listdir('.'))

# Adds name for the next section to the log.
aip.log(log_path, f'\nPROCESSING AIPS ({total_aips} TOTAL)')

# Runs the scripts for each step of making an AIP, one folder at a time. Checks if the AIP is still present before
# running each script, in case it was moved due to an error in the previous script.
for aip_folder in os.listdir('.'):

    # Updates the current AIP number and displays the script progress.
    current_aip += 1
    aip.log(log_path, f'\n{aip_folder}')
    print(f'\nProcessing {aip_folder} ({current_aip} of {total_aips}).')

    # Calculates the department group name in ARCHive from the folder name (the AIP ID).
    # If it does not match the expected pattern, moves the AIP to an error folder and begins processing the next AIP.
    if aip_folder.startswith('harg'):
        department = 'hargrett'
    elif aip_folder.startswith('rbrl'):
        department = 'russell'
    else:
        aip.log(log_path, f'AIP ID {aip_folder} does not start with an expected department prefix. '
                          f'AIP moved to error folder.')
        aip.move_error('department_prefix', aip_folder)
        continue

    # Extracts technical metadata from the files using FITS.
    if aip_folder in os.listdir('.'):
        aip.extract_metadata(aip_folder, f'{c.script_output}/{aips_directory}', log_path)

    # Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets.
    if aip_folder in os.listdir('.'):
        aip.make_preservationxml(aip_folder, aip_to_title[aip_folder], department, 'website', log_path)

    # Bags the aip.
    if aip_folder in os.listdir('.'):
        aip.bag(aip_folder, log_path)

    # Tars, and zips the aip.
    if f'{aip_folder}_bag' in os.listdir('.'):
        aip.package(aip_folder, os.getcwd())

    # Tests the AIPs completeness and saves the result to a CSV.
    # First gets the seed id (key) using aip id (value) from seed_to_aip.
    seed = list(seed_to_aip.keys())[list(seed_to_aip.values()).index(aip_folder)]
    row_values = check_aip(aip_folder, seed)
    with open(csv_path, 'a', newline='') as complete_csv:
        complete_write = csv.writer(complete_csv)
        complete_write.writerow(row_values)

# Makes MD5 manifests of all AIPs the in this download using md5deep, with one manifest per department.
aip.make_manifest()

# Adds completion of the script to the log.
aip.log(log_path, f'\nScript finished running at {datetime.datetime.today()}.')
