"""Purpose: Downloads archived web content and associated metadata for a single seed from Archive-It.org using their
APIs and converts it into an AIP that is ready to ingest into the UGA Libraries' digital preservation system (ARCHive).

Dependencies:
    * Python libraries: requests
    * Tools: bagit.py, fits, md5deep, saxon, xmllint

Prior to the preservation download, all seed metadata should be entered into Archive-It.

Ideas for improvement: make a log of completeness check instead of printing to the terminal to have a record;
include time in log start date and calculate how long it took to run.
"""

# Usage: python /path/web_aip_single.py seed_id aip_id collection_id [last_download_date]

import datetime
import os
import re
import requests
import sys

# Import functions and constant variables from other UGA scripts.
import aip_functions as aip
import configuration as c
import web_functions as web


def seed_data():
    """Returns the AIP title and crawl definition id from the seed report. This is different from the seed_data()
    function used by web_aip_batch.py since the AIP id does not need to be calculated."""

    # Uses the Partner API to get data about this seed.
    seed_report = requests.get(f'{c.partner_api}/seed?id={seed_id}', auth=(c.username, c.password))

    # If there was an error with the API call, quits the script.
    if not seed_report.status_code == 200:
        aip.log(log_path, f'\nAPI error {seed_report.status_code} for seed report.')
        print("API error, ending script. See log for details.")
        exit()

    # Converts the seed data from json to a Python object.
    py_seed_report = seed_report.json()

    # Constructs the aip id from the seed data. If at any stage a piece of the aip id cannot be calculated,
    # the seed is added to the exclude list and the next warc is processed.
    for seed_info in py_seed_report:

        # Gets the title for the seed from the Title field.
        # Stops processing this seed if the title is missing. It is required.
        try:
            title = seed_info['metadata']['Title'][0]['value']
            crawl_def = seed_info['crawl_definition']
        except (KeyError, IndexError):
            aip.log(log_path, f'Seed {seed_info["id"]} has no metadata.')
            raise ValueError

    return title, crawl_def


def check_aip():
    """Verifies a single AIP is complete, checking the contents of the metadata and objects folders. Prints any
    errors to the terminal. This is different from the check_aips() function used by web_aip_batch.py since it has to
    filter by seed id and is only checking one AIP. """

    def aip_warcs_count():
        """Uses the Archive-It APIs and Python filters to determine how many warcs should be in the AIP. Using Python
        instead of the API to filter the results for a more independent analysis."""

        # Downloads the entire WARC list, in json, and converts it to a python object.
        warcs = requests.get(c.wasapi, params={'page_size': 1000}, auth=(c.username, c.password))
        py_warcs = warcs.json()

        # If there was an API error, ends the function.
        if warcs.status_code != 200:
            aip.log(log_path, f'WASAPI error: {warcs.status_code}.')
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
                aip.log(log_path, f'No seed for {warc_info["warc_filename"]}.')
                raise ValueError

            # Filter one: do not count the WARC if it is from a different seed.
            if not seed_id == warc_seed_identifier:
                warcs_exclude += 1
                continue

            # Filter two: do not count the WARC if it was created before the last download. Simplifies the date format
            # to YYYY-MM-DD by removing the time information before comparing it to the last download date.
            try:
                regex_crawl_date = re.match(r"(\d{4}-\d{2}-\d{2})T.*", warc_info['crawl-start'])
                crawl_date = regex_crawl_date.group(1)
            except AttributeError:
                aip.log(log_path, f'No date for {warc_info["warc_filename"]}.')
                raise ValueError

            if crawl_date < last_download:
                warcs_exclude += 1
                continue

            # If the WARC was not excluded by the previous two filters, it should be included in the AIP.
            warcs_include += 1

        # Checks that the right number of WARCs were evaluated.
        if warcs_from_api != warcs_include + warcs_exclude:
            aip.log(log_path, 'Script did not review expected number of WARCs.')
            raise ValueError

        return warcs_include

    # Saves the file paths to the metadata and objects folders to variables, since they are long and reused.
    objects = f'{c.script_output}/aips_{current_download}/{aip_id}_bag/data/objects'
    metadata = f'{c.script_output}/aips_{current_download}/{aip_id}_bag/data/metadata'

    # List of suffixes used for the expected metadata reports.
    expected_endings = ('_coll.csv', '_collscope.csv', '_crawldef.csv', '_crawljob.csv', '_seed.csv',
                        '_seedscope.csv', '_preservation.xml', '_fits.xml')

    # Calculates the number of WARCs that should be in this AIP. Exits the function if it is not calculated since
    # multiple tests depend on this.
    try:
        warcs_expected = aip_warcs_count()
    except ValueError:
        print('Cannot check AIP for completeness. WARC count not calculated.')
        return

    # Tests if there is a folder for this AIP in the AIPs directory.
    if not any(folder.startswith(aip_id) for folder in os.listdir(f'{c.script_output}/aips_{current_download}')):
        print('The AIP folder was not created.')

    # Tests if each of the expected metadata reports is present. Skips FITS because the filename is formatted
    # differently and it is checked in the next test.
    for end in expected_endings:
        if end == '_fits.xml':
            continue
        if not os.path.exists(f'{metadata}/{aip_id}{end}'):
            print(end, 'was not created.')

    # Tests if the number of FITS files is correct (one for each WARC).
    fits_count = len([file for file in os.listdir(metadata) if file.endswith('_fits.xml')])
    if not fits_count == warcs_expected:
        print(f'The number of FITS files is incorrect: {fits_count} instead of {warcs_expected}.')

    # Tests if everything in the AIP's metadata folder is an expected file type.
    for file in os.listdir(metadata):
        if not file.endswith(expected_endings):
            print('File in metadata folder that is not expected:', file)

    # Tests if the number of WARCs is correct.
    warcs_in_objects = len([file for file in os.listdir(objects) if file.endswith('.warc.gz')])
    if not warcs_expected == warcs_in_objects:
        print(f'The number of WARCs is incorrect: {warcs_in_objects} instead of {warcs_expected}')

    # Tests if everything in the AIP's objects folder is a WARC.
    for file in os.listdir(objects):
        if not file.endswith('.warc.gz'):
            print('File in objects folder that is not a WARC:', file)


# Tests required script arguments were provided and assigns to variables.
try:
    seed_id = sys.argv[1]
    aip_id = sys.argv[2]
    collection_id = sys.argv[3]
except IndexError:
    print('Exiting script: missing required argument(s). Must include seed id, AIP id, and collection id.')
    exit()

# Tests optional script argument was provided and assigns to variable. Must be formatted YYYY-MM-DD.
# If no last download date is given, sets the date as when UGA Libraries' began web archiving.
try:
    last_download = sys.argv[4]
    if not re.match(r'\d{4}-\d{2}-\d{2}', last_download):
        print('Exiting script: date argument must be formatted YYYY-MM-DD.')
        exit()
except IndexError:
    last_download = '2019-06-01'

# Extracts the department from the aip_id saves it to a variable. Quits the script if the AIP id is formatted wrong.
try:
    regex_dept = re.match('^(harg|rbrl).*', aip_id)
    department = regex_dept.group(1)
except AttributeError:
    print('Exiting script: AIP id is not formatted correctly. Department could not be identified.')
    exit()

print(f'Making AIP for {seed_id}.')

# Makes a folder for aips within the script_output folder, a designated place on the local machine for web archiving
# documents. The folder name includes today's date to keep it separate from previous downloads which may still be
# saved on the same machine. current_download is a variable because it is also used as part of the quality_control
# function, and depending on how long it takes to download WARCs, recalculating today() may give a different result.
current_download = datetime.date.today()
aips_directory = f'aips_{current_download}'
if not os.path.exists(f'{c.script_output}/{aips_directory}'):
    os.makedirs(f'{c.script_output}/{aips_directory}')

# Changes current directory to the aips folder.
os.chdir(f'{c.script_output}/{aips_directory}')

# Starts a log for saving status information about the script. Saving to a document instead of printing to the screen
# since it allows for a permanent record of the download and because the terminal closed at the end of a script when
# it is run automatically with chronjob. The log is not started until after the current_download variable is set so that
# can be included in the file name.
log_path = f'../script_log_{current_download}.txt'
aip.log(log_path, f'\nStarting web preservation script on {current_download}.\n')


# PART ONE: DOWNLOAD WARCS AND METADATA INTO THE AIP DIRECTORY STRUCTURE.
print('Downloading AIP content.')

# Uses WASAPI to get information about the WARCs for this seed's collection. The WARC's seed id is checked before it
# is downloaded to skip other seeds from this collection. It is not possible to filter by seed using WASAPI.
warc_metadata = web.warc_data(last_download, log_path, collection_id)

# Uses the Archive-It Partner API to get information about this seed.
try:
    aip_title, crawl_definition = seed_data()
except ValueError:
    aip.log(log_path, 'Seed has no metadata.')
    print('Exiting script: seed has no metadata.')
    exit()

# Makes the aip directory for the seed's aip (aip folder with metadata and objects subfolders). Unlike with the batch
# script, the folder does not need to temporarily include the AIP title since the title is already stored in a variable.
web.make_aip_directory(aip_id)

# Downloads the seed metadata from Archive-It into the seed's metadata folder.
# The aip_id is passed twice, as both the AIP id and AIP folder. These are different values for the batch script.
web.download_metadata(aip_id, aip_id, collection_id, crawl_definition, seed_id, current_download, log_path)

# Iterates through information about each WARC.
for warc in warc_metadata['files']:

    # Saves relevant information about the WARC in variables to use for downloading the WARC.
    try:
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
    except (KeyError, IndexError):
        aip.log(log_path, 'WARC information is formatted wrong.')
        continue

    # Checks if the WARC is from the seed being downloaded. If not, skips the WARC.
    # The WARC's seed id is the portion of the WARC's filename between "-SEED" and "-".
    try:
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        warc_seed_id = regex_seed_id.group(1)
    except AttributeError:
        aip.log(log_path, 'Cannot calculate the WARC seed id.')
        continue
    if not seed_id == warc_seed_id:
        continue

    # Downloads the warc from Archive-It into the seed's objects folder.
    web.download_warc(aip_id, warc_filename, warc_url, warc_md5, current_download, log_path)

# Checks for empty metadata or objects folders in the AIPs. These happens if there were uncaught download errors.
web.find_empty_directory(log_path)


# PART TWO: CREATE AIPS THAT ARE READY FOR INGEST INTO ARCHIVE
print('Converting into an AIP.')

# Makes directories used to store script outputs, if they aren't already there.
aip.make_output_directories()

# Extracts technical metadata from the files using FITS.
if aip_id in os.listdir('.'):
    aip.extract_metadata(aip_id, f'{c.script_output}/{aips_directory}', log_path)

# Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets. Determines the
# third argument (ARCHive group name) from the department code parsed from the folder name.
if aip_id in os.listdir('.'):
    aip.make_preservationxml(aip_id, aip_title, department, 'website', log_path)

# Bags the aip.
if aip_id in os.listdir('.'):
    aip.bag(aip_id, log_path)

# Tars, and zips the aip.
if f'{aip_id}_bag' in os.listdir('.'):
    aip.package(aip_id, aips_directory)

# If the AIP has not been moved to the errors folder, verifies the AIP is complete. Errors are printed to the terminal.
if f'{aip_id}_bag' in os.listdir('.'):
    print('\nStarting completeness check.')
    check_aip()

# Makes MD5 manifest of the AIP.
aip.make_manifest()

# Adds completion of the script to the log.
aip.log(log_path, f'\nScript finished running at {datetime.datetime.today()}.')
