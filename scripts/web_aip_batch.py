"""Purpose: Downloads archived web content and associated metadata for a group of seeds from Archive-It.org using
their APIs and converts them into AIPs that are ready to ingest into the UGA Libraries' digital preservation system
(ARCHive). At UGA, this script is run every three months to download content for all seeds crawled that quarter.

There will be one AIP per seed, even if that seed was crawled multiple times in the same quarter. A seed may have
multiple AIPs in the system, as a new AIP is made for every quarter's crawls.

Dependencies:
    * Python libraries: requests, python-dateutil
    * Tools: bagit.py, fits, md5deep, saxon, xmllint

Prior to the preservation download, all seed metadata should be entered into Archive-It. Use the metadata_check.py
script to verify all required fields are present. """

# Usage: python /path/web_aip_batch.py [last_download_date]

import datetime
import os
import re
import sys

import dateutil.relativedelta

# Import functions and constant variables from other UGA scripts.
import aip_functions as aip
import configuration as c
import web_functions as web

# The preservation download is limited to warcs created since the last download date. This can be calculated by the
# script as 3 months ago for when the script is run on schedule (February 1, May 1, August 1, and November 1) or
# another date may be supplied via a script argument. If supplied via argument, the format must be YYYY-MM-DD to be
# able to compare it to WARC dates. If the date is not the expected format, quits the script.
try:
    last_download = sys.argv[1]
    if not re.match(r'\d{4}-\d{2}-\d{2}', last_download):
        print('Date argument must be formatted YYYY-MM-DD. Please try the script again.')
        exit()
except IndexError:
    last_download = datetime.date.today() - dateutil.relativedelta.relativedelta(months=3)

# Makes a folder for AIPs within the script_output folder, a designated place on the local machine for web archiving
# documents). The folder name includes today's date to keep it separate from previous downloads which may still be
# saved on the same machine. current_download is a variable because it is also used as part of the quality_control
# function, and depending on how long it takes to download WARCs, recalculating today() may give a different result.
current_download = datetime.date.today()
aips_directory = f'aips_{current_download}'
if not os.path.exists(f'{c.script_output}/{aips_directory}'):
    os.makedirs(f'{c.script_output}/{aips_directory}')

# Changes current directory to the AIPs folder.
os.chdir(f'{c.script_output}/{aips_directory}')

# Starts a log for saving status information about the script. Saving to a document instead of printing to the screen
# since it allows for a permanent record of the download and because the terminal closed at the end of a script when
# it is run automatically with chronjob. The log is not started until after the current_download variable is set so that
# can be included in the file name.
log_path = f'../script_log_{current_download}.txt'
aip.log(log_path, f'Starting web preservation script on {current_download}.\n')


# PART ONE: DOWNLOAD WARCS AND METADATA INTO AIP DIRECTORY STRUCTURE.

# Uses Archive-It APIs to get information about the WARCs and seeds in this download. If there is an API failure,
# warc_data() and seed_data() returns the API status code instead of the expected data, which quits the script.
warc_metadata = web.warc_data(last_download, log_path)
seed_metadata = web.seed_data(warc_metadata, current_download, log_path)

# Starts counts for tracking script progress. Some processes are slow, so this shows the script is still working.
current_warc = 0
total_warcs = warc_metadata['count']

# Starts a dictionary to store a mapping of seed id to AIP id, used for checking the downloaded AIPs for completeness.
seed_to_aip = {}

# Iterates through information about each WARC.
for warc in warc_metadata['files']:

    # Updates the current WARC number and displays the script progress.
    current_warc += 1
    aip.log(log_path, f"\nProcessing {warc['filename']} ({current_warc} of {total_warcs}).")
    print(f"\nProcessing {warc['filename']} ({current_warc} of {total_warcs}).")

    # Calculates seed id, which is a portion of the WARC filename between "-SEED" and "-".
    # Stops processing this WARC and starts the next if the WARC filename doesn't match expected pattern.
    try:
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc['filename'])
        seed_id = regex_seed_id.group(1)
    except AttributeError:
        aip.log(log_path, 'Cannot calculate seed id.')
        continue

    # Saves relevant information about the WARC in variables for future use.
    # Stops processing if the WARC does not have the required metadata.
    try:
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
    except (KeyError, IndexError):
        aip.log(log_path, 'WARC information is formatted wrong.')
        continue

    # Saves relevant information the WARC's seed in variables for future use.
    # Stops processing if the WARC does not the required metadata.
    try:
        aip_id = seed_metadata[seed_id][0]
        aip_title = seed_metadata[seed_id][1]
        crawl_definition = seed_metadata[seed_id][2]
    except (KeyError, IndexError):
        aip.log(log_path, 'Seed has no metadata.')
        continue

    # Adds the seed to the seed_to_aip dictionary. This is used for checking the downloaded AIPs for completeness.
    seed_to_aip[seed_id] = aip_id

    # Title is included in the folder temporarily so the AIP functions can access the title.
    aip_name = f'{aip_id}_{aip_title}'

    # Makes the AIP directory for the seed's AIP (AIP folder with metadata and objects subfolders).
    web.make_aip_directory(aip_name)

    # Downloads the seed metadata from Archive-It into the seed's metadata folder if it is not already there (and so
    # the folder is empty). A seed may have multiple WARCs and only want to download the seed's reports once.
    if len(os.listdir(f'{aip_name}/metadata')) == 0:
        web.download_metadata(aip_id, aip_name, warc_collection, crawl_definition, seed_id, current_download, log_path)

    # Downloads the WARC from Archive-It into the seed's objects folder.
    web.download_warc(aip_name, warc_filename, warc_url, warc_md5, current_download, log_path)

# Checks for empty metadata or objects folders in the AIPs. These happens if there were uncaught download errors.
web.find_empty_directory(log_path)


# PART TWO: CREATE AIPS THAT ARE READY FOR INGEST INTO ARCHIVE

# Makes directories used to store script outputs, if they aren't already there.
aip.make_output_directories()

# Starts counts for tracking script progress. Some processes are slow, so this shows the script is still working.
current_aip = 0
total_aips = len(os.listdir('.'))

# Runs the scripts for each step of making an AIP, one folder at a time. Checks if the AIP is still present before
# running each script, in case it was moved due to an error in the previous script.
for aip_folder in os.listdir('.'):

    # Updates the current AIP number and displays the script progress.
    current_aip += 1
    aip.log(log_path, f'\n>>>Processing {aip_folder} ({current_aip} of {total_aips}).')
    print(f'\n>>>Processing {aip_folder} ({current_aip} of {total_aips}).')

    # Extracts the AIP id, department, and AIP title from the folder name and saves them to variables. If the folder
    # name doesn't match this pattern, moves the AIP to an error folder and begins processing the next AIP.
    #   (Group 1) The AIP id is before the first underscore and is only lowercase letters, numbers, or dashes.
    #   (Group 2) The department is indicated by the first part of the AIP id, either 'harg' or 'rbrl'.
    #   (Group 3) The AIP title is everything after the first underscore.
    try:
        regex_aip = re.match('^((harg|rbrl)[a-z0-9-]+)_(.*)', aip_folder)
        aip_id = regex_aip.group(1)
        department = regex_aip.group(2)
        aip_title = regex_aip.group(3)
    except AttributeError:
        aip.log(log_path, 'Stop processing. Folder name not structured correctly.')
        aip.move_error('folder_name', aip_folder)
        continue

    # Renames the AIP folder to just the AIP id.
    os.replace(aip_folder, aip_id)

    # Extracts technical metadata from the files using FITS.
    if aip_id in os.listdir('.'):
        aip.extract_metadata(aip_id, f'{c.script_output}/{aips_directory}', log_path)

    # Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets.
    if aip_id in os.listdir('.'):
        aip.make_preservationxml(aip_id, aip_title, department, 'website', log_path)

    # Bags, tars, and zips the AIP.
    if aip_id in os.listdir('.'):
        aip.package(aip_id, log_path)

# Makes MD5 manifests of all AIPs the in this download using md5deep, with one manifest per department.
aip.make_manifest()

# Verifies the AIPs are complete and no extra AIPs were created. Does not look at the errors folder, so any AIPs with
# errors will show as missing. Saves the result as a csv in the folder with the downloaded AIPs.
print('\nStarting completeness check.')
web.check_aips(current_download, last_download, seed_to_aip, log_path)

# Adds completion of the script to the log.
aip.log(log_path, f'\nScript finished running at {datetime.datetime.today()}.')

# Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) into the AIPs folder for this
# download to keep everything together if another set is downloaded before these are deleted.
os.chdir(c.script_output)
to_move = ['aips-to-ingest', 'errors', 'fits-xml', 'preservation-xml', f'script_log_{current_download}.txt']
for item in os.listdir('.'):
    if item in to_move:
        os.replace(item, f'{aips_directory}/{item}')
