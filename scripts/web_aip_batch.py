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

# Usage: python /path/web_aip_batch.py date_start date_end

import datetime
import os
import re
import sys

# Import functions and constant variables from other UGA scripts.
import aip_functions as aip
import configuration as c
import web_functions as web

# The preservation download is limited to warcs created during a particular time frame.
# UGA downloads every quarter (2/1-4/30, 5/1-7/31, 8/1-10/31, 11/1-1/31)
# Tests that both dates are provided. If not, ends the script.
try:
    date_start, date_end = sys.argv[1:]
except IndexError:
    print("Exiting script: must provide exactly two arguments, the start and end date of the quarter.")
    exit()

# Tests that both dates are formatted correctly (YYYY-MM-DD). If not, ends the script.
if not re.match(r"\d{4}-\d{2}-\d{2}", date_start):
    print(f"Exiting script: start date '{date_start}' must be formatted YYYY-MM-DD.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_end):
    print(f"Exiting script: end date '{date_end}' must be formatted YYYY-MM-DD.")
    exit()

# Tests the paths in the configuration file to verify they exist. Quits the script if any are incorrect.
# It is common to get typos when setting up the configuration file on a new machine.
valid_errors = aip.check_paths()
if not valid_errors == "no errors":
    print('The following path(s) in the configuration file are not correct:')
    for error in valid_errors:
        print(error)
    print('Correct the configuration file and run the script again.')
    sys.exit()

# Makes a folder for AIPs within the script_output folder, a designated place on the local machine for web archiving
# documents). The folder name includes the end date for the download to keep it separate from previous downloads
# which may still be saved on the same machine.
aips_directory = f'aips_{date_end}'
if not os.path.exists(f'{c.script_output}/{aips_directory}'):
    os.makedirs(f'{c.script_output}/{aips_directory}')

# Changes current directory to the AIPs folder.
os.chdir(f'{c.script_output}/{aips_directory}')

# PART ONE: DOWNLOAD WARCS AND METADATA INTO AIP DIRECTORY STRUCTURE.

# Uses Archive-It APIs to get information about the WARCs and seeds in this download. If there is an API failure,
# warc_data() and seed_data() quit the script.
warc_metadata = web.warc_data(date_start, date_end)
seed_metadata = web.seed_data(warc_metadata, date_end)

# Starts counts for tracking script progress. Some processes are slow, so this shows the script is still working.
current_warc = 0
total_warcs = warc_metadata['count']

# Starts a dictionary to store a mapping of seed id to AIP id, used for checking the downloaded AIPs for completeness.
seed_to_aip = {}

# Starts a dictionary to store the title for each AIP. Using a dictionary instead of making it part of the initial AIP
# folder name like the general aip script to allow for using characters in the title which cannot be part of a folder.
aip_to_title = {}

# Starts a log in the script output folder to use for recording errors related to the warcs.
web.warc_log("header")

# Iterates through information about each WARC.
for warc in warc_metadata['files']:

    # Starts a dictionary of information for the log.
    log_data = {"filename": "TBD", "warc_json": "n/a", "seed_id": "n/a", "job_id": "n/a",
                "seed_metadata": "n/a", "report_download": "n/a", "report_info": "n/a", "warc_api": "n/a",
                "warc_fixity": "n/a", "complete": "Errors during WARC processing."}

    # Updates the current WARC number and displays the script progress.
    current_warc += 1
    print(f"\nProcessing WARC {current_warc} of {total_warcs}.")

    # Saves relevant information about the WARC in variables for future use.
    # Stops processing if the WARC does not have the required metadata.
    try:
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
        warc_collection = warc['collection']
        log_data["filename"] = warc_filename
        log_data["warc_json"] = "Successfully got WARC data."
    except KeyError:
        log_data["warc_json"] = f"Could not find at least one expected value in JSON: {warc}"
        web.warc_log(log_data)
        continue
    except IndexError:
        log_data["warc_json"] = f"Could not find URL in JSON: {warc}"
        web.warc_log(log_data)
        continue

    # Calculates seed id, which is a portion of the WARC filename between "-SEED" and "-".
    # Stops processing this WARC and starts the next if the WARC filename doesn't match expected pattern.
    try:
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc_filename)
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."
    except AttributeError:
        log_data["seed_id"] = "Could not calculate seed id from the WARC filename."
        web.warc_log(log_data)
        continue

    # Calculates the job id from the WARC filename, which are the numbers after "-JOB".
    # Stops processing this WARC and starts the next if the WARC filename doesn't match the expected pattern.
    try:
        regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
        job_id = regex_job_id.group(1)
        log_data["job_id"] = "Successfully calculated job id."
    except AttributeError:
        log_data["job_id"] = "Could not calculate job id from the WARC filename."
        web.warc_log(log_data)
        continue

    # Saves relevant information the WARC's seed in variables for future use.
    # Stops processing if the WARC does not the required metadata.
    try:
        aip_id = seed_metadata[seed_id][0]
        aip_title = seed_metadata[seed_id][1]
        log_data["seed_metadata"] = "Successfully got seed metadata."
    except KeyError:
        log_data["seed_metadata"] = "Seed id is not in seed JSON."
        web.warc_log(log_data)
        continue
    except IndexError:
        log_data["seed_metadata"] = f"At least one value missing from JSON for this seed: {seed_metadata[seed_id]}"
        web.warc_log(log_data)
        continue

    # Adds the seed to the seed_to_aip dictionary. This is used for checking the downloaded AIPs for completeness.
    seed_to_aip[seed_id] = aip_id

    # Adds the AIP title to the aip_to_title dictionary. This is used for making the preservation.xml later.
    aip_to_title[aip_id] = aip_title

    # Makes the AIP directory for the seed's AIP (AIP folder with metadata and objects subfolders).
    web.make_aip_directory(aip_id)

    # Downloads the seed metadata from Archive-It into the seed's metadata folder.
    web.download_metadata(aip_id, warc_collection, job_id, seed_id, date_end, log_data)

    # Downloads the WARC from Archive-It into the seed's objects folder.
    web.download_warc(aip_id, warc_filename, warc_url, warc_md5, date_end, log_data)

    # If no errors were encountered (the last test was successful), updates the completion status.
    # Saves the log information for all completed WARCs, even if there were errors.
    if log_data["warc_fixity"].startswith("Successfully"):
        log_data["complete"] = "Successfully processed WARC."
    web.warc_log(log_data)

# Checks for empty metadata or objects folders in the AIPs. These happens if there were uncaught download errors.
web.find_empty_directory()


# # PART TWO: CREATE AIPS THAT ARE READY FOR INGEST INTO ARCHIVE
#
# # Makes directories used to store script outputs, if they aren't already there.
# aip.make_output_directories()
#
# # Starts counts for tracking script progress. Some processes are slow, so this shows the script is still working.
# current_aip = 0
# total_aips = len(os.listdir('.'))
#
# # Adds name for the next section to the log.
# aip.log(log_path, f'\n\nPROCESSING AIPS ({total_aips} TOTAL)')
#
# # Runs the scripts for each step of making an AIP, one folder at a time. Checks if the AIP is still present before
# # running each script, in case it was moved due to an error in the previous script.
# for aip_folder in os.listdir('.'):
#
#     # Updates the current AIP number and displays the script progress.
#     current_aip += 1
#     aip.log(log_path, f'\n{aip_folder}')
#     print(f'\nProcessing {aip_folder} ({current_aip} of {total_aips}).')
#
#     # Calculates the department group name in ARCHive from the folder name (the AIP ID).
#     # If it does not match the expected pattern, moves the AIP to an error folder and begins processing the next AIP.
#     if aip_folder.startswith('harg'):
#         department = 'hargrett'
#     elif aip_folder.startswith('magil'):
#         department = 'magil'
#     elif aip_folder.startswith('rbrl'):
#         department = 'russell'
#     else:
#         aip.log(log_path, f'AIP ID {aip_folder} does not start with an expected department prefix. '
#                           f'AIP moved to error folder.')
#         aip.move_error('department_prefix', aip_folder)
#         continue
#
#     # Extracts technical metadata from the files using FITS.
#     if aip_folder in os.listdir('.'):
#         aip.extract_metadata(aip_folder, f'{c.script_output}/{aips_directory}', log_path)
#
#     # Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets.
#     if aip_folder in os.listdir('.'):
#         aip.make_preservationxml(aip_folder, aip_to_title[aip_folder], department, 'website', log_path)
#
#     # Bags the aip.
#     if aip_folder in os.listdir('.'):
#         aip.bag(aip_folder, log_path)
#
#     # Tars, and zips the aip.
#     if f'{aip_folder}_bag' in os.listdir('.'):
#         aip.package(aip_folder, os.getcwd(), zip=True)
#
# # Makes MD5 manifests of all AIPs the in this download using md5deep, with one manifest per department.
# aip.make_manifest()
#
# # Verifies the AIPs are complete and no extra AIPs were created. Does not look at the errors folder, so any AIPs with
# # errors will show as missing. Saves the result as a csv in the folder with the downloaded AIPs.
# print('\nStarting completeness check.')
# web.check_aips(date_end, date_start, seed_to_aip, log_path)
# print('\nFinished completeness check. See completeness_check_YYYY-MM-DD.csv for details.')
#
# # Adds completion of the script to the log.
# aip.log(log_path, f'\nScript finished running at {datetime.datetime.today()}.')
#
# # Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) into the AIPs folder for this
# # download to keep everything together if another set is downloaded before these are deleted.
# os.chdir(c.script_output)
# to_move = ['aips-to-ingest', 'errors', 'fits-xml', 'preservation-xml',
#            f'web_preservation_download_log_{date_end}.txt']
# for item in os.listdir('.'):
#     if item in to_move:
#         os.replace(item, f'{aips_directory}/{item}')
