"""Purpose: Downloads archived web content and associated metadata for a single seed from Archive-It.org using their
APIs and converts it into an AIP that is ready to ingest into the UGA Libraries' digital preservation system (ARCHive).

Dependencies:
    * Python libraries: requests
    * Tools: bagit.py, fits, md5deep, saxon, xmllint

Prior to the preservation download, all seed metadata should be entered into Archive-It.

Ideas for improvement: make a log of completeness check instead of printing to the terminal to have a record;
include time in log start date and calculate how long it took to run.
"""

# Usage: python /path/web_aip_single.py seed_id aip_id collection_id date_start date_end

import datetime
import os
import re
import requests
import sys

# Import functions and constant variables from other UGA scripts.
import aip_functions as aip
import configuration as c
import web_functions as web


def get_title():
    """Returns the AIP title from the seed report. Replaces the functionality of seed_data(), which also calculates
    the AIP ID for batch downloads. """

    # Uses the Partner API to get data about this seed.
    seed_report = requests.get(f"{c.partner_api}/seed?id={seed_id}", auth=(c.username, c.password))

    # If there was an error with the API call, quits the script.
    if not seed_report.status_code == 200:
        aip.log(log_path, f"\nAPI error {seed_report.status_code} for seed report.")
        print("API error, ending script. See log for details.")
        exit()

    # Converts the seed data from json to a Python object.
    py_seed_report = seed_report.json()

    # Gets the title for the seed from the Title field.
    # Raises an error if the title is missing so the script stops processing the seed. Title is required.
    try:
        title = py_seed_report[0]['metadata']['Title'][0]['value']
    except (KeyError, IndexError):
        raise ValueError

    return title


def check_aip():
    """Verifies a single AIP is complete, checking the contents of the metadata and objects folders. Prints any
    errors to the terminal. This is different from the check_aips() function used by web_aip_batch.py since it has to
    filter by seed id and is only checking one AIP. """
    # TODO: this is outdated for how it expects fits and crawldef to be named.

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

            if crawl_date < date_start:
                warcs_exclude += 1
                continue

            # If the WARC was not excluded by the previous two filters, it should be included in the AIP.
            warcs_include += 1

        # Checks that the right number of WARCs were evaluated.
        if warcs_from_api != warcs_include + warcs_exclude:
            aip.log(log_path, "Script did not review expected number of WARCs.")
            raise ValueError

        return warcs_include

    # Starts a section in the log for the completeness check.
    aip.log(log_path, '\nCompleteness check results:')

    # Saves the file paths to the metadata and objects folders to variables, since they are long and reused.
    objects = f"{c.script_output}/aips_{date_end}/{aip_id}_bag/data/objects"
    metadata = f"{c.script_output}/aips_{date_end}/{aip_id}_bag/data/metadata"

    # List of suffixes used for the expected metadata reports.
    expected_endings = ("coll.csv", "collscope.csv", "crawldef.csv", "crawljob.csv", "seed.csv",
                        "seedscope.csv", "preservation.xml", "fits.xml")

    # Calculates the number of WARCs that should be in this AIP. Exits the function if it is not calculated since
    # multiple tests depend on this.
    try:
        warcs_expected = aip_warcs_count()
    except ValueError:
        aip.log(log_path, "Cannot check AIP for completeness. WARC count not calculated.")

    # Variable tracks if anything has been found missing so a summary can be printed to the terminal.
    missing = False

    # Tests if there is a folder for this AIP in the AIPs directory.
    if not any(folder.startswith(aip_id) for folder in os.listdir(f'{c.script_output}/aips_{date_end}')):
        aip.log(log_path, 'The AIP folder was not created.')
        missing = True

    # Tests if each of the expected metadata reports is present.
    # Skips crawldef, crawljob, and FITS because the filenames are formatted
    # differently and are checked in the next test.
    for end in expected_endings:
        if end in ("crawldef.csv", "crawljob.csv", "fits.xml"):
            continue
        if not os.path.exists(f'{metadata}/{aip_id}_{end}'):
            aip.log(log_path, f'{end} was not created.')
            missing = True

    # Saves the number of crawldef and crawljob reports to the log so staff can verify the count.
    crawldef_count = len([file for file in os.listdir(metadata) if file.endswith('_crawldef.csv')])
    aip.log(log_path, f'Number of crawl definitions: {crawldef_count}.')
    crawljob_count = len([file for file in os.listdir(metadata) if file.endswith('_crawljob.csv')])
    aip.log(log_path, f'Number of crawl jobs: {crawljob_count}.')

    # Tests if the number of FITS files is correct (one for each WARC).
    fits_count = len([file for file in os.listdir(metadata) if file.endswith("_fits.xml")])
    if not fits_count == warcs_expected:
        aip.log(log_path, f'The number of FITS files is incorrect: {fits_count} instead of {warcs_expected}.')
        missing = True

    # Tests if everything in the AIP's metadata folder is an expected file type.
    for file in os.listdir(metadata):
        if not file.endswith(expected_endings):
            aip.log(log_path, f'File in metadata folder that is not expected: {file}')
            missing = True

    # Tests if the number of WARCs is correct.
    warcs_in_objects = len([file for file in os.listdir(objects) if file.endswith(".warc.gz")])
    if not warcs_expected == warcs_in_objects:
        aip.log(log_path, f'The number of WARCs is incorrect: {warcs_in_objects} instead of {warcs_expected}')
        missing = True

    # Tests if everything in the AIP's objects folder is a WARC.
    for file in os.listdir(objects):
        if not file.endswith('.warc.gz'):
            aip.log(log_path, f'File in objects folder that is not a WARC: {file}.')
            missing = True

    # Log if nothing was missing.
    # If some things were missing, they are already in the log and no additional indication is needed.
    if missing is False:
        aip.log(log_path, f'The AIP is complete.')

    # Return value of missing to print a summary to the terminal.
    return missing


# Tests required script arguments were provided and assigns to variables. If one isn't, ends the script.
# Skips sys.argv[0] when assigning variables since that is the path to the script.
try:
    seed_id, aip_id, collection_id, date_start, date_end = sys.argv[1:]
except ValueError:
    print("Exiting script: missing at least one required argument.")
    print("Must include seed ID, AIP ID, collection ID, date start, and date end")
    exit()

# Tests the formatting on the date start and end, which define the time frame the desired warcs were saved.
# Must be formatted YYYY-MM-DD. If one isn't, ends the script.
if not re.match(r"\d{4}-\d{2}-\d{2}", date_start):
    print(f"Exiting script: start date '{date_start}' must be formatted YYYY-MM-DD.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_end):
    print(f"Exiting script: end date '{date_end}' must be formatted YYYY-MM-DD.")
    exit()

# Extracts the department from the aip_id saves it to a variable. Quits the script if the AIP id is formatted wrong.
try:
    regex_dept = re.match('^(harg|magil|rbrl).*', aip_id)
    dept_code = regex_dept.group(1)
except AttributeError:
    print(f"Exiting script: AIP id '{aip_id}' is not formatted correctly. Department could not be identified.")
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

print(f"Making AIP for {seed_id}.")

# Makes a folder for aips within the script_output folder, a designated place on the local machine for web archiving
# documents. The folder name includes today's date to keep it separate from previous downloads which may still be
# saved on the same machine. current_download is a variable because it is also used as part of the quality_control
# function, and depending on how long it takes to download WARCs, recalculating today() may give a different result.

aips_directory = f"aips_{date_end}"
if not os.path.exists(f"{c.script_output}/{aips_directory}"):
    os.makedirs(f"{c.script_output}/{aips_directory}")

# Changes current directory to the aips folder.
os.chdir(f"{c.script_output}/{aips_directory}")

# Starts a log for saving status information about the script. Saving to a document instead of printing to the screen
# since it allows for a permanent record of the download and because the terminal closed at the end of a script when
# it is run automatically with chronjob. The log is not started until after the current_download variable is set so that
# can be included in the file name.
log_path = f'../web_preservation_download_log_{aip_id}.txt'
aip.log(log_path, f'Creating AIP {aip_id} (for seed {seed_id}) using the web_aip_single.py script. '
                  f'Script started running at {datetime.datetime.today()}.\n')


# PART ONE: DOWNLOAD WARCS AND METADATA INTO THE AIP DIRECTORY STRUCTURE.
print("Downloading AIP content.")

# Uses WASAPI to get information about the WARCs for this seed's collection. The WARC's seed id is checked before it
# is downloaded to skip other seeds from this collection. It is not possible to filter by seed using WASAPI.
warc_metadata = web.warc_data(date_start, date_end, log_path, collection_id)

# Uses the Archive-It Partner API to get the seed's title.
try:
    aip_title = get_title()
except ValueError:
    aip.log(log_path, "Seed has no title.")
    print('Exiting script: seed is missing metadata in Archive-It. See log for details.')

# Makes the aip directory for the seed's aip (aip folder with metadata and objects subfolders). Unlike with the batch
# script, the folder does not need to temporarily include the AIP title since the title is already stored in a variable.
web.make_aip_directory(aip_id)

# Iterates through information about each WARC.
for warc in warc_metadata['files']:

    # Saves relevant information about the WARC in variables to use for downloading the WARC.
    try:
        warc_filename = warc['filename']
        warc_url = warc['locations'][0]
        warc_md5 = warc['checksums']['md5']
    except (KeyError, IndexError):
        aip.log(log_path, 'WARC information is formatted wrong. JSON from API:\n')
        aip.log(log_path, warc)
        continue

    # Checks if the WARC is from the seed being downloaded. If not, skips the WARC.
    # The WARC's seed id is the portion of the WARC's filename between "-SEED" and "-".
    try:
        regex_seed_id = re.match(r"^.*-SEED(\d+)-", warc_filename)
        warc_seed_id = regex_seed_id.group(1)
    except AttributeError:
        aip.log(log_path, f'Cannot calculate the WARC seed id from {warc_filename}.')
        continue
    if not seed_id == warc_seed_id:
        continue

    # Calculates the job id from the WARC filename.
    try:
        regex_job_id = re.match(r"^.*-JOB(\d+)", warc_filename)
        job_id = regex_job_id.group(1)
    except AttributeError:
        aip.log(log_path, "Cannot calculate the WARC job id.")
        continue

    # Downloads the seed metadata from Archive-It into the seed's metadata folder.
    # While five of the reports are the same for each WARC, the crawl definition could be different.
    web.download_metadata(aip_id, collection_id, job_id, seed_id, date_end, log_path)

    # Downloads the warc from Archive-It into the seed's objects folder.
    web.download_warc(aip_id, warc_filename, warc_url, warc_md5, date_end, log_path)

# Checks for empty metadata or objects folders in the AIPs. These happens if there were uncaught download errors.
web.find_empty_directory(log_path)

# PART TWO: CREATE AIPS THAT ARE READY FOR INGEST INTO ARCHIVE
print("Converting into an AIP.")

# Makes directories used to store script outputs, if they aren't already there.
aip.make_output_directories()

# Extracts technical metadata from the files using FITS.
if aip_id in os.listdir('.'):
    aip.extract_metadata(aip_id, f"{c.script_output}/{aips_directory}", log_path)

# Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets. Determines the
# third argument (ARCHive group name) from the department code parsed from the folder name.
if aip_id in os.listdir('.'):
    group = {"harg": "hargrett", "magil": "magil", "rbrl": "russell"}
    department = group[dept_code]
    aip.make_preservationxml(aip_id, aip_title, department, "website", log_path)

# Bags the aip.
if aip_id in os.listdir('.'):
    aip.bag(aip_id, log_path)

# Tars, and zips the aip.
if f"{aip_id}_bag" in os.listdir('.'):
    aip.package(aip_id, os.getcwd(), zip=True)

# If the AIP has not been moved to the errors folder, verifies the AIP is complete.
# Anything that is missing is added to the log. A notification if anything was missing or not prints to the terminal.
if f'{aip_id}_bag' in os.listdir('.'):
    print('\nStarting completeness check.')
    any_missing = check_aip()
    if any_missing is True:
        print("At least one file is missing, although this may not be an error. See the log for details.")
    else:
        print("The AIP is complete.")

# Makes MD5 manifest of the AIP.
aip.make_manifest()

# Adds completion of the script to the log.
aip.log(log_path, f'\nScript finished running at {datetime.datetime.today()}.')
print('Script is complete.')
