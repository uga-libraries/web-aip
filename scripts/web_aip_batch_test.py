"""Purpose: This script generates every known error to use for testing the error handling of web_aip_batch.py.

Usage: python /path/web_aip_batch_test.py date_start date_end
    for date_start, use ?????
    for date_end, use ????

"""

import os
import re
import sys

# Import functions and constant variables from other UGA scripts.
import aip_functions as aip
import configuration as c
import web_functions as web

# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT IS THE SAME AS web_aip_batch.py TO SET UP EVERYTHING CORRECTLY BEFORE THE DESIRED TESTS.
# ERROR HANDLING FOR SCRIPT ARGUMENTS AND THE CONFIGURATION FILE ARE TESTED BY GIVING THE WRONG INPUTS INSTEAD.
# ----------------------------------------------------------------------------------------------------------------

# Gets the start and end dates from script arguments and verify their formatting.
try:
    date_start, date_end = sys.argv[1:]
except IndexError:
    print("Exiting script: must provide exactly two arguments, the start and end date of the quarter.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_start):
    print(f"Exiting script: start date '{date_start}' must be formatted YYYY-MM-DD.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_end):
    print(f"Exiting script: end date '{date_end}' must be formatted YYYY-MM-DD.")
    exit()

# Tests the paths in the configuration file.
valid_errors = aip.check_paths()
if not valid_errors == "no errors":
    print('The following path(s) in the configuration file are not correct:')
    for error in valid_errors:
        print(error)
    print('Correct the configuration file and run the script again.')
    sys.exit()

# Makes a folder for AIPs within the script_output folder and makes it the current directory.
aips_directory = f'aips_{date_end}'
if not os.path.exists(f'{c.script_output}/{aips_directory}'):
    os.makedirs(f'{c.script_output}/{aips_directory}')
os.chdir(f'{c.script_output}/{aips_directory}')

# Downloads information from APIs, starts variables for tracking information, and starts the warc log.
warc_metadata = web.warc_data(date_start, date_end)
seed_metadata = web.seed_data(warc_metadata, date_end)
current_warc = 0
total_warcs = warc_metadata['count']
seed_to_aip = {}
aip_to_title = {}
web.warc_log("header")

# ----------------------------------------------------------------------------------------------------------------
# THIS PART OF THE SCRIPT MAKES A DIFFERENT ERROR EVERY TIME IT STARTS A NEW WARC.
# FOR ERRORS GENERATING WITHIN FUNCTIONS, IT USES A DIFFERENT VERSION OF THE FUNCTION.
# PRINTS AN ERROR TO THE TERMINAL IF THE ERROR IS NOT CAUGHT AND STARTS THE LOOP WITH THE NEXT WARC.
# ----------------------------------------------------------------------------------------------------------------

for warc in warc_metadata['files']:

    # Starts a dictionary of information for the log.
    log_data = {"filename": warc['filename'], "seed_id": "n/a", "warc_json": "n/a", "job_id": "n/a",
                "seed_metadata": "n/a", "seed_report": "n/a", "seedscope_report": "n/a", "collscope_report": "n/a",
                "coll_report": "n/a", "crawljob_report": "n/a", "crawldef_report": "n/a", "warc_api": "n/a",
                "md5deep": "n/a", "fixity": "n/a", "complete": "Errors during WARC processing."}

    # Updates the current WARC number and displays the script progress.
    current_warc += 1
    print(f"\nProcessing {warc['filename']} ({current_warc} of {total_warcs}).")

    # ERROR 1: Cannot extract seed_id from the WARC filename.
    if current_warc == 1:

        # Generate the error by updating the value of warc['filename'].
        warc["filename"] = "warc-name-error.warc.gz"

        # Calculates seed id, which is a portion of the WARC filename between "-SEED" and "-".
        try:
            regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc['filename'])
            seed_id = regex_seed_id.group(1)
            log_data["seed_id"] = "Successfully calculated seed id."
        except AttributeError:
            log_data["seed_id"] = "Could not calculate seed id from the WARC filename."
            web.warc_log(log_data)
            continue

        # Should catch the error in the previous step and this should not run.
        print("Did not catch error 1 correctly.")
        continue

    # ERROR 2: Cannot find expected values in WARC JSON (KeyError).
    if current_warc == 2:

        # Step before the error. Should work.
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', warc['filename'])
        seed_id = regex_seed_id.group(1)
        log_data["seed_id"] = "Successfully calculated seed id."

        # Generate the error by removing two fields needed for variables.
        warc["checksums"].pop("md5")
        warc.pop("collection")

        # Saves relevant information about the WARC in variables for future use.
        # Stops processing if the WARC does not have the required metadata.
        try:
            warc_filename = warc['filename']
            warc_url = warc['locations'][0]
            warc_md5 = warc['checksums']['md5']
            warc_collection = warc['collection']
        except (KeyError, IndexError):
            log_data["warc_json"] = f"Could not find at least one expected value in JSON: {warc}"
            web.warc_log(log_data)
            continue

        # Should catch the error in the previous step and this should not run.
        print("Did not catch error 2 correctly.")
        continue
