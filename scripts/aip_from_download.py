# The WARCs and metadata files for a group of web AIPs were downloaded but the AIPs still need to be made.
# Using a separate script for ease of customization, but basically it is the second half of web_aip_batch.
# Extras: creates metadata dictionaries from a csv with the information and extra checking.

# Usage: python path/aip_from_download.py path/aips_directory date_start date_end

import csv
import datetime
import os
import re
import requests
import sys

# Import functions and constant variables from other UGA scripts.
import aip_functions as a
import configuration as c
import web_functions as web


# PART ONE: ANY STEPS THAT ARE USUALLY DONE IN web_aip_batch.py NEEDED FOR THE AIP STAGE

# Script arguments.
try:
    aips_directory, date_start, date_end = sys.argv[1:]
    os.chdir(aips_directory)
except:
    print("Something is wrong with the script arguments.")
    print("Expect full file path for aips directory, date start (yyyy-mm-dd) and date end (yyyy-mm-dd).")
    sys.exit()

# Tests the paths in the configuration file to verify they exist. Quits the script if any are incorrect.
# It is common to get typos when setting up the configuration file on a new machine.
configuration_errors = a.check_configuration()
if len(configuration_errors) > 0:
    print("/nProblems detected with configuration.py:")
    for error in configuration_errors:
        print(error)
    print("Correct the configuration file and run the script again.")
    sys.exit()

# Dictionary that maps seed id to aip id for the completeness check.
# Will add the values when reading the metadata.csv for each AIP.
seed_to_aip = {}

# PART TWO: CREATE AIPS THAT ARE READY FOR INGEST INTO ARCHIVE

# Verifies the metadata.csv file is present.
aip_metadata_csv = "metadata.csv"
if not os.path.exists(aip_metadata_csv):
    print("Cannot make AIPs from the downloaded content because metadata.csv is missing.")
    sys.exit()

# Reads the metadata.csv and verifies that the contents are formatted correctly.
open_metadata = open(aip_metadata_csv)
read_metadata = csv.reader(open_metadata)
metadata_errors = a.check_metadata_csv(read_metadata)
if len(metadata_errors) > 0:
    print('\nProblems detected with metadata.csv:')
    for error in metadata_errors:
        print("   * " + error)
    print('\nCannot make AIPs from the downloaded content.')
    sys.exit()

# Starts a log for AIP information.
a.log("header")

# Makes directories used to store script outputs, if they aren't already there.
a.make_output_directories()

# Starts counts for tracking script progress. Some processes are slow, so this shows the script is still working.
# Subtracts one from the count for the metadata file.
current_aip = 0
total_aips = len(os.listdir('.')) - 1

# Returns to the beginning of the CSV (the script is at the end because of checking it for errors) and skips the header.
open_metadata.seek(0)
next(read_metadata)

# Uses the AIP functions to create an AIP for each folder in the metadata CSV.
# Checks if the AIP folder is still present before calling the function for the next step
# in case it was moved due to an error in the previous step.
for aip_row in read_metadata:

    # Makes an instance of the AIP class using metadata from the CSV and global variables.
    department, collection_id, aip_folder, aip_id, title, version = aip_row
    aip = a.AIP(aips_directory, department, collection_id, aip_folder, aip_id, title, version, True)

    # Updates the current AIP number and displays the script progress.
    current_aip += 1
    print(f"\tStarting AIP {current_aip} of {total_aips}.")

    # Verifies the metadata and objects folders exist and have content.
    # This is unlikely but could happen if there were uncaught download errors.
    web.check_directory(aip)

    # Deletes any temporary files and makes a log of each deleted file.
    a.delete_temp(aip)

    # Calculates the seed id from the first WARC in the objects folder and
    # adds the AIP to the seed_to_aip dictionary.
    first_warc = next(warc for warc in os.listdir(f"{aip.id}/objects"))
    try:
        seed_regex = re.match(r'^.*-SEED(\d+)-', first_warc)
        seed_id = seed_regex.group(1)
    except AttributeError:
        print("Can't extract seed id from: ", first_warc)
        continue
    seed_to_aip[seed_id] = aip.id

    # Extracts technical metadata from the files using FITS.
    if aip_folder in os.listdir('.'):
        a.extract_metadata(aip)

    # Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets.
    if aip_folder in os.listdir('.'):
        a.make_preservationxml(aip, "website")

    # Bags the aip.
    if aip_folder in os.listdir('.'):
        a.bag(aip)

    # Tars, and zips the aip.
    if f'{aip_folder}_bag' in os.listdir('.'):
        a.package(aip)

    # Adds the packaged AIP to the MD5 manifest in the aips-to-ingest folder.
    if f'{aip.id}_bag' in os.listdir('.'):
        a.manifest(aip)

# Closes the metadata CSV.
open_metadata.close()

# Verifies the AIPs are complete and no extra AIPs were created. Does not look at the errors folder, so any AIPs with
# errors will show as missing. Saves the result as a csv in the folder with the downloaded AIPs.
print('\nStarting completeness check.')
web.check_aips(date_end, date_start, seed_to_aip)

# Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) and logs into the AIPs folder
# to keep everything together if another set is downloaded before these are deleted.
os.chdir(c.script_output)
to_move = ("aips-to-ingest", "errors", "fits-xml", "preservation-xml",
           "warc_log.csv", "aip_log.csv", "completeness_check.csv")
for item in os.listdir("."):
    if item in to_move:
        os.replace(item, f"{aips_directory}/{item}")

print('\nScript is complete.')
