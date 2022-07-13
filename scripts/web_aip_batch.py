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

import os
import re
import sys

# Import functions and constant variables from other UGA scripts.
import aip_functions as a
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
configuration_errors = a.check_configuration()
if len(configuration_errors) > 0:
    print("/nProblems detected with configuration.py:")
    for error in configuration_errors:
        print(error)
    print("Correct the configuration file and run the script again.")
    sys.exit()

# Makes a folder for AIPs within the script_output folder, a designated place on the local machine for web archiving
# documents). The folder name includes the end date for the download to keep it separate from previous downloads
# which may still be saved on the same machine.
aips_directory = os.path.join(c.script_output, f"aips_{date_end}")
if not os.path.exists(aips_directory):
    os.makedirs(aips_directory)

# Changes current directory to the AIPs folder.
os.chdir(aips_directory)

# Uses Archive-IT WASAPI to get information about seeds in this download.
# The information is also saved as a CSV (seeds.csv) in the script output folder.
seed_df = web.seed_data(date_start, date_end)

# Starts counter for tracking script progress.
# Some processes are slow, so this shows the script is still working and how much remains.
current_seed = 0
total_seeds = len(seed_df)

# Makes directories used to store script outputs and AIP log.
a.make_output_directories()
a.log("header")

# Makes a dictionary for mapping seed ids to AIP ids, which is needed at the end to test the script.
seed_to_aip = {}

# Iterates through information about each seed, downloading metadata and WARC files from Archive-It
# and creating AIPs ready for ingest into the digital preservation system.
for seed in seed_df.itertuples():

    # Updates the current seed number and displays the script progress.
    current_seed += 1
    print(f"\nStarting seed {current_seed} of {total_seeds}.")

    # Makes the AIP directory for the seed (AIP folder with metadata and objects subfolders).
    try:
        os.makedirs(os.path.join(seed.Seed_ID, "metadata"))
        os.makedirs(os.path.join(seed.Seed_ID, "objects"))
    except FileExistsError:
        print(f"\nCannot make AIP for seed {seed.Seed_ID}. Directory for seed already exists.")
        continue

    # Downloads the seed metadata from Archive-It into the seed's metadata folder.
    web.download_metadata(seed, date_end, seed_df)

    # Downloads the WARCs from Archive-It into the seed's objects folder.
    web.download_warcs(seed, date_end)

    # Makes an instance of the AIP class, using seed dataframe and calculating additional values.
    # If there was an error when making the instance, starts the next AIP.
    aip = web.make_aip_instance(seed, date_end)
    if aip == "API error for seed report":
        print("Unable to make an AIP for this seed due to an API error.")
        continue

    # Updates the seed_to_aip dictionary with this seed.
    seed_to_aip[seed.Seed_ID] = aip.id

    # Renames the seed folder to the AIP ID.
    os.replace(seed.Seed_ID, aip.id)

    # Replaces the seed ID prefixes for the metadata files with the AIP ID.
    for file in os.listdir(os.path.join(aip.id, "metadata")):
        if file.startswith(seed.Seed_ID):
            new_filename = file.replace(seed.Seed_ID, aip.id)
            os.replace(os.path.join(aip.id, "metadata", file), os.path.join(aip.id, "metadata", new_filename))

    # Verifies the metadata and objects folders exist and have content.
    # This is unlikely but could happen if there were uncaught download errors.
    web.check_directory(aip)

    # Deletes any temporary files and makes a log of each deleted file.
    a.delete_temp(aip)

    # Extracts technical metadata from the files using FITS.
    if aip.id in os.listdir("."):
        a.extract_metadata(aip)

    # Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets.
    if aip.id in os.listdir("."):
        a.make_preservationxml(aip, "website")

    # Bags the aip.
    if aip.id in os.listdir("."):
        a.bag(aip)

    # Tars and zips the aip.
    if f"{aip.id}_bag" in os.listdir('.'):
        a.package(aip)

    # Adds the packaged AIP to the MD5 manifest in the aips-to-ingest folder.
    if f'{aip.id}_bag' in os.listdir('.'):
        a.manifest(aip)

# Verifies the AIPs are complete and no extra AIPs were created. Does not look at the errors folder, so any AIPs with
# errors will show as missing. Saves the result as a csv in the folder with the downloaded AIPs.
print('\nStarting completeness check.')
web.check_aips(date_end, date_start, seed_to_aip, aips_directory)

# Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) and logs into the AIPs folder
# to keep everything together if another set is downloaded before these are deleted.
os.chdir(c.script_output)
to_move = ("aips-to-ingest", "errors", "fits-xml", "preservation-xml",
           "seeds.csv", "aip_log.csv", "completeness_check.csv")
for item in os.listdir("."):
    if item in to_move:
        os.replace(item, f"{aips_directory}/{item}")

print('\nScript is complete.')
