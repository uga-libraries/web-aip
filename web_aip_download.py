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

# Usage: python /path/web_aip_download.py date_start date_end

import os
import pandas as pd
import re
import sys

# Import functions and constant variables from other UGA scripts.
# Configuration is made by the user and could be forgotten. The others are in the script repo.
try:
    import configuration as c
except ModuleNotFoundError:
    print("\nScript cannot run without a configuration file in the local copy of the GitHub repo.")
    print("Make a file named configuration.py using configuration_template.py and run the script again.")
    sys.exit()
import aip_functions as a
import web_functions as web

# The preservation download is limited to warcs created during a particular time frame.
# UGA downloads every quarter (2/1-4/30, 5/1-7/31, 8/1-10/31, 11/1-1/31)
# Tests that both dates are provided. If not, ends the script.
try:
    date_start, date_end = sys.argv[1:]
except ValueError:
    print("\nExiting script: must provide exactly two arguments, the start and end date of the download.")
    exit()

# Tests that both dates are formatted correctly (YYYY-MM-DD). If not, ends the script.
if not re.match(r"\d{4}-\d{2}-\d{2}", date_start):
    print(f"\nExiting script: start date '{date_start}' must be formatted YYYY-MM-DD.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_end):
    print(f"\nExiting script: end date '{date_end}' must be formatted YYYY-MM-DD.")
    exit()

# Tests that the first date is earlier than the second date. If not, ends the script.
if date_start > date_end:
    print(f"\nExisting script: start date '{date_start}' must be earlier than end date '{date_end}'.")
    exit()

# Tests the paths in the configuration file to verify they exist. Quits the script if any are incorrect.
# Uses two check_configuration functions, one web specific and one AIP specific, for a complete test.
# It is common to get typos when setting up the configuration file on a new machine.
configuration_web = web.check_configuration()
configuration_aip = a.check_configuration()
configuration_errors = configuration_web + configuration_aip
if len(configuration_errors) > 0:
    print("\nProblems detected with configuration.py:")
    for error in configuration_errors:
        print(error)
    print("\nCorrect the configuration file and run the script again.")
    sys.exit()

# Path to the folder in the script output directory (defined in the configuration file)
# where everything related to this download will be saved.
aips_directory = os.path.join(c.script_output, f"aips_{date_end}")

# The script may be run repeatedly if there are interruptions, such as due to API connections.
# If the AIPs directory is already present, that means the script has run before.
# It will use the seeds.csv, aip_log.csv, and output folders already there and skip seeds that were already done.
if os.path.exists(aips_directory):
    os.chdir(aips_directory)
    seed_df = pd.read_csv(os.path.join(c.script_output, "seeds.csv"), dtype="object")
# If the AIPs directory is not there, this is the first time the script is being run.
# It will make the AIPs directory, a new seeds.csv, the output folders needed, and start the aip_log.csv.
else:
    os.makedirs(aips_directory)
    os.chdir(aips_directory)
    seed_df = web.seed_data(date_start, date_end)
    a.make_output_directories()
    a.log("header")

# Starts counter for tracking script progress.
# Some processes are slow, so this shows the script is still working and how much remains.
current_seed = 0
total_seeds = len(seed_df[(seed_df["Seed_Metadata_Errors"].str.startswith("Successfully"))
                          & (seed_df["WARC_Unzip_Errors"].isnull())])

# Iterates through information about each seed, downloading metadata and WARC files from Archive-It
# and creating AIPs ready for ingest into the digital preservation system.
# Filtered for no data in the Seed_Metadata_Errors to skip seeds without the required metadata in Archive-It.
# Filtered for no data in the WARC_Fixity_Errors column to skip seeds done earlier if this is a restart.
for seed in seed_df[(seed_df["Seed_Metadata_Errors"].str.startswith("Successfully"))
                    & (seed_df["WARC_Unzip_Errors"].isnull())].itertuples():

    # Updates the current seed number and displays the script progress.
    current_seed += 1
    print(f"\nStarting seed {current_seed} of {total_seeds}.")

    # Makes the AIP directory for the seed (AIP folder with metadata and objects subfolders).
    # If the seed already has an AIP directory from an error in a previous iteration of the script,
    # deletes the contents and anything in the seeds.csv from the previous step so it can be remade.
    if os.path.exists(seed.AIP_ID):
        web.reset_aip(seed.AIP_ID, seed_df)
    os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
    os.makedirs(os.path.join(seed.AIP_ID, "objects"))

    # Downloads the seed metadata from Archive-It into the seed's metadata folder.
    web.download_metadata(seed, seed_df)

    # Downloads the WARCs from Archive-It into the seed's objects folder.
    web.download_warcs(seed, date_end, seed_df)

    # Makes an instance of the AIP class, using seed dataframe and calculating additional values.
    # If there was an error when making the instance, starts the next AIP.
    # Creates the AIP instance and returns it.
    aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, version=1, to_zip=True)

    # Verifies the metadata and objects folders exist and have content.
    # This is unlikely but could happen if there were uncaught download errors.
    if aip.id in os.listdir("."):
        web.check_directory(aip)

    # Deletes any temporary files and makes a log of each deleted file.
    if aip.id in os.listdir("."):
        a.delete_temp(aip)

    # Extracts technical metadata from the files using FITS.
    if aip.id in os.listdir("."):
        a.extract_metadata(aip)

    # Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets.
    if aip.id in os.listdir("."):
        a.make_preservationxml(aip)

    # Bags the aip.
    if aip.id in os.listdir("."):
        a.bag(aip)

    # Tars and zips the aip.
    if f"{aip.id}_bag" in os.listdir('.'):
        a.package(aip)

    # Adds the packaged AIP to the MD5 manifest in the aips-to-ingest folder.
    if f'{aip.id}_bag' in os.listdir('.'):
        a.manifest(aip)

# Adds the information from aip_log.csv to seeds.csv and deletes aip_log.csv
# to have one spreadsheet the documents the entire process.
os.chdir(c.script_output)
aip_df = pd.read_csv("aip_log.csv")
seed_df = pd.merge(seed_df, aip_df, left_on="AIP_ID", right_on="AIP ID", how="left")
seed_df.drop(["Time Started", "AIP ID"], axis=1, inplace=True)
seed_df.to_csv("seeds.csv", index=False)
os.remove("aip_log.csv")

# Verifies the AIPs are complete and no extra AIPs were created. Does not look at the errors folder, so any AIPs with
# errors will show as missing. Saves the result as a csv in the folder with the downloaded AIPs.
print('\nStarting completeness check.')
web.check_aips(date_end, date_start, seed_df, aips_directory)

# Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) and logs into the AIPs folder
# to keep everything together if another set is downloaded before these are deleted.
to_move = ("aips-to-ingest", "errors", "fits-xml", "preservation-xml",
           "seeds.csv", "completeness_check.csv")
for item in os.listdir("."):
    if item in to_move:
        os.replace(item, f"{aips_directory}/{item}")

print('\nScript is complete.')
