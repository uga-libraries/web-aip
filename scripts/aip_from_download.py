# The WARCs and metadata files for a group of web AIPs were downloaded but the AIPs still need to be made.
# Using a separate script for ease of customization, but basically it is the second half of web_aip_batch.

# Before running the script:
#   * The AIPs directory is named aips_date_end
#   * The AIPs directory contains one folder per AIP, named with the AIP ID
#   * The AIP folders contain a metadata folder and objects folder
#   * The metadata folder contains all Archive-It metadata reports
#   * The objects folder contains all WARCs, which are unzipped
#   * The parent folder of the AIPs directory contains:
#           * seeds.csv, with data through "Seed_Metadata_Errors" plus "Size_GB"
#           * aip_log.csv, with just the header
#           * script output folders (aips-to-ingest, fits-xml, preservation-xml), all empty
#           * optional: warc_unzip_log.csv (if had to unzip in Linux OS)

# Usage: python path/aip_from_download.py date_start date_end

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


# PART ONE: TEST SCRIPT INPUTS AND MAKE OTHER GLOBAL VARIABLES
# This is web_aip_batch.py but without making script output folders or the AIP log.

# Tests the script arguments (start and end date, which should be formatted YYYY-MM-DD).
try:
    date_start, date_end = sys.argv[1:]
except ValueError:
    print("\nExiting script: must provide exactly two arguments, the start and end date of the download.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_start):
    print(f"\nExiting script: start date '{date_start}' must be formatted YYYY-MM-DD.")
    exit()
if not re.match(r"\d{4}-\d{2}-\d{2}", date_end):
    print(f"\nExiting script: end date '{date_end}' must be formatted YYYY-MM-DD.")
    exit()
if date_start > date_end:
    print(f"\nExisting script: start date '{date_start}' must be earlier than end date '{date_end}'.")
    exit()

# Tests the paths in the configuration file to verify they exist. Quits the script if any are incorrect.
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

# Makes the AIPs directory the current directory.
aips_directory = os.path.join(c.script_output, f"aips_{date_end}")
os.chdir(aips_directory)

# Reads the data in seeds.csv into a dataframe.
seed_df = pd.read_csv(os.path.join(c.script_output, "seeds.csv"), dtype="object")

# Starts counter for tracking script progress.
# Some processes are slow, so this shows the script is still working and how much remains.
current_seed = 0
total_seeds = len(seed_df[(seed_df["Seed_Metadata_Errors"].str.startswith("Successfully"))
                          & (seed_df["WARC_Unzip_Errors"].isnull())])

# PART TWO: CREATE AIPS THAT ARE READY FOR INGEST INTO ARCHIVE FROM EACH AIP FOLDER
# This is like the loop in web_aip_batch.py but without making the AIP directory or downloading files.
for seed in seed_df[(seed_df["Seed_Metadata_Errors"].str.startswith("Successfully"))
                    & (seed_df["WARC_Unzip_Errors"].isnull())].itertuples():

    # Updates the current seed number and displays the script progress.
    current_seed += 1
    print(f"\nStarting seed {current_seed} of {total_seeds}.")

    # Makes an instance of the AIP class using metadata from the seeds.csv and global variables.
    aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, version=1, to_zip=True)

    # Verifies the metadata and objects folders exist and have content.
    # This is unlikely but could happen if there were uncaught download errors.
    web.check_directory(aip)

    # Deletes any temporary files and makes a log of each deleted file.
    a.delete_temp(aip)

    # Extracts technical metadata from the files using FITS.
    if aip.id in os.listdir('.'):
        a.extract_metadata(aip)

    # Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets.
    if aip.id in os.listdir('.'):
        a.make_preservationxml(aip)

    # Bags the aip.
    if aip.id in os.listdir('.'):
        a.bag(aip)

    # Tars and zips the aip.
    if f'{aip.id}_bag' in os.listdir('.'):
        a.package(aip)

    # Adds the packaged AIP to the MD5 manifest in the aips-to-ingest folder.
    if f'{aip.id}_bag' in os.listdir('.'):
        a.manifest(aip)

# PART THREE: UPDATE LOG, VERIFY AIP COMPLETENESS, AND CLEAN UP DIRECTORY
# This is the same as web_aip_batch.py except for the optional fixity check with the unzip log.

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

# If there is a warc_unzip_log.csv, verifies the fixity matches what is in the bag manifests.
if os.path.exists("warc_unzip_log.csv"):

    # Dataframe with WARC filename and MD5 from the unzipping log.
    # Removes ".gz" from the end of the WARC name and removes extra columns so it matches the bag manifest.
    log_df = pd.read_csv("warc_unzip_log.csv")
    log_df["WARC"] = log_df["WARC"].str.replace(".warc.gz", ".warc", regex=False)
    log_df.drop(["AIP", "Zip_MD5", "Fixity_Comparison", "Unzipping_Result"], inplace=True, axis=1)

    # Dataframe that combines the WARc rows from the md5 manifests from every bag.
    # Removes the path from the WARC filename so it matches what is in the warc unzip log.
    bag_df = pd.DataFrame(columns=["Unzip_MD5", "WARC"])
    for root, dirs, files in os.walk(f"aips_{date_end}"):
        if "manifest-md5.txt" in files:
            manifest_path = os.path.join(root, "manifest-md5.txt")
            manifest_df = pd.read_csv(manifest_path, names=["Unzip_MD5", "Extra_Space", "WARC"], sep=" ")
            manifest_df.drop(["Extra_Space"], inplace=True, axis=1)
            bag_df = pd.concat([bag_df, manifest_df], ignore_index=True)
    bag_df = bag_df[bag_df["WARC"].str.endswith(".warc")]
    bag_df["WARC"] = bag_df["WARC"].str.replace("data/objects/", "")

    # Compares the two dataframes. If they don't match, saves an error log.
    # It will be moved into the AIPs directory in the next step
    df = log_df.merge(bag_df, indicator=True, how="outer")
    if len(df[df["_merge"] != "both"]) > 0:
        df.to_csv(f"warc_md5_differences.csv", index=False)
    else:
        print("All WARC fixity was unchanged")

# Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) and logs into the AIPs folder
# to keep everything together if another set is downloaded before these are deleted.
to_move = ("aips-to-ingest", "errors", "fits-xml", "preservation-xml",
           "seeds.csv", "completeness_check.csv", "warc_md5_differences.csv", "warc_unzip_log.csv")
for item in os.listdir("."):
    if item in to_move:
        os.replace(item, f"{aips_directory}/{item}")

print('\nScript is complete.')
