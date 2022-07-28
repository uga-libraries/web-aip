"""Purpose: Test that the script is able to restart after errors. A pre-determined date range is used to allow
automatic verification that the test script has produced the expected outputs. The functions from web_functions.py
are used as much as possible for a more authentic test, but test versions of these functions are sometimes needed
to speed up the download process and to generate errors.

Usage: python /path/test_web_aip_batch_iteration
 """

import datetime
import os
import pandas as pd
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


def download_warcs(seed, date_end, seed_df, stop=False):
    """Makes a text file with the name of each WARC (for faster testing). Does not include fixity testing or unzipping,
    other than logging success. If stop is True, simulates the script breaking in the middle of downloading."""

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # Makes a list of the filenames for all WARCs for this seed.
    warc_names = seed.WARC_Filenames.split(",")

    # Creates a text file with the name of every WARC.
    for warc in warc_names:

        # TESTING ITERATION: SCRIPT BREAKS BEFORE DOWNLOADING ALL WARCS FOR A MULTI-WARC SEED.
        if stop is True and warc in ("ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191023012315001-00008-h3.warc.gz",
                                     "ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz"):
            print("Simulating script breaking before all warcs are downloaded.")
            sys.exit()

        # The path for where the WARC will be saved on the local machine (it is long and used twice in this script).
        # Removes ".gz" from the file extension to simulate the unzipping.
        warc_path = f'{c.script_output}/aips_{date_end}/{seed.AIP_ID}/objects/{warc}'
        new_warc_path = warc_path.replace(".warc.gz", ".warc")

        # Saves a text file with the warc filename.
        with open(new_warc_path, 'wb') as warc_file:
            warc_file.write(b"Testing Text")

        # Logs as if the entire function happened successfully.
        web.log(f"Successfully downloaded {warc}", seed_df, row_index, "WARC_API_Errors")
        web.log(f"Successfully verified {warc} fixity on {datetime.datetime.now()}", seed_df, row_index, "WARC_Fixity_Errors")
        web.log(f"Successfully unzipped {warc}", seed_df, row_index, "WARC_Unzip_Errors")


# FOR TESTING, DATES ARE PROVIDED. THESE ARE USUALLY SCRIPT ARGUMENTS.
date_start = "2019-11-21"
date_end = "2020-06-09"

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

    # TESTING ITERATION: TRACKS IF THE RESET FUNCTION IS CALLED THE SCRIPT ONLY BREAKS THE FIRST TIME.
    # THIS LETS A SEED BE THE CAUSE OF A BREAK MID-PROCESSING AND THEN BE CORRECTLY DOWNLOADED THE NEXT TIME.
    reset = False

    # Updates the current seed number and displays the script progress.
    current_seed += 1
    print(f"\nStarting seed {current_seed} of {total_seeds}.")

    # Makes the AIP directory for the seed (AIP folder with metadata and objects subfolders).
    # If the seed already has an AIP directory from an error in a previous iteration of the script,
    # deletes the contents and anything in the seeds.csv from the previous step so it can be remade.
    if os.path.exists(seed.AIP_ID):
        web.reset_aip(seed.AIP_ID, seed_df)
        reset = True
    os.makedirs(os.path.join(seed.AIP_ID, "metadata"))
    os.makedirs(os.path.join(seed.AIP_ID, "objects"))

    # Downloads the seed metadata from Archive-It into the seed's metadata folder.
    web.download_metadata(seed, seed_df)

    # TESTING ITERATION: SCRIPT BREAKS BETWEEN WEB-SPECIFIC STEPS FOR A SEED.
    if seed.Seed_ID == "2173769" and reset is False:
        print("Simulating script breaking after metadata download.")
        sys.exit()

    # Downloads the WARCs from Archive-It into the seed's objects folder.
    # FOR TESTING: uses local version of the function. Makes text file for warcs (faster) and errors when needed.
    # TESTING ITERATION: WILL STOP AFTER DOWNLOADING A WARC SPECIFIED BY NAME IN THE FUNCTION.
    if seed.Seed_ID in ("2089428", "2122426") and reset is False:
        download_warcs(seed, date_end, seed_df, stop=True)
    else:
        download_warcs(seed, date_end, seed_df)

    # Makes an instance of the AIP class, using seed dataframe and calculating additional values.
    # If there was an error when making the instance, starts the next AIP.
    # Creates the AIP instance and returns it.
    aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, version=1, to_zip=True)

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
        a.make_preservationxml(aip)

    # Bags the aip.
    if aip.id in os.listdir("."):
        a.bag(aip)

    # TESTING ITERATION: SCRIPT BREAKS BETWEEN GENERAL AIP STEPS FOR A SEED.
    if seed.Seed_ID == "2202440" and reset is False:
        print("Simulating script breaking after bagging.")
        sys.exit()

    # Tars and zips the aip.
    if f"{aip.id}_bag" in os.listdir('.'):
        a.package(aip)

    # Adds the packaged AIP to the MD5 manifest in the aips-to-ingest folder.
    if f'{aip.id}_bag' in os.listdir('.'):
        a.manifest(aip)

    # TESTING ITERATION: SCRIPT BREAKS BETWEEN TWO SEEDS.
    if seed.Seed_ID == "2084785":
        print("Simulating script breaking between processing two seeds.")
        sys.exit()

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
