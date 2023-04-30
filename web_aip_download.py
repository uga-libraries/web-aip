"""
Purpose: Downloads archived web content (WARCs) and associated metadata for a group of seeds from Archive-It.org using
their APIs and prepares them to be converted into AIPs with the general-aip.py script for long-term preservation.
At UGA, this script is run every three months to download content for all crawls saved that quarter.

There will be one AIP per seed, even if that seed was crawled multiple times in the same quarter.
A seed may have multiple AIPs in the system, as a new AIP is made for every quarter's crawls.

Prior to the preservation download, all seed metadata should be entered into Archive-It.
Use the seed_metadata_report.py script to verify all required fields are present.
"""

# Usage: python web_aip_download.py date_start date_end

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

# Verifies the configuration file has the correct values, and quits the script if not.
web.check_config()

# Path to the folder in the script output directory (defined in the configuration file)
# where everything related to this download will be saved.
aips_directory = os.path.join(c.script_output, f"aips_{date_end}")

# The script may be run repeatedly if there are interruptions, such as due to API connections.
# If it has run, it will use the existing seeds_log.csv for seed_df and and skip seeds that were already done.
# Otherwise, it makes seed_df by getting data from the Archive-It APIs.
if os.path.exists(aips_directory):
    os.chdir(aips_directory)
    seed_df = pd.read_csv(os.path.join(c.script_output, "seeds_log.csv"), dtype="object")
else:
    os.makedirs(aips_directory)
    os.chdir(aips_directory)
    seed_df = web.seed_data(date_start, date_end)

# Starts counter for tracking script progress.
# Some processes are slow, so this shows the script is still working and how much remains.
current_seed = 0
total_seeds = len(seed_df[seed_df["WARC_Unzip_Errors"].isnull()])

# Iterates through information about each seed, downloading metadata and WARC files from Archive-It.
# Filtered for no data in the WARC_Unzip_Errors (last log column) to skip seeds done earlier if this is a restart.
for seed in seed_df[seed_df["WARC_Unzip_Errors"].isnull()].itertuples():

    # Updates the current seed number and displays the script progress.
    current_seed += 1
    print(f"\nStarting seed {current_seed} of {total_seeds}.")

    # If the seed already has a folder from an error in a previous iteration of the script,
    # deletes the contents and anything in the seeds_log.csv from the previous iteration so it can be remade.
    if os.path.exists(str(seed.Seed_ID)):
        web.reset_aip(seed.Seed_ID, seed_df)

    # Makes a folder for the seed in the AIP directory,
    # and downloads the metadata and WARC files to that seed folder.
    os.mkdir(str(seed.Seed_ID))
    web.download_metadata(seed, seed_df)
    web.download_warcs(seed, date_end, seed_df)

# Saves the information in seed_df to a CSV as a record for the process.
os.chdir(c.script_output)
seed_df.to_csv("seeds_log.csv", index=False)

# Verifies the all expected seed folders are present and have all the expected metadata files and WARCs.
# Saves the result as a csv in the folder with the downloaded AIPs.
# TODO: Make this work with change to Seed ID instead of AIP ID, once decide when to calculate AIP ID.
# web.check_aips(date_end, date_start, seed_df, aips_directory)
