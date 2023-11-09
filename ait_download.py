"""
Purpose: Downloads archived web content (WARCs) and associated metadata for a group of seeds from Archive-It.org using
their APIs and prepares them to be converted into AIPs with the general-aip.py script for long-term preservation.
At UGA, this script is run every three months to download content for all crawls saved that quarter.

The download combines all WARCs saved within a quarter for a seed, even if that seed was crawled multiple times.
It also includes six of the metadata reports:
    * Collection
    * Collection Scope (not downloaded if no scope rules for the collection)
    * Crawl Definition (may be more than one)
    * Crawl Job (may be more than one)
    * Seed
    * Seed Scope (not downloaded if not scope rules for the seed)

Prior to the preservation download, all seed metadata should be entered into Archive-It.
Use the seed_metadata_report.py script to verify all required fields are present.
"""

# Usage: python ait_download.py date_start date_end

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
import web_functions as fun

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
fun.check_config()

# Path to the folder in the script output directory (defined in the configuration file)
# where everything related to this download will be saved.
seeds_directory = os.path.join(c.script_output, "preservation_download")

# The script may be run repeatedly if there are interruptions, such as due to API connections.
# If it has run, it will use the existing seeds_log.csv for seed_df and and skip seeds that were already done.
# Otherwise, it makes seed_df and metadata_csv by getting data from the Archive-It APIs
# and add the AIP_ID from metadata_csv to be the first column of seed_df.
if os.path.exists(seeds_directory):
    os.chdir(seeds_directory)
    seed_df = pd.read_csv(os.path.join(c.script_output, "seeds_log.csv"), dtype="object")
else:
    os.makedirs(seeds_directory)
    os.chdir(seeds_directory)
    seed_df = fun.seed_data(date_start, date_end)
    aip_id_df = fun.metadata_csv(seed_df['Seed_ID'].values.tolist(), date_end)
    seed_df = pd.merge(seed_df, aip_id_df, how="left")
    seed_df.insert(0, "AIP_ID", seed_df.pop('AIP_ID'))

# Starts counter for tracking script progress.
# Some processes are slow, so this shows the script is still working and how much remains.
current_seed = 0
total_seeds = len(seed_df[seed_df["Complete"].isnull()])

# Iterates through information about each seed, downloading metadata and WARC files from Archive-It.
# Filtered for no data in the WARC_Unzip_Errors (last log column) to skip seeds done earlier if this is a restart.
for seed in seed_df[seed_df["Complete"].isnull()].itertuples():

    # Updates the current seed number and displays the script progress.
    current_seed += 1
    print(f"\nStarting seed {current_seed} of {total_seeds}.")

    # Row index for the seed being processed in the dataframe, to use for adding logging information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # If the seed already has a folder from an error in a previous iteration of the script,
    # deletes the contents and anything in the seeds_log.csv from the previous iteration so it can be remade.
    if os.path.exists(str(seed.Seed_ID)):
        fun.reset_seed(seed.Seed_ID, seed_df)

    # Makes a folder for the seed in the AIP directory,
    # and downloads the metadata and WARC files to that seed folder.
    os.mkdir(str(seed.Seed_ID))
    fun.download_metadata(seed, row_index, seed_df)
    fun.download_warcs(seed, row_index, seed_df)

    # Updates the Complete column with a summary of error types or that the seed processed successfully.
    fun.add_completeness(row_index, seed_df)

# Verifies the all expected seed folders are present and have all the expected metadata files and WARCs.
# Saves the result as a csv in the folder with the downloaded AIPs.
fun.check_seeds(date_end, date_start, seed_df, seeds_directory)
