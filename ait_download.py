"""Download WARCs and associated metadata from Archive-It for long-term preservation.

At UGA, this script is run every three months to download content for all crawls saved that quarter.
The download combines all WARCs saved within a quarter for a seed, even if that seed was crawled multiple times.

It also includes six of the metadata reports:
    * Collection
    * Collection Scope (not downloaded if no scope rules for the collection)
    * Crawl Definition (may be more than one)
    * Crawl Job (may be more than one)
    * Seed
    * Seed Scope (not downloaded if no scope rules for the seed)

Parameters:
    There are two date parameters, formatted YYYY-MM-DD, which define which WARCs to include in the download.
    date_start : required. WARCs stored on this day will be included.
    date_end : required. WARCs stored on this day will NOT be included.

Returns:
    One folder for each seed, with the WARCs and metadata reports.
    A metadata.csv file needed for the general-aip script to prepare the folders for preservation.
    A seeds_log.csv file with information about each workflow step.
    A completeness_log.csv file with information about the download's completeness.
"""

# Usage: python ait_download.py date_start date_end

import os
import pandas as pd
import re
import sys

# Configuration is made by the user and could be forgotten.
try:
    import configuration as c
except ModuleNotFoundError:
    print("\nScript cannot run without a configuration file in the local copy of the GitHub repo.")
    print("Make a file named configuration.py using configuration_template.py and run the script again.")
    sys.exit()
import web_functions as fun

# Tests to validate the two date arguments, which specify the time frame for WARCs to include in the download.

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
# If it has run, it will use the existing seeds_log.csv for seed_df and skip seeds that were already done.
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

# Starts a counter for tracking script progress.
# Some processes are slow, so this shows the script is still working and how much work remains.
current_seed = 0
total_seeds = len(seed_df[seed_df["Complete"] == "TBD"])

# Iterates through information about each seed, downloading metadata and WARC files from Archive-It.
# Filtered for "TBD" in the Complete column to skip seeds done earlier if this is a restart.
for seed in seed_df[seed_df["Complete"] == "TBD"].itertuples():

    # Updates the current seed number and displays the script progress.
    current_seed += 1
    print(f"\nStarting seed {current_seed} of {total_seeds}.")

    # Calculates the row index for the seed being processed in the dataframe, to use for adding log information.
    row_index = seed_df.index[seed_df["Seed_ID"] == seed.Seed_ID].tolist()[0]

    # If the seed already has a folder from an error in a previous iteration of the script,
    # deletes the contents and anything in the seeds_log.csv from the previous iteration, so it can be remade.
    if os.path.exists(str(seed.Seed_ID)):
        fun.reset_seed(seed.Seed_ID, seed_df)

    # Makes a folder for the seed in the seeds directory,
    # and downloads the metadata and WARC files to that seed folder.
    os.mkdir(str(seed.Seed_ID))
    fun.download_metadata(seed, row_index, seed_df)
    fun.download_warcs(seed, row_index, seed_df)

    # Updates the Complete column with the error type or that the seed processed successfully.
    fun.add_completeness(row_index, seed_df)

# Verifies the all expected seed folders are present and contain all the expected metadata files and WARCs.
# Saves the result as a csv in the folder with the downloaded content.
fun.check_seeds(date_end, date_start, seed_df, seeds_directory)
