"""Purpose: This script generates every known variation to use for testing the check_aips function in web_functions.py.
This is a separate test from the rest of the workflow for making AIPs to make it easier to create the variations
and develop this function, which is not dependent on the data from the previous part of the script (other than
getting the seed and AIP ids from seed_df, which is made manually in this script).
"""

# Usage: python path/test_check_aips_function.py

import os
import numpy as np
import pandas as pd
import shutil
import configuration as c
import web_functions as web


def make_file(path):
    """Make a test file in the location and name indicated by the path.
    This is a function, even though it is so small, because it is used a lot of times in the script
    and having file creation be one line makes it easier to see in the script body what files are being created."""
    with open(path, "w") as new_file:
        new_file.write("For Testing")


def expected_completeness_check_df():
    """Makes a dataframe with the expected values for the completeness_check.csv that this script will produce
    if everything works correctly."""

    aip_list = ["magil-ggp-2472041-2022-03", "magil-ggp-2529671-2022-03",
                "magil-ggp-2529669-2022-03", "magil-ggp-2529633-2022-03",
                "magil-ggp-2529665-2022-03", "magil-ggp-2529634-2022-03",
                "magil-ggp-2529660-2022-03", "magil-ggp-2529629-2022-03",
                "magil-ggp-2529642-2022-03", "magil-ggp-2529627-2022-03",
                "magil-ggp-2529652-2022-03", "magil-ggp-2529631-2022-03",
                "magil-ggp-2529668-2022-03", "magil-ggp-2529681-2022-03",
                "magil-ggp-2529676-2022-03", "magil-ggp-extra1-2022-03_bag", "magil-ggp-extra2-2022-03_bag"]

    url_list = ["https://www.legis.ga.gov/", "https://grec.state.ga.us/",
                "https://oig.georgia.gov/", "https://medicalboard.georgia.gov/",
                "https://ltgov.georgia.gov/", "https://consumer.georgia.gov/",
                "https://dso.georgia.gov/", "https://gbp.georgia.gov/",
                "https://dcs.georgia.gov/", "https://gaa.georgia.gov/",
                "https://gdna.georgia.gov/", "https://gceo.georgia.gov/",
                "https://oca.georgia.gov/", "https://gspc.georgia.gov/",
                "https://gsba.georgia.gov/", np.NaN, np.NaN]

    folder_list = ["False", "True", "True", "True", "True", "True", "True", "True", "True", "True",
                   "True", "True", "True", "True", "True", "Not expected", "Not expected"]

    coll_list = [np.NaN, True, True, True, True, True, True, True, False, True,
                 True, True, True, True, True, np.NaN, np.NaN]

    collscope_list = [np.NaN, True, True, True, True, True, True, True, True, True,
                      False, True, True, True, True, np.NaN, np.NaN]

    seed_list = [np.NaN, True, True, True, True, True, False, True, True, True,
                 True, True, True, True, True, np.NaN, np.NaN]

    seedscope_list = [np.NaN, True, True, True, True, True, False, True, True, True,
                      True, True, True, True, True, np.NaN, np.NaN]

    crawldef_list = [np.NaN, 1, 1, 1, 0, 1, 1, 2, 1, 1,
                     1, 1, 1, 1, 1, np.NaN, np.NaN]

    crawljob_list = [np.NaN, 1, 1, 1, 0, 1, 1, 1, 1, 1,
                     1, 3, 1, 1, 1, np.NaN, np.NaN]

    presxml_list = [np.NaN, True, True, True, True, True, True, True, True, True,
                    True, True, False, True, True, np.NaN, np.NaN]

    warc_list = [np.NaN, False, True, True, True, True, True, True, True, True,
                 True, True, True, True, True, np.NaN, np.NaN]

    objects_list = [np.NaN, True, True, True, True, True, True, True, True, True,
                    True, True, True, False, True, np.NaN, np.NaN]

    fits_list = [np.NaN, False, False, True, True, True, True, True, True, True,
                 True, True, False, True, True, np.NaN, np.NaN]

    metadata_list = [np.NaN, True, True, True, True, True, True, True, True, True,
                     True, True, True, True, False, np.NaN, np.NaN]

    data = {"AIP": aip_list, "URL": url_list, "AIP Folder Made": folder_list, "coll.csv": coll_list,
            "collscope.csv": collscope_list, "seed.csv": seed_list, "seedscope.csv": seedscope_list,
            "crawldef.csv count": crawldef_list, "crawljob.csv count": crawljob_list, "preservation.xml": presxml_list,
            "WARC Count Correct": warc_list, "Objects is all WARCs": objects_list, "fits.xml Count Correct": fits_list,
            "No Extra Metadata": metadata_list}

    df = pd.DataFrame(data)
    return df


# The start and end dates that the test script requires to give predictable results.
# In the full script, these are arguments.
date_start = "2022-03-20"
date_end = "2022-03-25"

# Makes a folder for AIPs within the script_output folder and makes it the current directory.
aips_directory = os.path.join(c.script_output, f"aips_{date_end}")
os.makedirs(aips_directory)
os.chdir(aips_directory)

# Makes a dataframe with seed and AIP ids to use for making the test AIPs.
# In the full script, this is made using the seed_data() function and there is a lot more in the seed dataframe,
# but the check_aip() function only needs the two ids columns.
seed_df = pd.DataFrame({"Seed_ID": ["2472041", "2529627", "2529629", "2529631", "2529633",
                                    "2529634", "2529642", "2529652", "2529660", "2529665",
                                    "2529668", "2529669", "2529671", "2529676", "2529681"],
                        "AIP_ID": ["magil-ggp-2472041-2022-03", "magil-ggp-2529627-2022-03",
                                   "magil-ggp-2529629-2022-03", "magil-ggp-2529631-2022-03",
                                   "magil-ggp-2529633-2022-03", "magil-ggp-2529634-2022-03",
                                   "magil-ggp-2529642-2022-03", "magil-ggp-2529652-2022-03",
                                   "magil-ggp-2529660-2022-03", "magil-ggp-2529665-2022-03",
                                   "magil-ggp-2529668-2022-03", "magil-ggp-2529669-2022-03",
                                   "magil-ggp-2529671-2022-03", "magil-ggp-2529676-2022-03",
                                   "magil-ggp-2529681-2022-03"]})

# Makes a minimum correct AIP for each of the seeds.
# The folder is named _bag but is not a real bag and the files are plain text rather than actually downloaded.
# This saves time and is enough to pass the tests, which are based on folder and file names.
for aip in seed_df["AIP_ID"]:
    metadata_folder = os.path.join(f"{aip}_bag", "data", "metadata")
    os.makedirs(metadata_folder)
    make_file(f"{metadata_folder}/{aip}_123456_crawldef.csv")
    make_file(f"{metadata_folder}/{aip}_789123_crawljob.csv")
    make_file(f"{metadata_folder}/{aip}_coll.csv")
    make_file(f"{metadata_folder}/{aip}_collscope.csv")
    make_file(f"{metadata_folder}/{aip}_preservation.xml")
    make_file(f"{metadata_folder}/{aip}_seed.csv")
    make_file(f"{metadata_folder}/{aip}_seedscope.csv")
    make_file(f"{metadata_folder}/test.warc_fits.xml")
    objects_folder = os.path.join(f"{aip}_bag", "data", "objects")
    os.makedirs(objects_folder)
    make_file(f"{objects_folder}/test.warc")

# Creates additional files for examples of files that are allowed to repeat.
# These AIPs, as well as the first AIP which is left as minimum correct, will pass the completeness check.
make_file("magil-ggp-2529629-2022-03_bag/data/metadata/magil-ggp-2529629-2022-03_456123_crawldef.csv")
make_file("magil-ggp-2529631-2022-03_bag/data/metadata/magil-ggp-2529631-2022-03_123789_crawljob.csv")
make_file("magil-ggp-2529631-2022-03_bag/data/metadata/magil-ggp-2529631-2022-03_378912_crawljob.csv")
make_file("magil-ggp-2529633-2022-03_bag/data/metadata/magil-ggp-2529633-2022-03_files-deleted_2022-04-05_del.csv")
make_file("magil-ggp-2529634-2022-03_bag/data/metadata/test2.warc_fits.xml")
make_file("magil-ggp-2529634-2022-03_bag/data/objects/test2.warc")

# Creates incorrect AIPs by adding or deleting files or folders.
shutil.rmtree("magil-ggp-2472041-2022-03_bag")
os.remove("magil-ggp-2529642-2022-03_bag/data/metadata/magil-ggp-2529642-2022-03_coll.csv")
os.remove("magil-ggp-2529652-2022-03_bag/data/metadata/magil-ggp-2529652-2022-03_collscope.csv")
os.remove("magil-ggp-2529660-2022-03_bag/data/metadata/magil-ggp-2529660-2022-03_seed.csv")
os.remove("magil-ggp-2529660-2022-03_bag/data/metadata/magil-ggp-2529660-2022-03_seedscope.csv")
os.remove("magil-ggp-2529665-2022-03_bag/data/metadata/magil-ggp-2529665-2022-03_123456_crawldef.csv")
os.remove("magil-ggp-2529665-2022-03_bag/data/metadata/magil-ggp-2529665-2022-03_789123_crawljob.csv")
os.remove("magil-ggp-2529668-2022-03_bag/data/metadata/magil-ggp-2529668-2022-03_preservation.xml")
os.remove("magil-ggp-2529668-2022-03_bag/data/metadata/test.warc_fits.xml")
make_file("magil-ggp-2529669-2022-03_bag/data/metadata/extra.warc_fits.xml")
make_file("magil-ggp-2529671-2022-03_bag/data/objects/extra.warc")
make_file("magil-ggp-2529676-2022-03_bag/data/metadata/extra.txt")
make_file("magil-ggp-2529681-2022-03_bag/data/objects/extra.txt")
os.makedirs("magil-ggp-extra1-2022-03_bag")
os.makedirs("magil-ggp-extra2-2022-03_bag")

# Runs the function being tested by this script.
# It verifies the AIPs are complete and no extra AIPs were created, and saves the results to a csv.
web.check_aips(date_end, date_start, seed_df, aips_directory)

# Compares the results from the script (read from the script CSV output) to the expected results.
os.chdir(c.script_output)
expected_df = expected_completeness_check_df()
actual_df = pd.read_csv("completeness_check.csv")
compare_df = actual_df.merge(expected_df, indicator=True, how="outer")

# If there are any that don't match (_merge isn't both), saves to a CSV.
# Otherwise, prints a message to the terminal that the test passed.
compare_df = compare_df[compare_df["_merge"] != "both"]
if len(compare_df) is 0:
    print("\nAll tests pass. The check_aips function produced expected results.")
else:
    print("\nErrors: see 'completeness_check_differences.csv' for details.")
    print("Rows with 'left_only' in _merge are the actual values and 'right_only' are the expected values.")
    compare_df.to_csv("completeness_check_differences.csv", index=False)