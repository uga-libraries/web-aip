"""Purpose: This script generates every known variation to use for testing the check_aips function in web_functions.py.
This is a separate test from the rest of the workflow for making AIPs to make it easier to create the variations
and develop this function, which is not dependent on the data from the previous part of the script.

Usage: python path/test_check_aips.py
"""

import datetime
import os
import pandas as pd
import configuration as c
import web_functions as web

def make_file(path):
    """Make a test file in the location and name indicated by the path.
    This is used a lot of times in the script and makes it easier to read what files are being created."""
    with open(path, "w") as new_file:
        new_file.write("For Testing")


# The start and end dates that the test script requires to give predictable results.
# In the actual script, these are arguments.
date_start = "2022-03-20"
date_end = "2022-03-25"

# Makes a folder for AIPs within the script_output folder and makes it the current directory.
aips_directory = os.path.join(c.script_output, f"aips_{date_end}")
os.makedirs(aips_directory)
os.chdir(aips_directory)

# Dataframe with seed and AIP ids to use for making the test AIPs.
# In the full script, there is a lot more in the seed dataframe, but this function only needs the ids.
seed_df = pd.DataFrame({"Seed_ID": ["2529671", "2529669", "2529633", "2529665", "2529634", "2529660", "2529642",
                                    "2529627", "2529652", "2529631", "2529668", "2529681", "2529676", "2529629"],
                        "AIP_ID": ["magil-ggp-2529671-2022-03", "magil-ggp-2529669-2022-03", "magil-ggp-2529633-2022-03",
                                   "magil-ggp-2529665-2022-03", "magil-ggp-2529634-2022-03", "magil-ggp-2529660-2022-03",
                                   "magil-ggp-2529642-2022-03","magil-ggp-2529627-2022-03", "magil-ggp-2529652-2022-03",
                                   "magil-ggp-2529631-2022-03", "magil-ggp-2529668-2022-03", "magil-ggp-2529681-2022-03",
                                   "magil-ggp-2529676-2022-03", "magil-ggp-2529629-2022-03"]})

# Makes a minimum correct AIP for each of the seeds as a start for the test.
# The folder is named _bag but is not a real bag and the files are plain text rather than actually downloaded.
# It saves time and is enough to pass the tests, which are based on folder and file names.
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

# Creates additional files for examples of any files that are allowed to repeat.
# These AIPs, as well as the first AIP which is left as minimum correct, will pass the completeness check.
make_file("magil-ggp-2529629-2022-03_bag/data/metadata/magil-ggp-2529629-2022-03_456123_crawldef.csv")
make_file("magil-ggp-2529631-2022-03_bag/data/metadata/magil-ggp-2529631-2022-03_123789_crawljob.csv")
make_file("magil-ggp-2529631-2022-03_bag/data/metadata/magil-ggp-2529631-2022-03_378912_crawljob.csv")
make_file("magil-ggp-2529633-2022-03_bag/data/metadata/magil-ggp-2529633-2022-03_files-deleted_2022-04-05_del.csv")
make_file("magil-ggp-2529634-2022-03_bag/data/metadata/test2.warc_fits.xml")
make_file("magil-ggp-2529634-2022-03_bag/data/objects/test2.warc")

# # Make error of missing AIP folder by skipping this AIP.
# if seed == "2529627":
#     continue
#
# # Make error by not including the Archive-It metadata reports for this AIP.
# if seed == "2529629":
#     with open(f"{aip_folder}/data/metadata/{aip_id}_fits.xml", "w") as new_file:
#         new_file.write("Test")
#     with open(f"{aip_folder}/data/metadata/{aip_id}_preservation.xml", "w") as new_file:
#         new_file.write("Test")
#
# # Make error by not including the fits or preservation.xml files for this AIP.
# elif seed == "2529642":
#     for ext in ("seed.csv", "seedscope.csv", "collscope.csv", "coll.csv", "crawljob.csv", "crawldef.csv"):
#         with open(f"{aip_folder}/data/metadata/{aip_id}_{ext}", "w") as new_file:
#             new_file.write("Test")
#
# # Test fits count by adding extra FITS for one AIP with 2 WARCs (correct) and one with 1 WARC (error).
# if seed in ("2529634", "2529668"):
#     with open(f"{aip_folder}/data/metadata/{aip_id}_2_fits.xml", "w") as new_file:
#         new_file.write("Test")
#
# # Make error by making a metadata file that isn't an expected type for this AIP.
# if seed == "2529669":
#     with open(f"{aip_folder}/data/metadata/{aip_id}_error.txt", "w") as new_file:
#         new_file.write("Test")
#
# # Make error by not making a WARC for this AIP.
# if seed == "2529660":
#     continue
#
#
# # Test WARC count by adding extra WARC for one AIP with 2 WARCs (correct) and one with 1 WARC (error).
# if seed in ("2529634", "2529652"):
#     with open(f"{aip_folder}/data/objects/test2.warc.gz", "w") as new_file:
#         new_file.write("Test")
#
# # Make error by adding something other than a WARC to the objects folder for this AIP.
# if seed == "2529665":
#     with open(f"{aip_folder}/data/objects/error.txt", "w") as new_file:
#         new_file.write("Test")
#
# # Makes an error by adding an AIP to the AIPs directory that is not expected.
# os.makedirs("magil-error-000000-2022-03_bag")
#
# # Verifies the AIPs are complete and no extra AIPs were created. Does not look at the errors folder, so any AIPs with
# # errors will show as missing. Saves the result as a csv in the folder with the downloaded AIPs.
# # todo: seed_to_aip needs to be seed_df
# web.check_aips(date_end, date_start, seed_to_aip, aips_directory)
