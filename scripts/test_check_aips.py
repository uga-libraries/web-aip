"""Purpose: This script generates every known variation to use for testing the check_aips function in web_functions.py.
This is a separate test from the rest of the workflow for making AIPs to make it easier to create the variations
and develop this function, which is not dependent on the data from the previous part of the script.

Usage: python path/test_check_aips.py
"""

import datetime
import os
import configuration as c
import web_functions as web

# The start and end dates that the test script requires to give predictable results.
# In the actual script, these are arguments.
date_start = "2022-03-20"
date_end = "2022-03-25"

# Makes a folder for AIPs within the script_output folder and makes it the current directory.
aips_directory = os.path.join(c.script_output, f"aips_{date_end}")
os.makedirs(aips_directory)
os.chdir(aips_directory)

# Seed and AIP ids to use for making the test AIPs.
# TODO: this needs to be seed_df now
seed_to_aip = {"2529671": "magil-ggp-2529671-2022-03", "2529669": "magil-ggp-2529669-2022-03",
               "2529633": "magil-ggp-2529633-2022-03", "2529665": "magil-ggp-2529665-2022-03",
               "2529634": "magil-ggp-2529634-2022-03", "2529660": "magil-ggp-2529660-2022-03",
               "2529642": "magil-ggp-2529642-2022-03", "2529627": "magil-ggp-2529627-2022-03",
               "2529652": "magil-ggp-2529652-2022-03", "2529631": "magil-ggp-2529631-2022-03",
               "2529668": "magil-ggp-2529668-2022-03", "2529681": "magil-ggp-2529681-2022-03",
               "2529676": "magil-ggp-2529676-2022-03", "2529629": "magil-ggp-2529629-2022-03"}

# Make one AIP folder for each of the seeds with fake metadata files, WARC, and bagging to be faster.
# Just has to have everything that the completeness check looks for.
for seed in seed_to_aip:

    aip_id = seed_to_aip[seed]
    aip_folder = aip_id + "_bag"

    # Make error of missing AIP folder by skipping this AIP.
    if seed == "2529627":
        continue

    # Metadata folder and its contents.
    os.makedirs(f"{aip_folder}/data/metadata")
    metadata_ext = ["seed.csv", "seedscope.csv", "collscope.csv", "coll.csv", "crawljob.csv", "crawldef.csv",
                    "fits.xml", "preservation.xml"]

    # Make error by not including the Archive-It metadata reports for this AIP.
    if seed == "2529629":
        with open(f"{aip_folder}/data/metadata/{aip_id}_fits.xml", "w") as new_file:
            new_file.write("Test")
        with open(f"{aip_folder}/data/metadata/{aip_id}_preservation.xml", "w") as new_file:
            new_file.write("Test")

    # Make error by not including the fits or preservation.xml files for this AIP.
    elif seed == "2529642":
        for ext in ("seed.csv", "seedscope.csv", "collscope.csv", "coll.csv", "crawljob.csv", "crawldef.csv"):
            with open(f"{aip_folder}/data/metadata/{aip_id}_{ext}", "w") as new_file:
                new_file.write("Test")

    # For the rest of the AIPs, add all expected metadata files.
    else:
        for ext in metadata_ext:
            # Add one each of all of the expected metadata files.
            with open(f"{aip_folder}/data/metadata/{aip_id}_{ext}", "w") as new_file:
                new_file.write("Test")

    # Test counting by adding extra crawldef and crawljob to this AIP.
    if seed == "2529631":
        with open(f"{aip_folder}/data/metadata/{aip_id}_2_crawldef.csv", "w") as new_file:
            new_file.write("Test")
        with open(f"{aip_folder}/data/metadata/{aip_id}_2_crawljob.csv", "w") as new_file:
            new_file.write("Test")
        with open(f"{aip_folder}/data/metadata/{aip_id}_3_crawljob.csv", "w") as new_file:
            new_file.write("Test")

    # Test fits count by adding extra FITS for one AIP with 2 WARCs (correct) and one with 1 WARC (error).
    if seed in ("2529634", "2529668"):
        with open(f"{aip_folder}/data/metadata/{aip_id}_2_fits.xml", "w") as new_file:
            new_file.write("Test")

    # Test deletion log (permitted) by creating one in one AIP.
    if seed == "2529681":
        with open(f"{aip_folder}/data/metadata/{aip_id}_files-deleted_{datetime.datetime.today().date()}_del.csv",
                  "w") as new_file:
            new_file.write("Test")

    # Make error by making a metadata file that isn't an expected type for this AIP.
    if seed == "2529669":
        with open(f"{aip_folder}/data/metadata/{aip_id}_error.txt", "w") as new_file:
            new_file.write("Test")

    # Objects folder and its contents. All by one seed have 1 WARC.
    # Making files with a WARC extension but they are not really WARCs.
    os.makedirs(f"{aip_folder}/data/objects")

    # Make error by not making a WARC for this AIP.
    if seed == "2529660":
        continue
    # For all other AIPs, make one test WARC.
    else:
        with open(f"{aip_folder}/data/objects/test.warc.gz", "w") as new_file:
            new_file.write("Test")

    # Test WARC count by adding extra WARC for one AIP with 2 WARCs (correct) and one with 1 WARC (error).
    if seed in ("2529634", "2529652"):
        with open(f"{aip_folder}/data/objects/test2.warc.gz", "w") as new_file:
            new_file.write("Test")

    # Make error by adding something other than a WARC to the objects folder for this AIP.
    if seed == "2529665":
        with open(f"{aip_folder}/data/objects/error.txt", "w") as new_file:
            new_file.write("Test")

# Makes an error by adding an AIP to the AIPs directory that is not expected.
os.makedirs("magil-error-000000-2022-03_bag")

# Verifies the AIPs are complete and no extra AIPs were created. Does not look at the errors folder, so any AIPs with
# errors will show as missing. Saves the result as a csv in the folder with the downloaded AIPs.
# todo: seed_to_aip needs to be seed_df
web.check_aips(date_end, date_start, seed_to_aip, aips_directory)
