"""Purpose: Test that the script is able to restart after errors. A pre-determined date range is used to allow
automatic verification that the test script has produced the expected outputs. The functions from web_functions.py
are used as much as possible for a more authentic test, but test versions of these functions are sometimes needed
to speed up the download process and to generate errors.

Usage: python /path/test_web_aip_batch_iteration
 """

import datetime
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
        web.log(f"Successfully verified {warc} fixity on {datetime.datetime.now()}", seed_df, row_index,
                "WARC_Fixity_Errors")
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
    aip = a.AIP(aips_directory, seed.Department, seed.UGA_Collection, seed.AIP_ID, seed.AIP_ID, seed.Title, version=1,
                to_zip=True)

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

# The rest of the script compares seeds.csv, completeness_check.csv, and the directory structure produced by the test
# to expected values and creates a spreadsheet with the results.

# Makes dataframe with expected values for seeds.csv.
seeds = ["2187482", "2030942", "2084785", "2270486", "2173769", "2184592", "2184360", "2202440", "2173766", "2028986",
         "2122426", "2084781", "2120634", "2084780", "2089428", "2070895", "2018084"]
aips = ["harg-0000-web-202006-0001", "harg-0000-web-202006-0002", "harg-0000-web-202006-0003",
        "harg-0000-web-202006-0004", "harg-0000-web-202006-0005", "harg-0000-web-202006-0006",
        "harg-0000-web-202006-0007", "harg-0000-web-202006-0008", "harg-0000-web-202006-0009", "BLANK",
        "rbrl-246-web-202006-0001", "harg-0000-web-202006-0010", "harg-0000-web-202006-0011",
        "harg-0000-web-202006-0012", "rbrl-459-web-202006-0001", "rbrl-343-web-202006-0001", "rbrl-447-web-202006-0001"]
titles = ["Student Government Association Facebook", "UGA NAACP Twitter", "Zeta Pi Chapter of Alpha Phi Alpha Twitter",
          "UGA Commencement", "Coronavirus (COVID-19) Information and Resources website", "UGA Twitter", "UGA Today",
          "UGA Healthy Dawg - University Health Center", "University of Georgia homepage", "The Now Explosion Website",
          "Johnny Isakson YouTube Channel", "UGA Football Facebook", "UGA Office of Institutional Research website",
          "UGA Athletics Twitter", "Southern Alliance for Clean Energy",
          "Statewide Independent Living Council of Georgia", "K7MOA Legacy Voteview Website"]
depts = ["hargrett", "hargrett", "hargrett", "hargrett", "hargrett", "hargrett", "hargrett", "hargrett", "hargrett",
         "BLANK", "russell", "hargrett", "hargrett", "hargrett", "russell", "russell", "russell"]
uga_colls = ["0000", "0000", "0000", "0000", "0000", "0000", "0000", "0000", "0000", "BLANK", "rbrl-246", "0000",
             "0000", "0000", "rbrl-459", "rbrl-343", "rbrl-447"]
ait_colls = ["12181", "12181", "12181", "12912", "12912", "12912", "12912", "12181", "12912", "12470", "12265", "12907",
             "12912", "12907", "12263", "12264", "12262"]
jobs = ["1177700", "1177700", "1177700", "1176433", "1180178", "1122966", "1122766", "1137665", "1115523", "1085452",
        "1046326", "1043800", "1043812", "1043802", "1010662", "1033772", "1033408"]
gbs = ["3.62", "0.05", "0.11", "0.06", "0.0", "0.28", "70.80000000000001", "0.43", "0.08", "1.3", "0.18", "2.29",
       "0.59", "0.03", "16.669999999999998", "3.64", "0.67"]
warcs = ["4", "1", "1", "1", "10", "1", "62", "1", "1", "2", "1", "3", "1", "1", "19", "3", "1"]
warc_names = [
    "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
    "ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
    "ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
    "ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
    "ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz",
    "ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc.gz",
    "ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc.gz",
    "ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc.gz",
    "ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc.gz",
    "ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129224159397-00001-h3.warc.gz,ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129213514425-00000-h3.warc.gz",
    "ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
    "ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc.gz,ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc.gz,ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc.gz",
    "ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc.gz",
    "ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc.gz",
    "ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191023012315001-00008-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191023232420715-00009-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191024225325499-00010-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191025205356890-00011-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191026180403577-00012-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191027150309943-00013-h3-SALVAGED.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191027222744036-00014-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191027223442169-00015-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191029035842313-00016-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191030020954917-00017-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191030232552542-00018-h3.warc.gz",
    "ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc.gz,ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc.gz,ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc.gz",
    "ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc.gz"]
seed_errs = ["Successfully calculated seed metadata", "Successfully calculated seed metadata",
             "Successfully calculated seed metadata", "Successfully calculated seed metadata",
             "Successfully calculated seed metadata", "Successfully calculated seed metadata",
             "Successfully calculated seed metadata", "Successfully calculated seed metadata",
             "Successfully calculated seed metadata",
             "Couldn't get all required metadata values from the seed report. Will not download files or make AIP.",
             "Successfully calculated seed metadata", "Successfully calculated seed metadata",
             "Successfully calculated seed metadata", "Successfully calculated seed metadata",
             "Successfully calculated seed metadata", "Successfully calculated seed metadata",
             "Successfully calculated seed metadata"]
metadata_errs = ["Successfully downloaded all metadata reports", "Successfully downloaded all metadata reports",
                 "Successfully downloaded all metadata reports", "Successfully downloaded all metadata reports",
                 "Successfully downloaded all metadata reports", "Successfully downloaded all metadata reports",
                 "Successfully downloaded all metadata reports", "Successfully downloaded all metadata reports",
                 "Successfully downloaded all metadata reports", "BLANK",
                 "Successfully downloaded all metadata reports", "Successfully downloaded all metadata reports",
                 "Successfully downloaded all metadata reports", "Successfully downloaded all metadata reports",
                 "Successfully downloaded all metadata reports", "Successfully downloaded all metadata reports",
                 "Successfully downloaded all metadata reports"]
info = ["No additional information", "No additional information", "No additional information",
        "Empty report harg-0000-web-202006-0004_seedscope.csv not saved",
        "Empty report harg-0000-web-202006-0005_seedscope.csv not saved", "No additional information",
        "No additional information", "Empty report harg-0000-web-202006-0008_seedscope.csv not saved",
        "Empty report harg-0000-web-202006-0009_seedscope.csv not saved", "BLANK", "No additional information",
        "Empty report harg-0000-web-202006-0010_collscope.csv not saved",
        "Empty report harg-0000-web-202006-0011_seedscope.csv not saved",
        "Empty report harg-0000-web-202006-0012_collscope.csv not saved", "No additional information",
        "No additional information", "Empty report rbrl-447-web-202006-0001_seedscope.csv not saved"]
api = [
    "Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc.gz", "BLANK",
    "Successfully downloaded ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc.gz",
    "Successfully downloaded ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc.gz; Successfully downloaded ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc.gz; Successfully downloaded ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc.gz",
    "Successfully downloaded ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc.gz"]
fixity = [
    "Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc.gz fixity on",
    "BLANK",
    "Successfully verified ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc.gz fixity on; Successfully verified ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc.gz fixity on; Successfully verified ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc.gz fixity on",
    "Successfully verified ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc.gz fixity on"]
unzip = [
    "Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc.gz", "BLANK",
    "Successfully unzipped ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc.gz",
    "Successfully unzipped ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc.gz; Successfully unzipped ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc.gz; Successfully unzipped ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc.gz",
    "Successfully unzipped ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc.gz"]
deleted = ["No files deleted", "No files deleted", "No files deleted", "No files deleted", "No files deleted",
           "No files deleted", "No files deleted", "BLANK", "No files deleted", "BLANK", "No files deleted",
           "No files deleted", "No files deleted", "No files deleted", "BLANK", "No files deleted", "No files deleted"]
objects = ["Successfully created objects folder", "Successfully created objects folder",
           "Successfully created objects folder", "Successfully created objects folder",
           "Successfully created objects folder", "Successfully created objects folder",
           "Successfully created objects folder", "BLANK", "Successfully created objects folder", "BLANK",
           "Successfully created objects folder", "Successfully created objects folder",
           "Successfully created objects folder", "Successfully created objects folder", "BLANK",
           "Successfully created objects folder", "Successfully created objects folder"]
metadata = ["Successfully created metadata folder", "Successfully created metadata folder",
            "Successfully created metadata folder", "Successfully created metadata folder",
            "Successfully created metadata folder", "Successfully created metadata folder",
            "Successfully created metadata folder", "BLANK", "Successfully created metadata folder", "BLANK",
            "Successfully created metadata folder", "Successfully created metadata folder",
            "Successfully created metadata folder", "Successfully created metadata folder", "BLANK",
            "Successfully created metadata folder", "Successfully created metadata folder"]
fits_tools = ["No FITS tools errors", "No FITS tools errors", "No FITS tools errors", "No FITS tools errors",
              "No FITS tools errors", "No FITS tools errors", "No FITS tools errors", "BLANK", "No FITS tools errors",
              "BLANK", "No FITS tools errors", "No FITS tools errors", "No FITS tools errors", "No FITS tools errors",
              "BLANK", "No FITS tools errors", "No FITS tools errors"]
fits_combos = ["Successfully created combined-fits.xml", "Successfully created combined-fits.xml",
               "Successfully created combined-fits.xml", "Successfully created combined-fits.xml",
               "Successfully created combined-fits.xml", "Successfully created combined-fits.xml",
               "Successfully created combined-fits.xml", "BLANK", "Successfully created combined-fits.xml", "BLANK",
               "Successfully created combined-fits.xml", "Successfully created combined-fits.xml",
               "Successfully created combined-fits.xml", "Successfully created combined-fits.xml", "BLANK",
               "Successfully created combined-fits.xml", "Successfully created combined-fits.xml"]
pres_made = ["Successfully created preservation.xml", "Successfully created preservation.xml",
             "Successfully created preservation.xml", "Successfully created preservation.xml",
             "Successfully created preservation.xml", "Successfully created preservation.xml",
             "Successfully created preservation.xml", "BLANK", "Successfully created preservation.xml", "BLANK",
             "Successfully created preservation.xml", "Successfully created preservation.xml",
             "Successfully created preservation.xml", "Successfully created preservation.xml", "BLANK",
             "Successfully created preservation.xml", "Successfully created preservation.xml"]
valid = ["Preservation.xml valid on", "Preservation.xml valid on", "Preservation.xml valid on",
         "Preservation.xml valid on", "Preservation.xml valid on", "Preservation.xml valid on",
         "Preservation.xml valid on", "BLANK", "Preservation.xml valid on", "BLANK", "Preservation.xml valid on",
         "Preservation.xml valid on", "Preservation.xml valid on", "Preservation.xml valid on", "BLANK",
         "Preservation.xml valid on", "Preservation.xml valid on"]
bag_valid = ["Bag valid on", "Bag valid on", "Bag valid on", "Bag valid on", "Bag valid on", "Bag valid on",
             "Bag valid on", "BLANK", "Bag valid on", "BLANK", "Bag valid on", "Bag valid on", "Bag valid on",
             "Bag valid on", "BLANK", "Bag valid on", "Bag valid on"]
package = ["Successfully made package", "Successfully made package", "Successfully made package",
           "Successfully made package", "Successfully made package", "Successfully made package",
           "Successfully made package", "BLANK", "Successfully made package", "BLANK", "Successfully made package",
           "Successfully made package", "Successfully made package", "Successfully made package", "BLANK",
           "Successfully made package", "Successfully made package"]
manifest = ["Successfully added AIP to manifest.", "Successfully added AIP to manifest.",
            "Successfully added AIP to manifest.", "Successfully added AIP to manifest.",
            "Successfully added AIP to manifest.", "Successfully added AIP to manifest.",
            "Successfully added AIP to manifest.", "BLANK", "Successfully added AIP to manifest.", "BLANK",
            "Successfully added AIP to manifest.", "Successfully added AIP to manifest.",
            "Successfully added AIP to manifest.", "Successfully added AIP to manifest.", "BLANK",
            "Successfully added AIP to manifest.", "Successfully added AIP to manifest."]
complete = ["Successfully completed processing", "Successfully completed processing",
            "Successfully completed processing", "Successfully completed processing",
            "Successfully completed processing", "Successfully completed processing",
            "Successfully completed processing", "BLANK", "Successfully completed processing", "BLANK",
            "Successfully completed processing", "Successfully completed processing",
            "Successfully completed processing", "Successfully completed processing", "BLANK",
            "Successfully completed processing", "Successfully completed processing"]
data = {"Seed_ID": seeds, "AIP_ID": aips, "Title": titles, "Department": depts, "UGA_Collection": uga_colls,
        "AIT_Collection": ait_colls, "Job_ID": jobs, "Size_GB": gbs, "WARCs": warcs, "WARC_Filenames": warc_names,
        "Seed_Metadata_Errors": seed_errs, "Metadata_Report_Errors": metadata_errs, "Metadata_Report_Info": info,
        "WARC_API_Errors": api, "WARC_Fixity_Errors": fixity, "WARC_Unzip_Errors": unzip, "Files Deleted": deleted,
        "Objects Folder": objects, "Metadata Folder": metadata, "FITS Tool Errors": fits_tools,
        "FITS Combination Errors": fits_combos, "Preservation.xml Made": pres_made, "Preservation.xml Valid": valid,
        "Bag Valid": bag_valid, "Package Errors": package, "Manifest Errors": manifest, "Processing Complete": complete}
expected_seeds_df = pd.DataFrame(data)

# Fills NaN with text and removes timestamps from validation columns to allow the comparison.
seed_df.fillna("BLANK", inplace=True)
seed_df["WARC_Fixity_Errors"] = seed_df["WARC_Fixity_Errors"].str.replace(" \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}", "")
seed_df["Preservation.xml Valid"] = seed_df["Preservation.xml Valid"].str.replace(" \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}", "")
seed_df["Bag Valid"] = seed_df["Bag Valid"].str.replace(" \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}", "")

# Compares the expected seeds.csv to the seeds.csv produced by the script.
compare_seed_df = seed_df.merge(expected_seeds_df, indicator=True, how="outer")

# Makes dataframe with expected values for completeness_check.csv.
expected_check_df = pd.DataFrame({"AIP": ["harg-0000-web-202006-0001", "harg-0000-web-202006-0002", "harg-0000-web-202006-0003", "harg-0000-web-202006-0004", "harg-0000-web-202006-0005", "harg-0000-web-202006-0006", "harg-0000-web-202006-0007", "harg-0000-web-202006-0008", "harg-0000-web-202006-0009", "rbrl-246-web-202006-0001", "harg-0000-web-202006-0010", "harg-0000-web-202006-0011", "harg-0000-web-202006-0012", "rbrl-459-web-202006-0001", "rbrl-343-web-202006-0001", "rbrl-447-web-202006-0001"],
                                  "URL": ["https://www.facebook.com/ugasga/", "https://twitter.com/uganaacp/", "https://twitter.com/UGA_Alphas/", "https://commencement.uga.edu/", "https://www.uga.edu/coronavirus/", "https://twitter.com/universityofga/", "https://news.uga.edu/", "https://ugahealthydawg.com/", "https://www.uga.edu/", "https://www.youtube.com/channel/UC-LF69SBOSgT1S1-yy-VgaA/videos?view=0&sort=dd&shelf_id=0", "https://www.facebook.com/FootballUGA/", "https://oir.uga.edu/", "https://twitter.com/UGAAthletics/", "https://cleanenergy.org/", "https://www.silcga.org/", "http://k7moa.com/site_map.htm"],
                                  "AIP Folder Made": [True, True, True, True, True, True, True, True, True, True, True, True, True, False, True, True],
                                  "coll.csv": [True, True, True, True, True, True, True, True, True, True, True, True, True, "BLANK", True, True],
                                  "collscope.csv": [True, True, True, True, True, True, True, True, True, True, False, True, False, "BLANK", True, True],
                                  "seed.csv": [True, True, True, True, True, True, True, True, True, True, True, True, True, "BLANK", True, True],
                                  "seedscope.csv": [True, True, True, False, False, True, True, False, False, True, True, False, True, "BLANK", True, False],
                                  "crawldef.csv count": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "BLANK", 1, 1],
                                  "crawljob.csv count": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "BLANK", 1, 1],
                                  "preservation.xml": [True, True, True, True, True, True, True, True, True, True, True, True, True, "BLANK", True, True],
                                  "WARC Count Correct": [True, True, True, True, True, True, True, True, True, True, True, True, True, "BLANK", True, True],
                                  "Objects is all WARCs": [True, True, True, True, True, True, True, True, True, True, True, True, True, "BLANK", True, True],
                                  "fits.xml Count Correct": [True, True, True, True, True, True, True, True, True, True, True, True, True, "BLANK", True, True],
                                  "No Extra Metadata": [True, True, True, True, True, True, True, True, True, True, True, True, True, "BLANK", True, True]
                                  })

# Reads completeness_check.csv, made by the script, into a dataframe.
# Fills blanks with text to allow the comparison.
check_df = pd.read_csv("aips_2020-06-09/completeness_check.csv")
check_df.fillna("BLANK", inplace=True)

# Compares the expected completeness check values to the completeness_check.csv produced by the script.
compare_check_df = check_df.merge(expected_check_df, indicator=True, how="outer")

# Checks directory structure.
# Creates a list with the expected related paths.
directory_expected = [r"aips_2020-06-09\completeness_check.csv", r"aips_2020-06-09\seeds.csv",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0001_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0002_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0003_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0004_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0005_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0006_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0007_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0009_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0010_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0011_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\harg-0000-web-202006-0012_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\manifest_hargrett.txt",
                      r"aips_2020-06-09\aips-to-ingest\manifest_russell.txt",
                      r"aips_2020-06-09\aips-to-ingest\rbrl-246-web-202006-0001_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\rbrl-343-web-202006-0001_bag.tar.bz2",
                      r"aips_2020-06-09\aips-to-ingest\rbrl-447-web-202006-0001_bag.tar.bz2",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0001_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0002_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0003_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0004_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0005_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0006_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0007_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0008_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0009_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0010_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0011_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\harg-0000-web-202006-0012_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\rbrl-246-web-202006-0001_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\rbrl-343-web-202006-0001_combined-fits.xml",
                      r"aips_2020-06-09\fits-xml\rbrl-447-web-202006-0001_combined-fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\2187482_31104333391_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\harg-0000-web-202006-0001_1177700_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\harg-0000-web-202006-0001_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\harg-0000-web-202006-0001_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\harg-0000-web-202006-0001_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\harg-0000-web-202006-0001_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\harg-0000-web-202006-0001_seedscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\objects\ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\objects\ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\objects\ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\objects\ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\2030942_31104333391_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\harg-0000-web-202006-0002_1177700_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\harg-0000-web-202006-0002_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\harg-0000-web-202006-0002_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\harg-0000-web-202006-0002_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\harg-0000-web-202006-0002_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\harg-0000-web-202006-0002_seedscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\objects\ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\2084785_31104333391_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\harg-0000-web-202006-0003_1177700_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\harg-0000-web-202006-0003_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\harg-0000-web-202006-0003_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\harg-0000-web-202006-0003_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\harg-0000-web-202006-0003_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\harg-0000-web-202006-0003_seedscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\objects\ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\metadata\2270486_31104332809_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\metadata\harg-0000-web-202006-0004_1176433_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\metadata\harg-0000-web-202006-0004_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\metadata\harg-0000-web-202006-0004_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\metadata\harg-0000-web-202006-0004_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\metadata\harg-0000-web-202006-0004_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\2173769_31104307305_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1180178_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\objects\ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\2184592_31104308049_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\harg-0000-web-202006-0006_1122966_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\harg-0000-web-202006-0006_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\harg-0000-web-202006-0006_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\harg-0000-web-202006-0006_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\harg-0000-web-202006-0006_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\harg-0000-web-202006-0006_seedscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\2184360_31104307875_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_1122766_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_seedscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\metadata\2202440_31104315076_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\metadata\ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\metadata\harg-0000-web-202006-0008_1137665_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\metadata\harg-0000-web-202006-0008_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\metadata\harg-0000-web-202006-0008_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\metadata\harg-0000-web-202006-0008_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\metadata\harg-0000-web-202006-0008_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\objects\ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\metadata\2173766_31104303986_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\metadata\harg-0000-web-202006-0009_1115523_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\metadata\harg-0000-web-202006-0009_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\metadata\harg-0000-web-202006-0009_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\metadata\harg-0000-web-202006-0009_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\metadata\harg-0000-web-202006-0009_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\2084781_31104281287_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\harg-0000-web-202006-0010_1043800_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\harg-0000-web-202006-0010_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\harg-0000-web-202006-0010_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\harg-0000-web-202006-0010_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\harg-0000-web-202006-0010_seedscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\objects\ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\objects\ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\objects\ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\metadata\2120634_31104281297_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\metadata\ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\metadata\harg-0000-web-202006-0011_1043812_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\metadata\harg-0000-web-202006-0011_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\metadata\harg-0000-web-202006-0011_collscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\metadata\harg-0000-web-202006-0011_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\metadata\harg-0000-web-202006-0011_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\objects\ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\bag-info.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\bagit.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\manifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\metadata\2084780_31104281289_crawldef.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\metadata\ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\metadata\harg-0000-web-202006-0012_1043802_crawljob.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\metadata\harg-0000-web-202006-0012_coll.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\metadata\harg-0000-web-202006-0012_preservation.xml",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\metadata\harg-0000-web-202006-0012_seed.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\metadata\harg-0000-web-202006-0012_seedscope.csv",
                      r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\objects\ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0001_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0002_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0003_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0004_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0005_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0006_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0007_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0008_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0009_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0010_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0011_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\harg-0000-web-202006-0012_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\rbrl-246-web-202006-0001_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\rbrl-343-web-202006-0001_preservation.xml",
                      r"aips_2020-06-09\preservation-xml\rbrl-447-web-202006-0001_preservation.xml",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\bag-info.txt",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\bagit.txt",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\manifest-md5.txt",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\2122426_31104282900_crawldef.csv",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\rbrl-246-web-202006-0001_1046326_crawljob.csv",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\rbrl-246-web-202006-0001_coll.csv",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\rbrl-246-web-202006-0001_collscope.csv",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\rbrl-246-web-202006-0001_preservation.xml",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\rbrl-246-web-202006-0001_seed.csv",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\rbrl-246-web-202006-0001_seedscope.csv",
                      r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\objects\ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\bag-info.txt",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\bagit.txt",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\manifest-md5.txt",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\2070895_31104276639_crawldef.csv",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc_fits.xml",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc_fits.xml",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc_fits.xml",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\rbrl-343-web-202006-0001_1033772_crawljob.csv",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\rbrl-343-web-202006-0001_coll.csv",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\rbrl-343-web-202006-0001_collscope.csv",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\rbrl-343-web-202006-0001_preservation.xml",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\rbrl-343-web-202006-0001_seed.csv",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\rbrl-343-web-202006-0001_seedscope.csv",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\objects\ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\objects\ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc",
                      r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\objects\ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\bag-info.txt",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\bagit.txt",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\manifest-md5.txt",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\manifest-sha256.txt",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\tagmanifest-md5.txt",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\tagmanifest-sha256.txt",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\2018084_31104276333_crawldef.csv",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc_fits.xml",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_1033408_crawljob.csv",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_coll.csv",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_collscope.csv",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_preservation.xml",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_seed.csv",
                      r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\objects\ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\metadata\2089428_31104269611_crawldef.csv",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\metadata\rbrl-459-web-202006-0001_1010662_crawljob.csv",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\metadata\rbrl-459-web-202006-0001_coll.csv",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\metadata\rbrl-459-web-202006-0001_collscope.csv",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\metadata\rbrl-459-web-202006-0001_seed.csv",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\metadata\rbrl-459-web-202006-0001_seedscope.csv",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\objects\ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\objects\ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\objects\ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\objects\ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\objects\ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\objects\ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\objects\ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc",
                      r"aips_2020-06-09\rbrl-459-web-202006-0001\objects\ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc"]

# Creates a list with relative paths starting at the AIPs directory so paths are predictable.
# Removes the size from the file names in aips-to-ingest since they vary slightly each time they are made.
directory_list = []
for root, dirs, files in os.walk("aips_2020-06-09"):
    for file in files:
        if root == r"aips_2020-06-09\aips-to-ingest" and file.endswith(".tar.bz2"):
            file = re.sub(r"_bag.\d+.tar", "_bag.tar", file)
        directory_list.append(os.path.join(root, file))

# Makes dataframes of the expected and actual directory lists and compares them.
actual_dir = pd.DataFrame({"File_Path": directory_list})
expected_dir = pd.DataFrame({"File_Path": directory_expected})
compare_dir_df = actual_dir.merge(expected_dir, indicator=True, how="outer")

# Saves the result of all three comparisons to a spreadsheet.
with pd.ExcelWriter(os.path.join(c.script_output, "error_test_results.xlsx")) as results:
    compare_seed_df.to_excel(results, sheet_name="Seeds_CSV", index=False)
    compare_check_df.to_excel(results, sheet_name="completeness_check")
    compare_dir_df.to_excel(results, sheet_name="Directory", index=False)

print('\nScript is complete.')
