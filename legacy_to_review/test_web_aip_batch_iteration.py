"""Purpose: Test that the script is able to restart after errors. A pre-determined date range is used to allow
automatic verification that the test script has produced the expected outputs. The functions from web_functions.py
are used as much as possible for a more authentic test, but test versions of these functions are sometimes needed
to speed up the download process and to generate errors.

Usage: python /path/test_web_aip_batch_iteration
 """

import datetime
import numpy as np
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


def make_expected_seed_df():
    """Makes and returns a dataframe with the expected values for seed_df. Starts by making a list of the values
        for each column in the dataframe."""

    rows = [["2018084", "rbrl-447-web-202006-0001", "K7MOA Legacy Voteview Website",
             "russell", "rbrl-447", "12262", "1033408", "0.67", "1",
             "ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "Empty report rbrl-447-web-202006-0001_seedscope.csv not saved",
             "Successfully downloaded ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors",
             "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2028986", np.NaN, "The Now Explosion Website", np.NaN, np.NaN, "12470", "1085452", "1.3", "2",
             "ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129224159397-00001-h3.warc.gz,ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129213514425-00000-h3.warc.gz",
             "Couldn't get all required metadata values from the seed report. Will not download files or make AIP.",
             np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN,
             np.NaN, np.NaN, np.NaN],

            ["2030942", "harg-0000-web-202006-0002", "UGA NAACP Twitter",
             "hargrett", "0000", "12181", "1177700", "0.05", "1",
             "ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "No additional information",
             "Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors", "Successfully created combined-fits.xml",
             "Successfully created preservation.xml", "Preservation.xml valid on", "Bag valid on",
             "Successfully made package", "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2070895", "rbrl-343-web-202006-0001", "Statewide Independent Living Council of Georgia",
             "russell", "rbrl-343", "12264", "1033772", "3.64", "3",
             "ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc.gz,ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc.gz,ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc.gz",
             "Successfully calculated seed metadata",
             "Successfully downloaded all metadata reports", "No additional information",
             "Successfully downloaded ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc.gz; Successfully downloaded ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc.gz; Successfully downloaded ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc.gz",
             "Successfully verified ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc.gz fixity on; Successfully verified ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc.gz fixity on; Successfully verified ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121135204052-00002-l6w381ei.warc.gz; Successfully unzipped ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121133208081-00001-l6w381ei.warc.gz; Successfully unzipped ARCHIVEIT-12264-TEST-JOB1033772-0-SEED2070895-20191121130726270-00000-l6w381ei.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors",
             "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2084780", "harg-0000-web-202006-0012", "UGA Athletics Twitter",
             "hargrett", "0000", "12907", "1043802", "0.03", "1",
             "ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "Empty report harg-0000-web-202006-0012_collscope.csv not saved",
             "Successfully downloaded ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12907-TEST-JOB1043802-SEED2084780-20191211212227155-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors",
             "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2084781", "harg-0000-web-202006-0010", "UGA Football Facebook",
             "hargrett", "0000", "12907", "1043800", "2.29", "3",
             "ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc.gz,ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc.gz,ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc.gz",
             "Successfully calculated seed metadata",
             "Successfully downloaded all metadata reports",
             "Empty report harg-0000-web-202006-0010_collscope.csv not saved",
             "Successfully downloaded ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212049009-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191211212347237-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12907-TEST-JOB1043800-SEED2084781-20191212001005445-00002-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors",
             "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2084785", "harg-0000-web-202006-0003", "Zeta Pi Chapter of Alpha Phi Alpha Twitter",
             "hargrett", "0000", "12181", "1177700", "0.11", "1",
             "ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "No additional information",
             "Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors", "Successfully created combined-fits.xml",
             "Successfully created preservation.xml", "Preservation.xml valid on", "Bag valid on",
             "Successfully made package", "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2089428", "rbrl-459-web-202006-0001", "Southern Alliance for Clean Energy",
             "russell", "rbrl-459", "12263", "1010662", "16.67", "19",
             "ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191023012315001-00008-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191023232420715-00009-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191024225325499-00010-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191025205356890-00011-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191026180403577-00012-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191027150309943-00013-h3-SALVAGED.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191027222744036-00014-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191027223442169-00015-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191029035842313-00016-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191030020954917-00017-h3.warc.gz,ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191030232552542-00018-h3.warc.gz",
            "Successfully calculated seed metadata",
            "Successfully downloaded all metadata reports", "No additional information",
            "Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc.gz; Successfully downloaded ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc.gz",
            "Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc.gz fixity on",
            "Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021140130019-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021171823909-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021174135718-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191021212110287-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022001451043-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022003507368-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022021604194-00006-h3.warc.gz; Successfully unzipped ARCHIVEIT-12263-TEST-JOB1010662-SEED2089428-20191022105907919-00007-h3.warc.gz",
            np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2120634", "harg-0000-web-202006-0011", "UGA Office of Institutional Research website",
             "hargrett", "0000", "12912", "1043812", "0.59", "1",
            "ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc.gz",
            "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
            "Empty report harg-0000-web-202006-0011_seedscope.csv not saved",
            "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc.gz",
            "Successfully verified ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc.gz fixity on",
            "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1043812-SEED2120634-20191211213407679-00000-h3.warc.gz",
            "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
            "No FITS tools errors",
            "Successfully created combined-fits.xml", "Successfully created preservation.xml",
            "Preservation.xml valid on", "Bag valid on", "Successfully made package",
            "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2122426", "rbrl-246-web-202006-0001", "Johnny Isakson YouTube Channel",
             "russell", "rbrl-246", "12265", "1046326", "0.18", "1",
             "ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "No additional information",
             "Successfully downloaded ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors", "Successfully created combined-fits.xml",
             "Successfully created preservation.xml", "Preservation.xml valid on", "Bag valid on",
             "Successfully made package", "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2173766", "harg-0000-web-202006-0009", "University of Georgia homepage",
             "hargrett", "0000", "12912", "1115523", "0.08", "1",
             "ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "Empty report harg-0000-web-202006-0009_seedscope.csv not saved",
             "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1115523-SEED2173766-20200326211216752-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors",
             "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2173769", "harg-0000-web-202006-0005", "Coronavirus (COVID-19) Information and Resources website",
             "hargrett", "0000", "12912",
             "1180178,1174790,1160413,1154002,1148757,1143415,1138866,1130525,1130530,1115532", "0.02", "10",
             "ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc.gz,ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "Empty report harg-0000-web-202006-0005_seedscope.csv not saved",
             "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1180178-SEED2173769-20200604034857305-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1174790-SEED2173769-20200527221806125-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1160413-SEED2173769-20200520233440915-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1138866-SEED2173769-20200423074942091-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130525-SEED2173769-20200416031337869-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1130530-SEED2173769-20200416032358049-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors", "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2184360", "harg-0000-web-202006-0007", "UGA Today",
             "hargrett", "0000", "12912", "1122766,1155626,1130531,1130526", "70.78", "62",
             "ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc.gz,ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc.gz,ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc.gz",
             "Successfully calculated seed metadata",
             "Successfully downloaded all metadata reports", "No additional information",
             "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200406162700376-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200407234140334-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200408130212270-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409023318272-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122766-SEED2184360-20200409060807591-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518132934870-00016-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518131337456-00015-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518125011505-00014-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518123847456-00013-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518122920575-00012-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518121823601-00011-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120543379-00010-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518120004297-00009-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518115430486-00008-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518113355447-00007-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518110234798-00006-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518104323186-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518102730964-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200518082421856-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517191839989-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200517074157513-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1155626-SEED2184360-20200516002718093-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424002847936-00034-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424001927641-00033-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200424000615846-00032-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423235506091-00031-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423234652890-00030-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423233430888-00029-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200423032552412-00028-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200422112404132-00027-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421232546625-00026-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421173343214-00025-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421171359626-00024-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421165804754-00023-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200421143108960-00022-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416031609218-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417073836591-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417080537836-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200417162052872-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-TEST-JOB1130526-SEED2184360-20200416143834702-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200419022328275-00021-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418233001610-00020-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418170707078-00019-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418130154514-00018-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200418034650942-00017-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417184426457-00016-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417141018314-00015-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417112703839-00014-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417010515435-00012-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200417111844304-00013-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416051116851-00011-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416050050747-00010-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416045432434-00009-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044758890-00008-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416044237931-00007-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043735875-00006-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416043039082-00005-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416042140522-00004-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416041303030-00003-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416040452697-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416035727594-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12912-MONTHLY-JOB1130531-SEED2184360-20200416032406054-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors",
             "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2184592", "harg-0000-web-202006-0006", "UGA Twitter",
             "hargrett", "0000", "12912", "1122966", "0.28", "1",
             "ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc.gz",
             "Successfully calculated seed metadata",
             "Successfully downloaded all metadata reports", "No additional information",
             "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1122966-SEED2184592-20200406182012644-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors",
             "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2187482", "harg-0000-web-202006-0001", "Student Government Association Facebook",
             "hargrett", "0000", "12181", "1177700", "3.62", "4",
             "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz,ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports", 
             "No additional information",
             "Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz; Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz; Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz; Successfully downloaded ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz fixity on; Successfully verified ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz; Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz; Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz; Successfully unzipped ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors", "Successfully created combined-fits.xml",
             "Successfully created preservation.xml", "Preservation.xml valid on", "Bag valid on", 
             "Successfully made package", "Successfully added AIP to manifest.", "Successfully completed processing"],

            ["2202440", "harg-0000-web-202006-0008", "UGA Healthy Dawg - University Health Center",
             "hargrett", "0000", "12181", "1137665", "0.43", "1",
             "ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "Empty report harg-0000-web-202006-0008_seedscope.csv not saved",
             "Successfully downloaded ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12181-TEST-JOB1137665-SEED2202440-20200421192318754-00000-h3.warc.gz",
             np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],

            ["2270486", "harg-0000-web-202006-0004", "UGA Commencement",
             "hargrett", "0000", "12912", "1176433", "0.06", "1",
             "ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
             "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
             "Empty report harg-0000-web-202006-0004_seedscope.csv not saved",
             "Successfully downloaded ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
             "Successfully verified ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz fixity on",
             "Successfully unzipped ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
             "No files deleted", "Successfully created objects folder", "Successfully created metadata folder",
             "No FITS tools errors",
             "Successfully created combined-fits.xml", "Successfully created preservation.xml",
             "Preservation.xml valid on", "Bag valid on", "Successfully made package",
             "Successfully added AIP to manifest.", "Successfully completed processing"]]

    column_names = ["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                    "Size_GB", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                    "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                    "Files Deleted", "Objects Folder", "Metadata Folder", "FITS Tool Errors",
                    "FITS Combination Errors", "Preservation.xml Made", "Preservation.xml Valid",
                    "Bag Valid", "Package Errors", "Manifest Errors", "Processing Complete"]

    df = pd.DataFrame(rows, columns=column_names)
    return df


def make_expected_completeness_check_df():
    """Makes a dataframe with the expected values for the completeness_check.csv that this script will produce
    if everything works correctly."""

    rows = [["harg-0000-web-202006-0001", "https://www.facebook.com/ugasga/", True, True, True, True, True, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0002", "https://twitter.com/uganaacp/", True, True, True, True, True, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0003", "https://twitter.com/UGA_Alphas/", True, True, True, True, True, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0004", "https://commencement.uga.edu/", True, True, True, True, False, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0005", "https://www.uga.edu/coronavirus/", True, True, True, True, False, 3, 10, True, True, True, True, True],
            ["harg-0000-web-202006-0006", "https://twitter.com/universityofga/", True, True, True, True, True, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0007",  "https://news.uga.edu/", True, True, True, True, True, 3, 4, True, True, True, True, True],
            ["harg-0000-web-202006-0008", "https://ugahealthydawg.com/", True, True, True, True, False, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0009", "https://www.uga.edu/", True, True, True, True, False, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0010", "https://www.facebook.com/FootballUGA/", True, True, False, True, True, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0011", "https://oir.uga.edu/", True, True, True, True, False, 1, 1, True, True, True, True, True],
            ["harg-0000-web-202006-0012", "https://twitter.com/UGAAthletics/", True, True, False, True, True, 1, 1, True, True, True, True, True],
            ["rbrl-246-web-202006-0001", "https://www.youtube.com/channel/UC-LF69SBOSgT1S1-yy-VgaA/videos?view=0&sort=dd&shelf_id=0", True, True, True, True, True, 1, 1, True, True, True, True, True],
            ["rbrl-343-web-202006-0001", "https://www.silcga.org/", True, True, True, True, True, 1, 1, True, True, True, True, True],
            ["rbrl-447-web-202006-0001", "http://k7moa.com/site_map.htm", True, True, True, True, False, 1, 1, True, True, True, True, True],
            ["rbrl-459-web-202006-0001", "https://cleanenergy.org/", False, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]

    column_names = ["AIP", "URL", "AIP Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                    "crawldef.csv count", "crawljob.csv count", "preservation.xml", "WARC Count Correct",
                    "Objects is all WARCs", "fits.xml Count Correct", "No Extra Metadata"]

    df = pd.DataFrame(rows, columns=column_names)
    return df


def make_directory_structure_df():
    """Makes and returns a dataframe with the expected relative path for every file in the aips directory.
    Starts by making a list of the paths."""

    paths = [r"aips_2020-06-09\completeness_check.csv",
             r"aips_2020-06-09\seeds.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0001_bag\data\metadata\harg-0000-web-202006-0001_31104333391_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0002_bag\data\metadata\harg-0000-web-202006-0002_31104333391_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0003_bag\data\metadata\harg-0000-web-202006-0003_31104333391_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0004_bag\data\metadata\harg-0000-web-202006-0004_31104332809_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_31104303994_crawldef.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_31104307305_crawldef.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_31104312481_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1115532_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1130525_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1130530_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1138866_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1143415_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1148757_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1154002_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1160413_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0005_bag\data\metadata\harg-0000-web-202006-0005_1174790_crawljob.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0006_bag\data\metadata\harg-0000-web-202006-0006_31104308049_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_31104307871_crawldef.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_31104307875_crawldef.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_31104312482_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_1130526_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_1130531_crawljob.csv",
             r"aips_2020-06-09\harg-0000-web-202006-0007_bag\data\metadata\harg-0000-web-202006-0007_1155626_crawljob.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0008_bag\data\metadata\harg-0000-web-202006-0008_31104315076_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0009_bag\data\metadata\harg-0000-web-202006-0009_31104303986_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0010_bag\data\metadata\harg-0000-web-202006-0010_31104281287_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0011_bag\data\metadata\harg-0000-web-202006-0011_31104281297_crawldef.csv",
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
             r"aips_2020-06-09\harg-0000-web-202006-0012_bag\data\metadata\harg-0000-web-202006-0012_31104281289_crawldef.csv",
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
             r"aips_2020-06-09\rbrl-246-web-202006-0001_bag\data\metadata\rbrl-246-web-202006-0001_31104282900_crawldef.csv",
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
             r"aips_2020-06-09\rbrl-343-web-202006-0001_bag\data\metadata\rbrl-343-web-202006-0001_31104276639_crawldef.csv",
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
             r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_31104276333_crawldef.csv",
             r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc_fits.xml",
             r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_1033408_crawljob.csv",
             r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_coll.csv",
             r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_collscope.csv",
             r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_preservation.xml",
             r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\metadata\rbrl-447-web-202006-0001_seed.csv",
             r"aips_2020-06-09\rbrl-447-web-202006-0001_bag\data\objects\ARCHIVEIT-12262-TEST-JOB1033408-SEED2018084-20191120195755490-00000-h3.warc",
             r"aips_2020-06-09\rbrl-459-web-202006-0001\metadata\rbrl-459-web-202006-0001_31104269611_crawldef.csv",
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

    df = pd.DataFrame({"File_Path": paths})
    return df


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
        web.reset_seed(seed.AIP_ID, seed_df)
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
    if aip.id in os.listdir("../tests"):
        a.extract_metadata(aip)

    # Transforms the FITS metadata into the PREMIS preservation.xml file using saxon and xslt stylesheets.
    if aip.id in os.listdir("../tests"):
        a.make_preservationxml(aip)

    # Bags the aip.
    if aip.id in os.listdir("../tests"):
        a.bag(aip)

    # TESTING ITERATION: SCRIPT BREAKS BETWEEN GENERAL AIP STEPS FOR A SEED.
    if seed.Seed_ID == "2202440" and reset is False:
        print("Simulating script breaking after bagging.")
        sys.exit()

    # Tars and zips the aip.
    if f"{aip.id}_bag" in os.listdir('../tests'):
        a.package(aip)

    # Adds the packaged AIP to the MD5 manifest in the aips-to-ingest folder.
    if f'{aip.id}_bag' in os.listdir('../tests'):
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
web.check_seeds(date_end, date_start, seed_df, aips_directory)

# Moves script output folders (aips-to-ingest, errors, fits-xml, and preservation-xml) and logs into the AIPs folder
# to keep everything together if another set is downloaded before these are deleted.
to_move = ("aips-to-ingest", "errors", "fits-xml", "preservation-xml",
           "seeds.csv", "completeness_check.csv")
for item in os.listdir("../tests"):
    if item in to_move:
        os.replace(item, f"{aips_directory}/{item}")

# ------------------------------------------------------------------------------------------------------------------
# The rest of the script compares seeds.csv, completeness_check.csv, and the directory structure produced by the test
# to expected values and creates a spreadsheet with the results.
# ------------------------------------------------------------------------------------------------------------------

# Checks the values in seed_df against the expected values.
# Removes timestamps and login column error that is only sometimes present so the data can be compared to consistent expected values.
expected_seeds_df = make_expected_seed_df()
seed_df["WARC_Fixity_Errors"] = seed_df["WARC_Fixity_Errors"].str.replace(" \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}", "")
seed_df["Preservation.xml Valid"] = seed_df["Preservation.xml Valid"].str.replace(" \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}", "")
seed_df["Bag Valid"] = seed_df["Bag Valid"].str.replace(" \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}", "")
seed_df["Metadata_Report_Info"] = seed_df["Metadata_Report_Info"].str.replace("Seed report does not have login columns to redact; ", "")
seed_df["Metadata_Report_Info"] = seed_df["Metadata_Report_Info"].str.replace("Seed report does not have login columns to redact", "No additional information")
compare_seed_df = seed_df.merge(expected_seeds_df, indicator=True, how="outer")

# Compares the values in completeness_check.csv against the epxected values.
check_df = pd.read_csv("aips_2020-06-09/completeness_check.csv")
expected_check_df = make_expected_completeness_check_df()
compare_check_df = check_df.merge(expected_check_df, indicator=True, how="outer")

# Checks the directory structure against the expected directory structure.
# Makes a dataframe with the relative paths in the AIPs directory structure and compares to the expected paths.
# Removes the size from the file names in aips-to-ingest since they vary slightly each time they are made.
directory_list = []
for root, dirs, files in os.walk("aips_2020-06-09"):
    for file in files:
        if root == r"aips_2020-06-09\aips-to-ingest" and file.endswith(".tar.bz2"):
            file = re.sub(r"_bag.\d+.tar", "_bag.tar", file)
        directory_list.append(os.path.join(root, file))
directory_df = pd.DataFrame({"File_Path": directory_list})
expected_directory_df = make_directory_structure_df()
compare_dir_df = directory_df.merge(expected_directory_df, indicator=True, how="outer")

# Saves the result of all three comparisons to a spreadsheet.
with pd.ExcelWriter(os.path.join(c.script_output, "error_test_results.xlsx")) as results:
    compare_seed_df.to_excel(results, sheet_name="Seeds_CSV", index=False)
    compare_check_df.to_excel(results, sheet_name="completeness_check", index=False)
    compare_dir_df.to_excel(results, sheet_name="Directory", index=False)

# Removes the rows that match from the compare dataframes and prints if there are any mismatches remaining.
compare_seed_df = compare_seed_df[compare_seed_df["_merge"] != "both"]
if len(compare_seed_df) == 0:
    print("Tests passed for seeds_df")
else:
    print("Possible errors with seeds_df: check error_test_results.xlsx")

compare_check_df = compare_check_df[compare_check_df["_merge"] != "both"]
if len(compare_check_df) == 0:
    print("Tests passed for check_completeness_log.csv")
else:
    print("Possible errors with check_completeness_log.csv: check error_test_results.xlsx")

compare_dir_df = compare_dir_df[compare_dir_df["_merge"] != "both"]
if len(compare_dir_df) == 0:
    print("Tests passed for directory structure.")
else:
    print("Possible errors with directory structure: check error_test_results.xlsx")

print('\nScript is complete.')
