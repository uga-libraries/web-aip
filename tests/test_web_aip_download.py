"""
Tests the output of the web_aip_download.py script.
It downloads metadata and WARCs for the seeds saved during the specified date range.
Multiple date ranges are used to test the main data variation while limiting the amount downloaded.
"""
import pandas as pd
import os
import shutil
import subprocess
import unittest
import configuration as c


class MyTestCase(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the AIP directories, their contents, and the CSV files created by the tests.
        """
        for directory in (os.path.join(c.script_output, "aips_2019-07-13"),
                          os.path.join(c.script_output, "aips_2020-02-20"),
                          os.path.join(c.script_output, "aips_2020-05-15")):
            if os.path.exists(directory):
                shutil.rmtree(directory)

        os.remove(os.path.join(c.script_output, "completeness_check.csv"))
        os.remove(os.path.join(c.script_output, "seeds.csv"))

    def test_multi_warc_seed(self):
        """
        Tests the full script with a data range that has 1 Hargrett seed with 3 WARCs and multiple crawl jobs.
        Results for testing are the contents of seeds.csv and completeness_check.csv.
        """
        script_path = os.path.join("..", "web_aip_download.py")
        subprocess.run(f"python {script_path} 2020-04-30 2020-05-15", shell=True)

        # Test for seeds.csv
        # Removes seed info about login columns and time stamps, which are not consistent for each test.
        seeds_df = pd.read_csv(os.path.join(c.script_output, "seeds.csv"))
        info = "Empty report harg-0000-web-202005-0001_seedscope.csv not saved"
        seeds_df.loc[seeds_df["Metadata_Report_Info"].str.startswith(info), "Metadata_Report_Info"] = info
        seeds_df.loc[seeds_df["Metadata_Report_Info"] == "Seed report does not have login columns to redact",
                     "Metadata_Report_Info"] = "No additional information"
        seeds_df["WARC_Fixity_Errors"] = seeds_df["WARC_Fixity_Errors"].str.count("Successfully")
        expected_seeds = [seeds_df.columns.tolist()] + seeds_df.values.tolist()

        actual_seeds = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                         "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                         "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                         "Size_GB"],
                        [2173769, "harg-0000-web-202005-0001",
                         "Coronavirus (COVID-19) Information and Resources website",
                         "hargrett", 0, 12912, "1154002,1148757,1143415", 3,
                         "ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz,"
                         "ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz,"
                         "ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
                         "Empty report harg-0000-web-202005-0001_seedscope.csv not saved",
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; "
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; "
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         3,
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; "
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; "
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         0.01]]
        self.assertEqual(expected_seeds, actual_seeds, "Problem with test for two WARC seed, seeds.csv")

        # Test for completeness_check.csv
        cc_df = pd.read_csv(os.path.join(c.script_output, "completeness_check.csv"))
        expected_cc = [cc_df.columns.tolist()] + cc_df.values.tolist()
        actual_cc = [["AIP", "URL", "AIP Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                      "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                     ["harg-0000-web-202005-0001", "https://www.uga.edu/coronavirus/",
                      True, True, True, True, False, 1, 3, True, True]]
        self.assertEqual(expected_cc, actual_cc, "Problem with test for two WARC seed, completeness_check.csv")

    def test_one_warc_seeds(self):
        """
        Tests the full script with a data range that has 2 Russell seeds, each with 1 WARC.
        Results for testing are the contents of seeds.csv and completeness_check.csv.
        """
        script_path = os.path.join("..", "web_aip_download.py")
        subprocess.run(f"python {script_path} 2019-07-12 2019-07-13", shell=True)

        # Test for seeds.csv
        # Removes seed info about login columns and time stamps, which are not consistent for each test.
        seeds_df = pd.read_csv(os.path.join(c.script_output, "seeds.csv"))
        info = "Empty report rbrl-377-web-201907-0001_seedscope.csv not saved"
        seeds_df.loc[seeds_df["Metadata_Report_Info"].str.startswith(info), "Metadata_Report_Info"] = info
        seeds_df.loc[seeds_df["Metadata_Report_Info"] == "Seed report does not have login columns to redact",
                     "Metadata_Report_Info"] = "No additional information"
        fixity = "Successfully verified ARCHIVEIT"
        seeds_df.loc[seeds_df["WARC_Fixity_Errors"].str.startswith(fixity), "WARC_Fixity_Errors"] = fixity
        expected_seeds = [seeds_df.columns.tolist()] + seeds_df.values.tolist()

        actual_seeds = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                         "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                         "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                         "Size_GB"],
                        [2027776, "rbrl-377-web-201907-0001",
                         "Southeast ADA Center: Your Regional Resource for the Americans with Disabilities Act (ADA)",
                         "russell", "rbrl-377", 12264, 943446, 1,
                         "ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
                         "Empty report rbrl-377-web-201907-0001_seedscope.csv not saved",
                         "Successfully downloaded ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                         "Successfully verified ARCHIVEIT",
                         "Successfully unzipped ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                         0.1],
                        [2027707, "rbrl-498-web-201907-0001", "Open Records with Deborah Gonzalez", "russell",
                         "rbrl-498", 12265, 943048, 1,
                         "ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
                         "No additional information",
                         "Successfully downloaded ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                         "Successfully verified ARCHIVEIT",
                         "Successfully unzipped ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                         0.01]]
        self.assertEqual(expected_seeds, actual_seeds, "Problem with test for one WARC seeds, seeds.csv")

        # Test for completeness_check.csv
        cc_df = pd.read_csv(os.path.join(c.script_output, "completeness_check.csv"))
        expected_cc = [cc_df.columns.tolist()] + cc_df.values.tolist()
        actual_cc = [["AIP", "URL", "AIP Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                      "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                     ["rbrl-377-web-201907-0001", "http://adasoutheast.org/",
                      True, True, True, True, False, 1, 1, True, True],
                     ["rbrl-498-web-201907-0001", "https://openrecords.podbean.com/",
                      True, True, True, True, True, 1, 1, True, True]]
        self.assertEqual(expected_cc, actual_cc, "Problem with test for one WARC seeds, completeness_check.csv")

    def test_skip_seed(self):
        """
        Tests the full script with a data range that skips a seed without all the metadata.
        1 Russell seed with 1 WARC will be downloaded.
        Results for testing are the contents of seeds.csv and completeness_check.csv.
        """
        script_path = os.path.join("..", "web_aip_download.py")
        subprocess.run(f"python {script_path} 2020-02-10 2020-02-20", shell=True)

        # Test for seeds.csv
        # Removes seed info about login columns and time stamps, which are not consistent for each test.
        seeds_df = pd.read_csv(os.path.join(c.script_output, "seeds.csv"))
        seeds_df = seeds_df.fillna("")
        info = "Empty report rbrl-246-web-202002-0001_seedscope.csv not saved"
        seeds_df.loc[seeds_df["Metadata_Report_Info"].str.startswith(info), "Metadata_Report_Info"] = info
        seeds_df.loc[seeds_df["Metadata_Report_Info"] == "Seed report does not have login columns to redact",
                     "Metadata_Report_Info"] = "No additional information"
        fixity = "Successfully verified ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz"
        seeds_df.loc[seeds_df["WARC_Fixity_Errors"].str.startswith(fixity), "WARC_Fixity_Errors"] = fixity
        expected_seeds = [seeds_df.columns.tolist()] + seeds_df.values.tolist()

        actual_seeds = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                         "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                         "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                         "Size_GB"],
                        [2028986, "", "The Now Explosion Website", "", "", 12470, 1085452, 2,
                         "ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129224159397-00001-h3.warc.gz,"
                         "ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129213514425-00000-h3.warc.gz",
                         "Couldn't get all required metadata values from the seed report. Will not download files.",
                         "", "", "", "", "", 1.3],
                        [2122426, "rbrl-246-web-202002-0001", "Johnny Isakson YouTube Channel",
                         "russell", "rbrl-246", 12265, 1046326, 1,
                         "ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
                         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
                         "No additional information",
                         "Successfully downloaded ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
                         "Successfully verified ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
                         "Successfully unzipped ARCHIVEIT-12265-TEST-JOB1046326-SEED2122426-20191219180253353-00000-h3.warc.gz",
                         0.18]]
        self.assertEqual(expected_seeds, actual_seeds, "Problem with test for skip seed, seeds.csv")

        # Test for completeness_check.csv
        cc_df = pd.read_csv(os.path.join(c.script_output, "completeness_check.csv"))
        expected_cc = [cc_df.columns.tolist()] + cc_df.values.tolist()
        actual_cc = [["AIP", "URL", "AIP Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                      "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                     ["rbrl-246-web-202002-0001",
                      "https://www.youtube.com/channel/UC-LF69SBOSgT1S1-yy-VgaA/videos?view=0&sort=dd&shelf_id=0",
                      True, True, True, True, True, 1, 1, True, True]]
        self.assertEqual(expected_cc, actual_cc, "Problem with test for one skip seed, completeness_check.csv")


if __name__ == '__main__':
    unittest.main()
