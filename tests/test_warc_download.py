"""
Tests the output of the warc_download.py script.
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
        Deletes the preservation folder with all the downloaded seed contents and the CSV files created by the tests.
        """
        shutil.rmtree(os.path.join(c.script_output, "preservation_download"))

        # TODO: need to rework this function.
        # os.remove(os.path.join(c.script_output, "completeness_check.csv"))
        os.remove(os.path.join(c.script_output, "seeds_log.csv"))

    def test_multi_warc_seed(self):
        """
        Tests the full script with a data range that has 1 Hargrett seed with 3 WARCs and multiple crawl jobs.
        Results for testing are the contents of the three CSVs made by the script.
        """
        script_path = os.path.join("..", "warc_download.py")
        subprocess.run(f"python {script_path} 2020-04-30 2020-05-15", shell=True)

        # Test for metadata.csv
        metadata_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_metadata = [metadata_df.columns.tolist()] + metadata_df.values.tolist()
        expected_metadata = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                             ["hargrett", "harg-0000", 2173769, "harg-0000-web-202005-0001",
                              "Coronavirus (COVID-19) Information and Resources website", 1]]
        self.assertEqual(actual_metadata, expected_metadata, "Problem with test for multi WARC seed, metadata.csv")

        # Test for seeds_log.csv
        # Changes column with time stamps to allow comparison to consistent expected values.
        seeds_df = pd.read_csv(os.path.join(c.script_output, "seeds_log.csv"))
        seeds_df["WARC_Fixity_Errors"] = seeds_df["WARC_Fixity_Errors"].str.count("Successfully")
        expected_seeds = [seeds_df.columns.tolist()] + seeds_df.values.tolist()

        actual_seeds = [["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                         "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                        ["harg-0000-web-202005-0001", 2173769, 12912, "1154002;1148757;1143415", 0.007, 3,
                         "ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz;"
                         "ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz;"
                         "ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         "Successfully downloaded all metadata reports", "harg-0000-web-202005-0001_seedscope.csv",
                         "Successfully redacted",
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; "
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; "
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         3,
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; "
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; "
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         "Successfully completed"]]
        self.assertEqual(expected_seeds, actual_seeds, "Problem with test for multi WARC seed, seeds_log.csv")

        # TODO need to rework this function
        # # Test for completeness_check.csv
        # cc_df = pd.read_csv(os.path.join(c.script_output, "completeness_check.csv"))
        # expected_cc = [cc_df.columns.tolist()] + cc_df.values.tolist()
        # actual_cc = [["AIP", "URL", "AIP Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
        #               "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
        #              ["harg-0000-web-202005-0001", "https://www.uga.edu/coronavirus/",
        #               True, True, True, True, False, 1, 3, True, True]]
        # self.assertEqual(expected_cc, actual_cc, "Problem with test for multi WARC seed, completeness_check.csv")

    def test_one_warc_seeds(self):
        """
        Tests the full script with a data range that has 2 Russell seeds, each with 1 WARC.
        Results for testing are the contents of the three CSVs made by the script.
        """
        script_path = os.path.join("..", "warc_download.py")
        subprocess.run(f"python {script_path} 2019-07-12 2019-07-13", shell=True)

        # Test for metadata.csv
        metadata_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_metadata = [metadata_df.columns.tolist()] + metadata_df.values.tolist()
        expected_metadata = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                             ["russell", "rbrl-498", 2027707, "rbrl-498-web-201907-0001",
                              "Open Records with Deborah Gonzalez", 1],
                             ["russell", "rbrl-377", 2027776, "rbrl-377-web-201907-0001",
                              "Southeast ADA Center: Your Regional Resource for the "
                              "Americans with Disabilities Act (ADA)", 1]]
        self.assertEqual(actual_metadata, expected_metadata, "Problem with test for one WARC seeds, metadata.csv")

        # Test for seeds_log.csv
        # Changes column with time stamps to allow comparison to consistent expected values.
        seeds_df = pd.read_csv(os.path.join(c.script_output, "seeds_log.csv"))
        fixity = "Successfully verified ARCHIVEIT"
        seeds_df.loc[seeds_df["WARC_Fixity_Errors"].str.startswith(fixity), "WARC_Fixity_Errors"] = fixity
        actual_seeds = [seeds_df.columns.tolist()] + seeds_df.values.tolist()

        expected_seeds = [["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                         "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                        ["rbrl-498-web-201907-0001", 2027707, 12265, 943048, 0.007, 1,
                         "ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                         "Successfully downloaded all metadata reports", "No empty reports", "Successfully redacted",
                         "Successfully downloaded ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                         "Successfully verified ARCHIVEIT",
                         "Successfully unzipped ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                         "Successfully completed"],
                        ["rbrl-377-web-201907-0001", 2027776, 12264, 943446, 0.096, 1,
                         "ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                         "Successfully downloaded all metadata reports", "rbrl-377-web-201907-0001_seedscope.csv",
                         "Successfully redacted",
                         "Successfully downloaded ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                         "Successfully verified ARCHIVEIT",
                         "Successfully unzipped ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                         "Successfully completed"]]
        self.assertEqual(actual_seeds, expected_seeds, "Problem with test for one WARC seeds, seeds_log.csv")

        # TODO need to rework this function
        # # Test for completeness_check.csv
        # cc_df = pd.read_csv(os.path.join(c.script_output, "completeness_check.csv"))
        # expected_cc = [cc_df.columns.tolist()] + cc_df.values.tolist()
        # actual_cc = [["AIP", "URL", "AIP Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
        #               "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
        #              ["rbrl-377-web-201907-0001", "http://adasoutheast.org/",
        #               True, True, True, True, False, 1, 1, True, True],
        #              ["rbrl-498-web-201907-0001", "https://openrecords.podbean.com/",
        #               True, True, True, True, True, 1, 1, True, True]]
        # self.assertEqual(expected_cc, actual_cc, "Problem with test for one WARC seeds, completeness_check.csv")


if __name__ == '__main__':
    unittest.main()
