"""
Tests for the get_report function.
It downloads six types of reports from the Partner API and updates the log.

The tests are just confirming that the reports were downloaded.
Tests for other functions confirm the contents of the reports.
"""
import numpy as np
import os
import pandas as pd
import shutil
import unittest
from web_functions import download_metadata


class MyTestCase(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folders and any contents from the tests.
        """
        for seed_folder in ("error-000-web-0001", "harg-000-web-0001", "magil-000-web-0001", "rbrl-000-web-0001"):
            if os.path.exists(seed_folder):
                shutil.rmtree(os.path.join(os.getcwd(), seed_folder))

    def test_hargrett(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a Hargrett seed with one each of all six of the report types.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2187482, "harg-000-web-0001", "Student Government Association Facebook",
                                 "Hargrett Rare Book & Manuscript Library", "0000", 12181, "1177700", 3,
                                 "name0.warc.gz,name1.warc.gz,name2.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 3.62]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                        "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("harg-000-web-0001")
        download_metadata(seed, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("harg-000-web-0001"):
            actual.append(filename)
        actual.sort()
        expected = ["harg-000-web-0001_1177700_crawljob.csv",
                    "harg-000-web-0001_31104333391_crawldef.csv",
                    "harg-000-web-0001_coll.csv",
                    "harg-000-web-0001_collscope.csv",
                    "harg-000-web-0001_seed.csv",
                    "harg-000-web-0001_seedscope.csv"]
        self.assertEqual(actual, expected, "Problem with test for Hargrett, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df["Metadata_Report_Errors"][0]
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for Hargrett, log errors")

        # Test that the log has the correct information for additional metadata information.
        actual_info = seed_df["Metadata_Report_Info"][0]
        expected_info = "No additional information"
        self.assertEqual(actual_info, expected_info, "Problem with test for Hargrett, log info")

    def test_magil(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a MAGIL seed with one each of the four report types which always have data.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2529685, "magil-000-web-0001", "Teachers Retirement System of Georgia",
                                 "Map and Government Information Library", "0000", 15678, "1594228", 1,
                                 "name.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 0.36]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                        "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("magil-000-web-0001")
        download_metadata(seed, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("magil-000-web-0001"):
            actual.append(filename)
        actual.sort()
        expected = ["magil-000-web-0001_1594228_crawljob.csv",
                    "magil-000-web-0001_31104546937_crawldef.csv",
                    "magil-000-web-0001_coll.csv",
                    "magil-000-web-0001_seed.csv"]
        self.assertEqual(actual, expected, "Problem with test for MAGIL, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df["Metadata_Report_Errors"][0]
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for MAGIL, log errors")

        # Test that the log has the correct information for additional metadata information.
        actual_info = seed_df["Metadata_Report_Info"][0]
        expected_info = "Empty report magil-000-web-0001_seedscope.csv not saved; " \
                        "Empty report magil-000-web-0001_collscope.csv not saved"
        self.assertEqual(actual_info, expected_info, "Problem with test for MAGIL, log info")

    def test_russell(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a Russell two each of the scope report types and one each of the other four report types.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2547528, "rbrl-000-web-0001", "Latina South Podcast - Posts | Facebook",
                                 "Map and Government Information Library", "0000", 12265, "1436714,1718490", 3,
                                 "name0.warc.gz,name1.warc.gz,name2.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 0.72]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                        "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("rbrl-000-web-0001")
        download_metadata(seed, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("rbrl-000-web-0001"):
            actual.append(filename)
        actual.sort()
        expected = ["rbrl-000-web-0001_1436714_crawljob.csv",
                    "rbrl-000-web-0001_1718490_crawljob.csv",
                    "rbrl-000-web-0001_31104392189_crawldef.csv",
                    "rbrl-000-web-0001_31104463393_crawldef.csv",
                    "rbrl-000-web-0001_coll.csv",
                    "rbrl-000-web-0001_collscope.csv",
                    "rbrl-000-web-0001_seed.csv",
                    "rbrl-000-web-0001_seedscope.csv"]
        self.assertEqual(actual, expected, "Problem with test for RussellL, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df["Metadata_Report_Errors"][0]
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for Russell, log errors")

        # Test that the log has the correct information for additional metadata information.
        actual_info = seed_df["Metadata_Report_Info"][0]
        expected_info = "No additional information"
        self.assertEqual(actual_info, expected_info, "Problem with test for Russell, log info")


if __name__ == '__main__':
    unittest.main()
