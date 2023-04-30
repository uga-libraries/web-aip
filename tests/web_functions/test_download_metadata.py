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
        for seed_folder in ("2187482", "2529685", "2547528"):
            if os.path.exists(seed_folder):
                shutil.rmtree(os.path.join(os.getcwd(), seed_folder))

    def test_hargrett(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a Hargrett seed with one each of all six of the report types.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2187482, 12181, "1177700", 3.62, 3, "name0.warc.gz;name1.warc.gz;name2.warc.gz",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2187482")
        download_metadata(seed, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("2187482"):
            actual.append(filename)
        actual.sort()
        expected = ["2187482_1177700_crawljob.csv",
                    "2187482_31104333391_crawldef.csv",
                    "2187482_coll.csv",
                    "2187482_collscope.csv",
                    "2187482_seed.csv",
                    "2187482_seedscope.csv"]
        self.assertEqual(actual, expected, "Problem with test for Hargrett, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df["Metadata_Report_Errors"][0]
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for Hargrett, log errors")

        # Test that the log has the correct information for empty reports.
        actual_info = seed_df["Metadata_Report_Empty"][0]
        expected_info = "No empty reports"
        self.assertEqual(actual_info, expected_info, "Problem with test for Hargrett, log info")

    def test_magil(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a MAGIL seed with one each of the four report types which always have data.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2529685, 15678, "1594228", 0.36, 1, "name.warc.gz",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2529685")
        download_metadata(seed, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("2529685"):
            actual.append(filename)
        actual.sort()
        expected = ["2529685_1594228_crawljob.csv",
                    "2529685_31104546937_crawldef.csv",
                    "2529685_coll.csv",
                    "2529685_seed.csv"]
        self.assertEqual(actual, expected, "Problem with test for MAGIL, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df["Metadata_Report_Errors"][0]
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for MAGIL, log errors")

        # Test that the log has the correct information for empty reports.
        actual_info = seed_df["Metadata_Report_Empty"][0]
        expected_info = "2529685_seedscope.csv; 2529685_collscope.csv"
        self.assertEqual(actual_info, expected_info, "Problem with test for MAGIL, log info")

    def test_russell(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a Russell two each of the scope report types and one each of the other four report types.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2547528, 12265, "1436714;1718490", 0.72, 3,
                                 "name0.warc.gz;name1.warc.gz;name2.warc.gz",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2547528")
        download_metadata(seed, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("2547528"):
            actual.append(filename)
        actual.sort()
        expected = ["2547528_1436714_crawljob.csv",
                    "2547528_1718490_crawljob.csv",
                    "2547528_31104392189_crawldef.csv",
                    "2547528_31104463393_crawldef.csv",
                    "2547528_coll.csv",
                    "2547528_collscope.csv",
                    "2547528_seed.csv",
                    "2547528_seedscope.csv"]
        self.assertEqual(actual, expected, "Problem with test for RussellL, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df["Metadata_Report_Errors"][0]
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for Russell, log errors")

        # Test that the log has the correct information for empty reports.
        actual_info = seed_df["Metadata_Report_Empty"][0]
        expected_info = "No empty reports"
        self.assertEqual(actual_info, expected_info, "Problem with test for Russell, log info")


if __name__ == '__main__':
    unittest.main()
