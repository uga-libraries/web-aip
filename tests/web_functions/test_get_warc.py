"""
Tests for get_warc() function.
It downloads and saves the WARC.

To save time, fake data is supplied in seed_df for fields that are not used in these tests
and seed_df only has the WARC being tested, no other WARCs for that seed.
"""
import numpy as np
import os
import pandas as pd
import shutil
import unittest
from web_functions import get_warc


class MyTestCase(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed directory and contents, if any, produced by the tests.
        """
        if os.path.exists(os.path.join(os.getcwd(), "2529656")):
            shutil.rmtree(os.path.join(os.getcwd(), "2529656"))

    def test_correct(self):
        """
        Tests that the function downloads the expected WARC and updates the log correctly
        when a correct WARC URL is given.
        """
        # Makes the data needed for the function input and runs the function.
        warc = "ARCHIVEIT-15678-TEST-JOB1594318-0-SEED2529656-20220420025307556-00000-k3n6tj0y.warc.gz"
        seed_df = pd.DataFrame([[2529656, 15678, "1594318", 0.01, 1, warc,
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"])
        os.mkdir("2529656")
        get_warc(seed_df, 0, f"https://warcs.archive-it.org/webdatafile/{warc}", warc, f"2529656/{warc}")

        # Test the WARC was downloaded.
        warc_downloaded = os.path.exists(os.path.join(os.getcwd(), "2529656", warc))
        self.assertEqual(warc_downloaded, True, "Problem with test for correct, WARC download")

        # Test the log is updated correctly.
        actual = seed_df["WARC_API_Errors"][0]
        expected = f"Successfully downloaded {warc}"
        self.assertEqual(actual, expected, "Problem with test for correct, log")

    def test_error(self):
        """
        Tests that the function does not download anything and updates the log correctly
        when an incorrect WARC URL is given, resulting in a get status code error.
        """
        # Makes the data needed for the function input and runs the function.
        warc = "ARCHIVEIT-error.warc.gz"
        seed_df = pd.DataFrame([[2529656, 15678, "1594318", 0.01, 1, warc,
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"])
        os.mkdir("2529656")

        with self.assertRaises(ValueError):
            get_warc(seed_df, 0, f"https://warcs.archive-it.org/webdatafile/{warc}", warc, f"2529656/{warc}")

        # Test the WARC was not downloaded.
        warc_downloaded = os.path.exists(os.path.join(os.getcwd(), "2529656", warc))
        self.assertEqual(warc_downloaded, False, "Problem with test for error, WARC download")

        # Test the log is updated correctly.
        actual = seed_df["WARC_API_Errors"][0]
        expected = f"API Error 404: can't download ARCHIVEIT-error.warc.gz"
        self.assertEqual(actual, expected, "Problem with test for error, log")


if __name__ == '__main__':
    unittest.main()
