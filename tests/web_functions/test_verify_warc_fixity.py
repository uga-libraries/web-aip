"""
Tests for the verify_warc_fixity() function.
It compares the fixity of the downloaded WARC to Archive-It and deletes the file if it does not match.
"""
import numpy as np
import os
import pandas as pd
import shutil
import unittest
from web_functions import get_warc, verify_warc_fixity


class MyTestCase(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folder and its contents, if any, produced by the tests.
        """
        for seed_folder in ("correct-aip", "error-fixity-aip", "error-regex-aip"):
            if os.path.exists(seed_folder):
                shutil.rmtree(os.path.join(os.getcwd(), seed_folder))

    def test_correct(self):
        """
        Tests that the function leaves the WARC in the seed folder and updates the log correctly
        when the WARC fixity matches Archive-It.
        """
        # Makes the data needed for the function input and runs the function.
        warc = "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2444051-20221203041251087-00001-h3.warc.gz"
        warc_path = os.path.join(os.getcwd(), "correct-aip", warc)
        seed_df = pd.DataFrame([[2444051, "correct-aip", "Title", "Russell", "0000", 12265, "1718490", 1,
                                 warc, "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 0.01]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames",
                                        "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                        "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        os.mkdir("correct-aip")
        get_warc(seed_df, 0,  f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        verify_warc_fixity(seed_df, 0, warc_path, warc, "7f0c9f11a27b06271b4137d99946fc52")

        # Test the WARC was not deleted.
        warc_downloaded = os.path.exists(os.path.join(os.getcwd(), "correct-aip", warc))
        self.assertEqual(warc_downloaded, True, "Problem with test for correct, WARC deletion")

        # Test the log is updated correctly.
        # Just tests for what it starts with, since the end of the log is the time stamp of the fixity check.
        actual = seed_df["WARC_Fixity_Errors"][0].startswith(f"Successfully verified {warc} fixity on ")
        self.assertEqual(actual, True, "Problem with test for correct, log")

    def test_error_fixity(self):
        """
        Tests that the function deletes the WARC in the seed folder and updates the log correctly
        when the WARC fixity does not match Archive-It.
        """
        # Makes the data needed for the function input and runs the function.
        warc = "ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz"
        warc_path = os.path.join(os.getcwd(), "error-fixity-aip", warc)
        seed_df = pd.DataFrame([[2173769, "error-fixity-aip", "Title", "Hargrett", "0000", 12912, "1115532", 1,
                                 warc, "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 0.01]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames",
                                        "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                        "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        os.mkdir("error-fixity-aip")
        get_warc(seed_df, 0,  f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        with self.assertRaises(ValueError):
            verify_warc_fixity(seed_df, 0, warc_path, warc, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

        # Test the WARC was deleted.
        warc_downloaded = os.path.exists(os.path.join(os.getcwd(), "error-fixity-aip", warc))
        self.assertEqual(warc_downloaded, False, "Problem with test for correct, WARC deletion")

        # Test the log is updated correctly.
        actual = seed_df["WARC_Fixity_Errors"][0]
        expected = f"Fixity for {warc} changed and it was deleted: " \
                   f"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx before, 422c2c674cac30a015120483c2fa25cd after"
        self.assertEqual(actual, expected, "Problem with test for correct, log")

    def test_error_regex(self):
        """
        Tests that the function does not delete the WARC in the seed folder and updates the log correctly
        when the WARC fixity cannot be does not match Archive-It.
        """
        # Makes the data needed for the function input and runs the function.
        warc = "ARCHIVEIT-12265-TEST-JOB1365541-SEED2454528-20210217005857702-00002-h3.warc.gz"
        warc_path = os.path.join(os.getcwd(), "error-regex-aip", warc)
        seed_df = pd.DataFrame([[2454528, "error-regex-aip", "Title", "Russell", "0000", 12265, "1365541", 1,
                                 warc, "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 0.01]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames",
                                        "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                        "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        os.mkdir("error-regex-aip")
        get_warc(seed_df, 0,  f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        with self.assertRaises(AttributeError):
            verify_warc_fixity(seed_df, 0, os.path.join(os.getcwd(), "error.warc.gz"), warc,
                               "18080e6f3c82ad095d15be8c5ab6ca21")

        # Test the WARC was not deleted.
        warc_downloaded = os.path.exists(os.path.join(os.getcwd(), "error-regex-aip", warc))
        self.assertEqual(warc_downloaded, True, "Problem with test for correct, WARC deletion")

        # Test the log is updated correctly.
        actual = seed_df["WARC_Fixity_Errors"][0]
        expected = f"Fixity for {warc} cannot be extracted from md5deep output: b''"
        self.assertEqual(actual, expected, "Problem with test for correct, log")


if __name__ == '__main__':
    unittest.main()
