"""
Tests for the verify_warc_fixity() function.
It compares the fixity of the downloaded WARC to Archive-It and deletes the file if it does not match.

To save time, fake data is supplied in seed_df for fields that are not used in these tests
and seed_df only has the WARC being tested, not other WARCs for that seed.
"""
import os
import pandas as pd
import shutil
import unittest
import configuration as config
from web_functions import get_warc, verify_warc_fixity


def make_df(df_row):
    """
    Makes a dataframe with the provided row information. The column values are the same for all tests.
    Returns the dataframe.
    """
    column_list = ["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                   "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                   "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"]
    df = pd.DataFrame([df_row], columns=column_list)
    return df


class TestVerifyWarcFixity(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folder and its contents, if any, and the seeds_log.csv produced by the tests.
        """
        for seed_folder in ("2173769", "2444051", "2454528"):
            if os.path.exists(seed_folder):
                shutil.rmtree(os.path.join(os.getcwd(), seed_folder))
        os.remove(os.path.join(config.script_output, "seeds_log.csv"))

    def test_correct(self):
        """
        Tests that the function leaves the WARC in the seed folder and updates the log correctly
        when the WARC fixity matches Archive-It.
        """
        # Makes the data needed for the function input and runs the function.
        warc = "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2444051-20221203041251087-00001-h3.warc.gz"
        warc_path = os.path.join(os.getcwd(), "2444051", warc)
        seed_df = make_df(["rbrl-1", 2444051, 12265, "1718490", 0.01, 1, warc,
                           "TBD", "TBD", "TBD", "TBD", "TBD", "TBD", "TBD"])
        os.mkdir("2444051")
        get_warc(seed_df, 0,  f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        verify_warc_fixity(seed_df, 0, warc_path, warc, "7f0c9f11a27b06271b4137d99946fc52")

        # Test the WARC was not deleted.
        warc_downloaded = os.path.exists(os.path.join(os.getcwd(), "2444051", warc))
        self.assertEqual(warc_downloaded, True, "Problem with test for correct, WARC deletion")

        # Test the log is updated correctly.
        # Just tests for what it starts with, since the end of the log is the time stamp of the fixity check.
        actual = seed_df.at[0, 'WARC_Fixity_Errors'].startswith(f"Successfully verified {warc} fixity on ")
        self.assertEqual(actual, True, "Problem with test for correct, log")

    def test_error_fixity(self):
        """
        Tests that the function deletes the WARC in the seed folder and updates the log correctly
        when the WARC fixity does not match Archive-It.
        """
        # Makes the data needed for the function input and runs the function.
        warc = "ARCHIVEIT-12912-TEST-JOB1115532-SEED2173769-20200326213812038-00000-h3.warc.gz"
        warc_path = os.path.join(os.getcwd(), "2173769", warc)
        seed_df = make_df(["harg-1", 2173769, 12912, "1115532", 0.01, 1, warc,
                           "TBD", "TBD", "TBD", "TBD", "TBD", "TBD", "TBD"])
        os.mkdir("2173769")
        get_warc(seed_df, 0,  f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        with self.assertRaises(ValueError):
            verify_warc_fixity(seed_df, 0, warc_path, warc, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

        # Test the WARC was deleted.
        warc_downloaded = os.path.exists(os.path.join(os.getcwd(), "2173769", warc))
        self.assertEqual(warc_downloaded, False, "Problem with test for correct, WARC deletion")

        # Test the log is updated correctly.
        actual = seed_df.at[0, 'WARC_Fixity_Errors']
        expected = f"Error: fixity for {warc} changed and it was deleted: " \
                   f"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx before, 422c2c674cac30a015120483c2fa25cd after"
        self.assertEqual(actual, expected, "Problem with test for correct, log")

    def test_error_regex(self):
        """
        Tests that the function does not delete the WARC in the seed folder and updates the log correctly
        when the WARC fixity cannot be extracted from the md5deep output.
        """
        # Makes the data needed for the function input and runs the function.
        warc = "ARCHIVEIT-12265-TEST-JOB1365541-SEED2454528-20210217005857702-00002-h3.warc.gz"
        warc_path = os.path.join(os.getcwd(), "2454528", warc)
        seed_df = make_df(["rbrl-1", 2454528, 12265, "1365541", 0.01, 1, warc,
                           "TBD", "TBD", "TBD", "TBD", "TBD", "TBD", "TBD"])
        os.mkdir("2454528")
        get_warc(seed_df, 0,  f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        with self.assertRaises(AttributeError):
            verify_warc_fixity(seed_df, 0, os.path.join(os.getcwd(), "error.warc.gz"), warc,
                               "18080e6f3c82ad095d15be8c5ab6ca21")

        # Test the WARC was not deleted.
        warc_downloaded = os.path.exists(os.path.join(os.getcwd(), "2454528", warc))
        self.assertEqual(warc_downloaded, True, "Problem with test for correct, WARC deletion")

        # Test the log is updated correctly.
        actual = seed_df.at[0, 'WARC_Fixity_Errors']
        expected = f"Error: fixity for {warc} cannot be extracted from md5deep output: b''"
        self.assertEqual(actual, expected, "Problem with test for correct, log")


if __name__ == '__main__':
    unittest.main()
