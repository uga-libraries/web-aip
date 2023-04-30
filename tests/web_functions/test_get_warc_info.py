"""
Tests for get_warc_info() function.
It gets the WARC URL and MD5 from WASAPI.

To save time, fake data is supplied in seed_df for fields that are not used in these tests
and seed_df only has the WARC being tested, not other WARCs for that seed.
"""
import numpy as np
import pandas as pd
import unittest
from web_functions import get_warc_info


class MyTestCase(unittest.TestCase):

    def test_bma(self):
        """
        Tests that the function returns the expected values for a BMA WARC.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2028986, 12470, "1085452", 0.01, 1,
                                 "ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129213514425-00000-h3.warc.gz",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"])
        warc_url, warc_md5 = get_warc_info(seed_df["WARC_Filenames"][0], seed_df, 0)

        # Test for the URL.
        expected_url = f"https://warcs.archive-it.org/webdatafile/{seed_df['WARC_Filenames'][0]}"
        self.assertEqual(warc_url, expected_url, "Problem with test for BMA, URL")

        # Test for the MD5.
        expected_md5 = "60d789913d1f4dfb7e8c0c67a6a57505"
        self.assertEqual(warc_md5, expected_md5, "Problem with test for BMA, MD5")

    def test_hargrett(self):
        """
        Tests that the function returns the expected values for a Hargrett WARC.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2173769, 12912, "362980", 0.01, 1,
                                 "ARCHIVEIT-12912-WEEKLY-JOB1362980-SEED2173769-20210210221704177-00000-h3.warc.gz",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"])
        warc_url, warc_md5 = get_warc_info(seed_df["WARC_Filenames"][0], seed_df, 0)

        # Test for the URL.
        expected_url = f"https://warcs.archive-it.org/webdatafile/{seed_df['WARC_Filenames'][0]}"
        self.assertEqual(warc_url, expected_url, "Problem with test for Hargrett, URL")

        # Test for the MD5.
        expected_md5 = "2d4646eb04920325ba3d9538d56e93ff"
        self.assertEqual(warc_md5, expected_md5, "Problem with test for Hargrett, MD5")

    def test_magil(self):
        """
        Tests that the function returns the expected values for a MAGIL WARC.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2529646, 15678, "1585231", 0.01, 1,
                                 "ARCHIVEIT-15678-TEST-JOB1585231-SEED2529646-20220406065532448-00002-h3.warc.gz",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"])
        warc_url, warc_md5 = get_warc_info(seed_df["WARC_Filenames"][0], seed_df, 0)

        # Test for the URL.
        expected_url = f"https://warcs.archive-it.org/webdatafile/{seed_df['WARC_Filenames'][0]}"
        self.assertEqual(warc_url, expected_url, "Problem with test for MAGIL, URL")

        # Test for the MD5.
        expected_md5 = "220ca00b247a3110533f3810e458722f"
        self.assertEqual(warc_md5, expected_md5, "Problem with test for MAGIL, MD5")

    def test_russell(self):
        """
        Tests that the function returns the expected values for a Russell WARC.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = pd.DataFrame([[2027713, 12264, "943066", 0.01, 1,
                                 "ARCHIVEIT-12264-TEST-JOB943066-SEED2027713-20190709150720209-00000-h3.warc.gz",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                                        "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"])
        warc_url, warc_md5 = get_warc_info(seed_df["WARC_Filenames"][0], seed_df, 0)

        # Test for the URL.
        expected_url = f"https://warcs.archive-it.org/webdatafile/{seed_df['WARC_Filenames'][0]}"
        self.assertEqual(warc_url, expected_url, "Problem with test for Hargrett, URL")

        # Test for the MD5.
        expected_md5 = "941b25ea237edcf3a5dd5aea80e812eb"
        self.assertEqual(warc_md5, expected_md5, "Problem with test for Hargrett, MD5")


if __name__ == '__main__':
    unittest.main()
