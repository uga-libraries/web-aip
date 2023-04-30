"""
Test for download_warcs() function.
It downloads every WARC for a seed, verifies its fixity, and unzips it.

To save time, fake data is supplied in seed_df for fields that are not used in these tests
and seed_df only has the WARC(s) being tested, not other WARCs for that seed.
"""
import numpy as np
import os
import pandas as pd
import shutil
import unittest
import configuration as c
from web_functions import download_warcs


class MyTestCase(unittest.TestCase):

    def setUp(self):
        """
        Makes the seed dataframe and AIP folder that is used for every test,
        and makes the AIP folder the current working directory so that the unzipped WARC saves to the right place.
        """
        columns = ["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                   "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                   "WARC_Fixity_Errors", "WARC_Unzip_Errors"]
        error = [2018086, 12264, "921631", 0.01, 2,
                 "error.warc.gz;ARCHIVEIT-12264-TEST-JOB921631-SEED2018086-20190607140142542-00000-h3.warc.gz",
                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]
        harg = [2173769, 12912, "1415330", 0.01, 1,
                "ARCHIVEIT-12912-WEEKLY-JOB1415330-SEED2173769-20210519233828683-00001-h3.warc.gz",
                np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]
        rbrl = [2485678, 12265, "718490", 0.02, 2,
                "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2485678-20221203180441653-00001-h3.warc.gz;"
                "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2485678-20221202160754903-00000-h3.warc.gz",
                np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]
        self.seed_df = pd.DataFrame([error, harg, rbrl], columns=columns)

        self.aip_dir = os.path.join(c.script_output, "aips_2000-01")
        os.mkdir(self.aip_dir)
        os.chdir(self.aip_dir)

    def tearDown(self):
        """
        Deletes the script output directory and contents, if any, and seeds.csv produced by the tests.
        The directory is changed first because aip_dir can't be deleted while it is the current working directory.
        """
        os.chdir("..")
        shutil.rmtree(self.aip_dir)
        os.remove(os.path.join(c.script_output, "seeds.csv"))

    def test_error_handling(self):
        """
        Tests that the function can continue to download and unzip other WARCs after a WARC has an anticipated error.
        """
        # Makes the seed folder in the output directory and the seed object from the df and runs the function.
        seed_path = os.path.join(self.aip_dir, "2018086")
        os.makedirs(seed_path)
        seed = [seed for seed in self.seed_df.itertuples()][0]
        download_warcs(seed, "2000-01", self.seed_df)

        # Test for the WARC download.
        warc = "ARCHIVEIT-12264-TEST-JOB921631-SEED2018086-20190607140142542-00000-h3.warc"
        downloaded = (os.path.exists(os.path.join(seed_path, "error.warc.gz")),
                      os.path.exists(os.path.join(seed_path, warc)))
        self.assertEqual(downloaded, (False, True), "Problem with test for error handling, WARC download")

        # Test for the three log fields related to WARC downloading.
        # WARC_Fixity_Errors includes a time stamp, so can only test for what it starts with.
        actual_log = (self.seed_df.at[0, "WARC_API_Errors"],
                      self.seed_df.at[0, "WARC_Fixity_Errors"].startswith(f"Successfully verified {warc}.gz fixity"),
                      self.seed_df.at[0, "WARC_Unzip_Errors"])
        expected_log = (f"Index Error: cannot get the WARC URL or MD5 for error.warc.gz; "
                        f"Successfully downloaded {warc}.gz", True, f"Successfully unzipped {warc}.gz")
        self.assertEqual(actual_log, expected_log, "Problem with test for error handling, log")

    def test_one_warc(self):
        """
        Tests that the function downloads and unzips the expected WARC, and correctly updates the log,
        for as seed with one WARC.
        """
        # Makes the seed folder in the output directory and the seed object from the df and runs the function.
        seed_path = os.path.join(self.aip_dir, "2173769")
        os.makedirs(seed_path)
        seed = [seed for seed in self.seed_df.itertuples()][1]
        download_warcs(seed, "2000-01", self.seed_df)

        # Test for the WARC download.
        warc = "ARCHIVEIT-12912-WEEKLY-JOB1415330-SEED2173769-20210519233828683-00001-h3.warc"
        downloaded = os.path.exists(os.path.join(seed_path, warc))
        self.assertEqual(downloaded, True, "Problem with test for seed with one WARC, WARC download")

        # Test for the three log fields related to WARC downloading.
        # WARC_Fixity_Errors includes a time stamp, so can only test for what it starts with.
        actual_log = (self.seed_df.at[1, "WARC_API_Errors"],
                      self.seed_df.at[1, "WARC_Fixity_Errors"].startswith(f"Successfully verified {warc}.gz fixity"),
                      self.seed_df.at[1, "WARC_Unzip_Errors"])
        expected_log = (f"Successfully downloaded {warc}.gz", True, f"Successfully unzipped {warc}.gz")
        self.assertEqual(actual_log, expected_log, "Problem with test for seed with one WARC, log")

    def test_two_warcs(self):
        """
        Tests that the function downloads and unzips the expected WARCs, and correctly updates the log,
        for as seed with two WARCs.
        """
        # Makes the seed folder in the output directory and the seed object from the df and runs the function.
        seed_path = os.path.join(self.aip_dir, "2485678")
        os.makedirs(seed_path)
        seed = [seed for seed in self.seed_df.itertuples()][2]
        download_warcs(seed, "2000-01", self.seed_df)

        # Test for the WARCs download.
        warc1 = "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2485678-20221203180441653-00001-h3.warc"
        warc2 = "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2485678-20221202160754903-00000-h3.warc"
        first = os.path.exists(os.path.join(seed_path, warc1))
        second = os.path.exists(os.path.join(seed_path, warc2))
        downloaded = (first, second)
        self.assertEqual(downloaded, (True, True), "Problem with test for seed with twp WARCs, WARC download")

        # Test for the three log fields related to WARC downloading.
        # WARC_Fixity_Errors includes a time stamp, so can only test for what it starts with.
        actual_log = (self.seed_df.at[2, "WARC_API_Errors"],
                      self.seed_df.at[2, "WARC_Fixity_Errors"].startswith(f"Successfully verified {warc1}.gz fixity"),
                      self.seed_df.at[2, "WARC_Unzip_Errors"])
        expected_log = (f"Successfully downloaded {warc1}.gz; Successfully downloaded {warc2}.gz", True,
                        f"Successfully unzipped {warc1}.gz; Successfully unzipped {warc2}.gz")
        self.assertEqual(actual_log, expected_log, "Problem with test for seed with one WARC, log")


if __name__ == '__main__':
    unittest.main()
