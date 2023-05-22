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


class TestDownloadWarcs(unittest.TestCase):

    def setUp(self):
        """
        Makes the seed dataframe and AIP folder that is used for every test,
        and makes the AIP folder the current working directory so that the unzipped WARC saves to the right place.
        """
        columns = ["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                   "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                   "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"]
        error = ["russell-1", 2018086, 12264, "921631", 0.01, 2,
                 "error.warc.gz|ARCHIVEIT-12264-TEST-JOB921631-SEED2018086-20190607140142542-00000-h3.warc.gz",
                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]
        harg = ["harg-1", 2173769, 12912, "1415330", 0.01, 1,
                "ARCHIVEIT-12912-WEEKLY-JOB1415330-SEED2173769-20210519233828683-00001-h3.warc.gz",
                np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]
        rbrl = ["rbrl-1", 2485678, 12265, "718490", 0.02, 2,
                "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2485678-20221203180441653-00001-h3.warc.gz|"
                "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2485678-20221202160754903-00000-h3.warc.gz",
                np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]
        self.seed_df = pd.DataFrame([error, harg, rbrl], columns=columns)

        self.seeds_dir = os.path.join(c.script_output, "preservation_download")
        os.mkdir(self.seeds_dir)
        os.chdir(self.seeds_dir)

    def tearDown(self):
        """
        Deletes the script output directory and contents, if any, and seeds_log.csv produced by the tests.
        The directory is changed first because seeds_dir can't be deleted while it is the current working directory.
        """
        os.chdir("..")
        shutil.rmtree(self.seeds_dir)
        os.remove(os.path.join(c.script_output, "seeds_log.csv"))

    def test_error_handling(self):
        """
        Tests that the function can continue to download and unzip other WARCs after a WARC has an anticipated error.
        """
        # Makes the seed folder in the output directory and the seed object from the df and runs the function.
        seed_path = os.path.join(self.seeds_dir, "2018086")
        os.makedirs(seed_path)
        seed = [seed for seed in self.seed_df.itertuples()][0]
        download_warcs(seed, 0, self.seed_df)

        # Test for the WARC download, error file not made.
        downloaded1 = os.path.exists(os.path.join(seed_path, "error.warc.gz"))
        self.assertEqual(downloaded1, False, "Problem with test for error handling, WARC download: error")

        # Test for the WARC download, correct WARC downloaded.
        warc = "ARCHIVEIT-12264-TEST-JOB921631-SEED2018086-20190607140142542-00000-h3.warc"
        downloaded2 = os.path.exists(os.path.join(seed_path, warc))
        self.assertEqual(downloaded2, True, "Problem with test for error handling, WARC download: correct")

        # Test for the log field WARC_API_Errors.
        actual_log1 = self.seed_df.at[0, "WARC_API_Errors"]
        expected_log1 = f"Index Error: cannot get the WARC URL or MD5 for error.warc.gz; " \
                        f"Successfully downloaded {warc}.gz"
        self.assertEqual(actual_log1, expected_log1, "Problem with test for error handling, log: WARC_API_Errors")

        # Test for the log field WARC_Fixity_Errors.
        # WARC_Fixity_Errors includes a time stamp, so the test cannot be for an exact match.
        actual_log2 = self.seed_df.at[0, "WARC_Fixity_Errors"]
        expected_log2 = f"Successfully verified {warc}.gz fixity"
        self.assertIn(expected_log2, actual_log2, "Problem with test for error handling, log: WARC_Fixity_Errors")

        # Test for the log field WARC_Unzip_Errors.
        actual_log3 = self.seed_df.at[0, "WARC_Unzip_Errors"]
        expected_log3 = f"Successfully unzipped {warc}.gz"
        self.assertEqual(actual_log3, expected_log3, "Problem with test for error handling, log: WARC_Unzip_Errors")

    def test_one_warc(self):
        """
        Tests that the function downloads and unzips the expected WARC, and correctly updates the log,
        for as seed with one WARC.
        """
        # Makes the seed folder in the output directory and the seed object from the df and runs the function.
        seed_path = os.path.join(self.seeds_dir, "2173769")
        os.makedirs(seed_path)
        seed = [seed for seed in self.seed_df.itertuples()][1]
        download_warcs(seed, 1, self.seed_df)

        # Test for the WARC download.
        warc = "ARCHIVEIT-12912-WEEKLY-JOB1415330-SEED2173769-20210519233828683-00001-h3.warc"
        downloaded = os.path.exists(os.path.join(seed_path, warc))
        self.assertEqual(downloaded, True, "Problem with test for seed with one WARC, WARC download")

        # Test for the log field WARC_API_Errors.
        actual_log1 = self.seed_df.at[1, "WARC_API_Errors"]
        expected_log1 = f"Successfully downloaded {warc}.gz"
        self.assertEqual(actual_log1, expected_log1,
                         "Problem with test for seed with one WARC, log: WARC_API_Errors")

        # Test for the log field WARC_Fixity_Errors.
        # WARC_Fixity_Errors includes a time stamp, so the test cannot be for an exact match.
        actual_log2 = self.seed_df.at[1, "WARC_Fixity_Errors"]
        expected_log2 = f"Successfully verified {warc}.gz fixity"
        self.assertIn(expected_log2, actual_log2,
                      "Problem with test for seed with one WARC, log: WARC_Fixity_Errors")

        # Test for the log field WARC_Unzip_Errors.
        actual_log3 = self.seed_df.at[1, "WARC_Unzip_Errors"]
        expected_log3 = f"Successfully unzipped {warc}.gz"
        self.assertEqual(actual_log3, expected_log3,
                         "Problem with test for seed with one WARC, log: WARC_Unzip_Errors")

    def test_two_warcs(self):
        """
        Tests that the function downloads and unzips the expected WARCs, and correctly updates the log,
        for as seed with two WARCs.
        """
        # Makes the seed folder in the output directory and the seed object from the df and runs the function.
        seed_path = os.path.join(self.seeds_dir, "2485678")
        os.makedirs(seed_path)
        seed = [seed for seed in self.seed_df.itertuples()][2]
        download_warcs(seed, 2, self.seed_df)

        # Test for the first WARC's download.
        warc1 = "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2485678-20221203180441653-00001-h3.warc"
        downloaded1 = os.path.exists(os.path.join(seed_path, warc1))
        self.assertEqual(downloaded1, True, "Problem with test for seed with two WARCs, first WARC download")

        # Test for the second WARC's download.
        warc2 = "ARCHIVEIT-12265-MONTHLY-JOB1718490-SEED2485678-20221202160754903-00000-h3.warc"
        downloaded2 = os.path.exists(os.path.join(seed_path, warc2))
        self.assertEqual(downloaded2, True, "Problem with test for seed with two WARCs, WARC download")

        # Test for the log field WARC_API_Errors.
        actual_log1 = self.seed_df.at[2, "WARC_API_Errors"]
        expected_log1 = f"Successfully downloaded {warc1}.gz; Successfully downloaded {warc2}.gz"
        self.assertEqual(actual_log1, expected_log1,
                         "Problem with test for seed with two WARCs, log: WARC_API_Errors")

        # Test for the log field WARC_Fixity_Errors.
        # WARC_Fixity_Errors includes a time stamp, so the test cannot be for an exact match.
        actual_log2 = self.seed_df.at[2, "WARC_Fixity_Errors"]
        expected_log2a = f"Successfully verified {warc1}.gz fixity"
        expected_log2b = f"Successfully verified {warc2}.gz fixity"
        self.assertIn(expected_log2a, actual_log2,
                      "Problem with test for seed with two WARCs, log: WARC_Fixity_Errors WARC 1")
        self.assertIn(expected_log2b, actual_log2,
                      "Problem with test for seed with two WARCs, log: WARC_Fixity_Errors WARC 2")

        # Test for the log field WARC_Unzip_Errors.
        actual_log3 = self.seed_df.at[2, "WARC_Unzip_Errors"]
        expected_log3 = f"Successfully unzipped {warc1}.gz; Successfully unzipped {warc2}.gz"
        self.assertEqual(actual_log3, expected_log3,
                         "Problem with test for seed with two WARCs, log: WARC_Unzip_Errors")


if __name__ == '__main__':
    unittest.main()
