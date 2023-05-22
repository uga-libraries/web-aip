"""
Tests for the unzip_error() function.
It unzips the download WARC and either deletes the zip (if it worked) or the unzipped file (if there was an error).

To save time, fake data is supplied in seed_df for fields that are not used in these tests
and seed_df only has the WARC being tested, not other WARCs for that seed.
"""
import numpy as np
import os
import pandas as pd
import shutil
import unittest
import configuration as c
from web_functions import get_warc, unzip_warc


class TestUnzipWarc(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the script output directory and contents, if any, and seeds_log.csv produced by the tests.
        The directory is changed first because seed_dir can't be deleted while it is the current working directory.
        """
        os.chdir("..")
        shutil.rmtree(os.path.join(c.script_output, "preservation_download"))
        os.remove(os.path.join(c.script_output, "seeds_log.csv"))

    def test_correct(self):
        """
        Tests that the function deletes the zip and updates the log correctly
        when the WARC is able to be unzipped.
        """
        # Makes the data needed for the function input and runs the function.
        seed_dir = os.path.join(c.script_output, "preservation_download")
        os.makedirs(os.path.join(seed_dir, "2173769"))
        os.chdir(seed_dir)
        warc = "ARCHIVEIT-12912-WEEKLY-JOB1215043-SEED2173769-20200625025209518-00000-h3.warc.gz"
        warc_path = os.path.join(seed_dir, "2173769", warc)
        seed_df = pd.DataFrame([["harg-1", 2173769, 12912, "1215043", 0.01, 1, warc,
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        get_warc(seed_df, 0, f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        unzip_warc(seed_df, 0, warc_path, warc, "2173769")

        # Test the zipped WARC was deleted.
        warc_zip = os.path.exists(warc_path)
        self.assertEqual(warc_zip, False, "Problem with test for correct, zipped WARC")

        # Test the unzipped WARC was made.
        # It is the same path and filename as warc_path except for without the last 3 characters (.gz)
        warc_unzip = os.path.exists(warc_path[:-3])
        self.assertEqual(warc_unzip, True, "Problem with test for correct, unzipped WARC")

        # Test the log is updated correctly.
        actual = seed_df.at[0, "WARC_Unzip_Errors"]
        expected = f"Successfully unzipped {warc}"
        self.assertEqual(actual, expected, "Problem with test for correct, log")

    def test_error_7zip(self):
        """
        Tests that the function updates the log correctly when the WARC cannot be unzipped due to an error from 7-Zip.
        The error is caused by not downloading the AIP to be unzipped.
        """
        # Makes the data needed for the function input and runs the function.
        seed_dir = os.path.join(c.script_output, "preservation_download")
        os.makedirs(os.path.join(seed_dir, "0000000"))
        os.chdir(seed_dir)
        warc = "ARCHIVEIT-ERROR.warc.gz"
        warc_path = os.path.join(seed_dir, "0000000", warc)
        seed_df = pd.DataFrame([["aip-0", 0000000, 00000, "0000000", 0.01, 1, warc,
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        unzip_warc(seed_df, 0, warc_path, warc, "0000000")

        # Test the log is updated correctly.
        actual = seed_df.at[0, "WARC_Unzip_Errors"]
        expected = f"Error unzipping {warc}: \r\nERROR: The system cannot find the file specified.\r\n{warc_path}" \
                   f"\r\n\r\n\r\n\r\nSystem ERROR:\r\nThe system cannot find the file specified.\r\n"
        self.assertEqual(actual, expected, "Problem with test for error: 7-Zip, log")

    def test_error_open(self):
        """
        Tests that the function does not delete the zip and updates the log correctly
        when the WARC does not unzip correctly due to the Windows bug with gzip.
        """
        # Makes the data needed for the function input and runs the function.
        seed_dir = os.path.join(c.script_output, "preservation_download")
        os.makedirs(os.path.join(seed_dir, "2912235"))
        os.chdir(seed_dir)
        warc = "ARCHIVEIT-12263-TEST-JOB1695540-0-SEED2912235-20221021155449526-00000-upoygm6a.warc.gz"
        warc_path = os.path.join(seed_dir, "2912235", warc)
        seed_df = pd.DataFrame([["rbrl-1", 2912235, 12263, "1695540", 0.01, 1, warc,
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        get_warc(seed_df, 0, f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        unzip_warc(seed_df, 0, warc_path, warc, "2912235")

        # Test the zipped WARC was not deleted.
        warc_zip = os.path.exists(warc_path)
        self.assertEqual(warc_zip, True, "Problem with test for error: unzips to open, zipped WARC")

        # Test the unzipped WARC was deleted.
        warc_unzip = os.path.exists(os.path.join(seed_dir, "2912235", f"{warc}.open"))
        self.assertEqual(warc_unzip, False, "Problem with test for error: unzips to open, unzipped WARC")

        # Test the log is updated correctly.
        actual = seed_df.at[0, "WARC_Unzip_Errors"]
        expected = f"Error unzipping {warc}: unzipped to '.gz.open' file"
        self.assertEqual(actual, expected, "Problem with test for error: unzips to open, log")


if __name__ == '__main__':
    unittest.main()
