"""
Tests for the unzip_error() function.
It unzips the download WARC and either deletes the zip (if it worked) or the unzipped file (if there was an error).

To save time, fake data is supplied in seed_df for fields that are not used in these tests
and seed_df only has the WARC being tested, not other WARCs for that seed.
"""
import os
import pandas as pd
import shutil
import unittest
import configuration as config
from web_functions import get_warc, unzip_warc


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


class TestUnzipWarc(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the script output directory and contents, if any, and seeds_log.csv produced by the tests.
        The directory is changed first because seed_dir can't be deleted while it is the current working directory.
        """
        os.chdir(config.script_output)
        shutil.rmtree(os.path.join(config.script_output, "preservation_download"))
        os.remove(os.path.join(config.script_output, "seeds_log.csv"))

    def test_correct(self):
        """
        Tests that the function deletes the zip and updates the log correctly
        when the WARC is able to be unzipped.
        """
        # Makes the data needed for the function input and runs the function.
        seed_dir = os.path.join(config.script_output, "preservation_download")
        os.makedirs(os.path.join(seed_dir, "2173769"))
        os.chdir(seed_dir)
        warc = "ARCHIVEIT-12912-WEEKLY-JOB1215043-SEED2173769-20200625025209518-00000-h3.warc.gz"
        warc_path = os.path.join(seed_dir, "2173769", warc)
        seed_df = make_df(["harg-1", 2173769, 12912, "1215043", 0.01, 1, warc,
                           "TBD", "TBD", "TBD", "TBD", "TBD", "TBD", "TBD"])
        get_warc(seed_df, 0, f"https://warcs.archive-it.org/webdatafile/{warc}", warc, warc_path)
        unzip_warc(seed_df, 0, warc_path, warc)

        # Test the zipped WARC was deleted.
        warc_zip = os.path.exists(warc_path)
        self.assertEqual(warc_zip, False, "Problem with test for correct, zipped WARC")

        # Test the unzipped WARC was made.
        # It is the same path and filename as warc_path except for without the last 3 characters (.gz)
        warc_unzip = os.path.exists(warc_path[:-3])
        self.assertEqual(warc_unzip, True, "Problem with test for correct, unzipped WARC")

        # Test the log is updated correctly.
        actual = seed_df.at[0, 'WARC_Unzip_Errors']
        expected = f"Successfully unzipped {warc}"
        self.assertEqual(actual, expected, "Problem with test for correct, log")

    def test_error(self):
        """
        Tests that the function updates the log correctly when the WARC cannot be unzipped.
        The error is caused by not downloading the AIP to be unzipped.
        """
        # Makes the data needed for the function input and runs the function.
        seed_dir = os.path.join(config.script_output, "preservation_download")
        os.makedirs(os.path.join(seed_dir, "0000000"))
        os.chdir(seed_dir)
        warc = "ARCHIVEIT-ERROR.warc.gz"
        warc_path = os.path.join(seed_dir, "0000000", warc)
        seed_df = make_df(["aip-0", 0000000, 00000, "0000000", 0.01, 1, warc,
                           "TBD", "TBD", "TBD", "TBD", "TBD", "TBD", "TBD"])
        unzip_warc(seed_df, 0, warc_path, warc)

        # Test the log is updated correctly.
        actual = seed_df.at[0, 'WARC_Unzip_Errors']
        expected = f"Error unzipping {warc}: gzip: {warc_path}: No such file or directory\n"
        self.assertEqual(actual, expected, "Problem with test for error, log")


if __name__ == '__main__':
    unittest.main()
