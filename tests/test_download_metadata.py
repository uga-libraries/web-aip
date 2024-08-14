"""
Tests for the get_report function.
It downloads six types of reports from the Partner API and updates the log.

The tests are just confirming that the reports were downloaded.
Tests for other functions confirm the contents of the reports.
The AIP IDs are abbreviations of the real AIP ID naming conventions.
"""
import numpy as np
import os
import pandas as pd
import shutil
import unittest
import configuration as config
from web_functions import download_metadata


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


class TestDownloadMetadata(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folders and any contents from the tests and the seeds_log.csv.
        """
        for seed_folder in ("2187482", "2529685", "2547528"):
            if os.path.exists(seed_folder):
                shutil.rmtree(os.path.join(os.getcwd(), seed_folder))
        os.remove(os.path.join(config.script_output, "seeds_log.csv"))

    def test_hargrett(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a Hargrett seed with one each of all six of the report types.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = make_df(["harg-1", 2187482, 12181, "1177700", 3.62, 3, "name0.warc.gz|name1.warc.gz|name2.warc.gz",
                           np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2187482")
        download_metadata(seed, 0, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("2187482"):
            actual.append(filename)
        actual.sort()
        expected = ["harg-1_1177700_crawljob.csv",
                    "harg-1_31104333391_crawldef.csv",
                    "harg-1_coll.csv",
                    "harg-1_collscope.csv",
                    "harg-1_seed.csv",
                    "harg-1_seedscope.csv"]
        self.assertEqual(actual, expected, "Problem with test for Hargrett, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df.at[0, 'Metadata_Report_Errors']
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for Hargrett, log errors")

        # Test that the log has the correct information for empty reports.
        actual_info = seed_df.at[0, 'Metadata_Report_Empty']
        expected_info = "No empty reports"
        self.assertEqual(actual_info, expected_info, "Problem with test for Hargrett, log info")

    def test_magil(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a MAGIL seed with one each of the four report types which always have data.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = make_df(["magil-1", 2529685, 15678, "1594228", 0.36, 1, "name.warc.gz",
                           np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2529685")
        download_metadata(seed, 0, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("2529685"):
            actual.append(filename)
        actual.sort()
        expected = ["magil-1_1594228_crawljob.csv",
                    "magil-1_31104546937_crawldef.csv",
                    "magil-1_coll.csv",
                    "magil-1_seed.csv"]
        self.assertEqual(actual, expected, "Problem with test for MAGIL, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df.at[0, 'Metadata_Report_Errors']
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for MAGIL, log errors")

        # Test that the log has the correct information for empty reports.
        actual_info = seed_df.at[0, 'Metadata_Report_Empty']
        expected_info = "magil-1_seedscope.csv; magil-1_collscope.csv"
        self.assertEqual(actual_info, expected_info, "Problem with test for MAGIL, log info")

    def test_russell(self):
        """
        Tests that the function downloads the correct reports, and correctly updates the log,
        for a Russell two each of the crawl job and definition reports and one each of the other four report types.
        """
        # Makes the data needed for the function input and runs the function.
        seed_df = make_df(["rbrl-1", 2547528, 12265, "1436714|1718490", 0.72, 3,
                           "name0.warc.gz|name1.warc.gz|name2.warc.gz",
                           np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2547528")
        download_metadata(seed, 0, seed_df)

        # Test that the correct metadata reports were downloaded.
        actual = []
        for filename in os.listdir("2547528"):
            actual.append(filename)
        actual.sort()
        expected = ["rbrl-1_1436714_crawljob.csv",
                    "rbrl-1_1718490_crawljob.csv",
                    "rbrl-1_31104392189_crawldef.csv",
                    "rbrl-1_31104463393_crawldef.csv",
                    "rbrl-1_coll.csv",
                    "rbrl-1_collscope.csv",
                    "rbrl-1_seed.csv",
                    "rbrl-1_seedscope.csv"]
        self.assertEqual(actual, expected, "Problem with test for RussellL, downloaded files")

        # Test that the log has the correct information for metadata errors.
        actual_errors = seed_df.at[0, 'Metadata_Report_Errors']
        expected_errors = "Successfully downloaded all metadata reports"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for Russell, log errors")

        # Test that the log has the correct information for empty reports.
        actual_info = seed_df.at[0, 'Metadata_Report_Empty']
        expected_info = "No empty reports"
        self.assertEqual(actual_info, expected_info, "Problem with test for Russell, log info")


if __name__ == '__main__':
    unittest.main()
