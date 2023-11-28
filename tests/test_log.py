"""
Tests for the log() function.
It updates and returns a dataframe with the log message and also saves the data to a spreadsheet.
"""
import os
import numpy as np
import pandas as pd
import unittest
import configuration as config
from web_functions import log


def csv_to_list(csv_path):
    """
    Reads a CSV into pandas and converts it to a list,
    with the header and each data row as a list within that list.
    Cells with no value are convert to empty strings for easier comparison.
    """
    df = pd.read_csv(csv_path)
    df.fillna("", inplace=True)
    row_list = [df.columns.tolist()] + df.values.tolist()
    return row_list


def df_to_list(df):
    """
    Converts a dataframe into a list, with the header and each data row as a list within that list.
    Cells with no value are convert to empty strings for easier comparison.
    """
    df.fillna("", inplace=True)
    row_list = [df.columns.tolist()] + df.values.tolist()
    return row_list


class TestLog(unittest.TestCase):

    def setUp(self):
        """
        Makes a dataframe and a CSV to use as the starting point for each test.
        """
        row_list = [[1111111, 12345, "1100000", 0.52, 1, "ARCHIVEIT.warc.gz",
                     "Successfully downloaded all metadata reports", "No empty reports",
                     np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],
                    [2222222, 12345, "2200000", 1.52, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                     "Successfully downloaded all metadata reports", "2222222_seedscope.csv",
                     np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]]
        columns_list = ["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                        "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"]
        self.seed_df = pd.DataFrame(row_list, columns=columns_list)
        self.seed_df.to_csv(os.path.join(config.script_output, "seeds_log.csv"), index=False)

    def tearDown(self):
        """
        Deletes the spreadsheet created by each test.
        """
        os.remove(os.path.join(config.script_output, "seeds_log.csv"))

    def test_first_message(self):
        """
        Tests that the function returns the correct dataframe and correctly updates the CSV
        when there is not already a message of this type in the dataframe.
        """
        log("Successfully downloaded ARCHIVEIT.warc.gz", self.seed_df, 0, "WARC_Download_Errors")
        
        # Test that the dataframe has the correct values.
        actual_df = df_to_list(self.seed_df)
        expected_df = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                        "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                       [1111111, 12345, "1100000", 0.52, 1, "ARCHIVEIT.warc.gz", 
                        "Successfully downloaded all metadata reports", "No empty reports", "",
                        "Successfully downloaded ARCHIVEIT.warc.gz", "", "", ""],
                       [2222222, 12345, "2200000", 1.52, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                        "Successfully downloaded all metadata reports", "2222222_seedscope.csv", "", "", "", "", ""]]
        self.assertEqual(actual_df, expected_df, "Problem with test for first message, dataframe values")

        # Test that the CSV has the correct values.
        actual_csv = csv_to_list(os.path.join(config.script_output, "seeds_log.csv"))
        expected_csv = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                         "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                        [1111111, 12345, 1100000, 0.52, 1, "ARCHIVEIT.warc.gz",
                         "Successfully downloaded all metadata reports", "No empty reports",
                         "", "Successfully downloaded ARCHIVEIT.warc.gz", "", "", ""],
                        [2222222, 12345, 2200000, 1.52, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                         "Successfully downloaded all metadata reports", "2222222_seedscope.csv", "", "", "", "", ""]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for first message, CSV values")

    def test_second_message(self):
        """
        Tests that the function returns the correct dataframe and correctly updates the CSV
        when there is already a message of this type in the dataframe.
        """
        log("2222222_collscope.csv", self.seed_df, 1, "Metadata_Report_Empty")

        # Test that the dataframe has the correct values.
        actual_df = df_to_list(self.seed_df)
        expected_df = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                        "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                       [1111111, 12345, "1100000", 0.52, 1, "ARCHIVEIT.warc.gz",
                        "Successfully downloaded all metadata reports", "No empty reports", "", "", "", "", ""],
                       [2222222, 12345, "2200000", 1.52, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                        "Successfully downloaded all metadata reports", "2222222_seedscope.csv; 2222222_collscope.csv",
                        "", "", "", "", ""]]
        self.assertEqual(actual_df, expected_df, "Problem with test for second message, dataframe values")

        # Test that the CSV has the correct values.
        actual_csv = csv_to_list(os.path.join(config.script_output, "seeds_log.csv"))
        expected_csv = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                         "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                        [1111111, 12345, 1100000, 0.52, 1, "ARCHIVEIT.warc.gz",
                         "Successfully downloaded all metadata reports", "No empty reports", "", "", "", "", ""],
                        [2222222, 12345, 2200000, 1.52, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                         "Successfully downloaded all metadata reports",
                         "2222222_seedscope.csv; 2222222_collscope.csv", "", "", "", "", ""]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for second message, CSV values")


if __name__ == '__main__':
    unittest.main()
