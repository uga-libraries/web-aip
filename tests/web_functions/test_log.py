"""
Tests for the log() function.
It updates and returns a dataframe with the log message and also saves the data to a spreadsheet.
"""
import csv
import os
import numpy as np
import pandas as pd
import unittest
import configuration as c
from web_functions import log


class MyTestCase(unittest.TestCase):

    def setUp(self):
        """
        Makes a dataframe and a CSV to use as the starting point for each test.
        """
        self.seed_df = pd.DataFrame([[1111111, 12345, "1100000", 0.52, 1,
                                      "ARCHIVEIT.warc.gz", "Successfully downloaded all metadata reports",
                                      "No empty reports", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],
                                     [2222222, 12345, "2200000", 1.52, 2,
                                      "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                                      "Successfully downloaded all metadata reports", "2222222_seedscope.csv",
                                      np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]],
                                    columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                             "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                             "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                             "WARC_Unzip_Errors", "Complete"])
        self.seed_df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)

    def tearDown(self):
        """
        Deletes the spreadsheet created by each test.
        """
        os.remove(os.path.join(c.script_output, "seeds_log.csv"))

    def test_first_message(self):
        """
        Tests that the function returns the correct dataframe and correctly updates the CSV
        when there is not already a message of this type in the dataframe.
        """
        log("Successfully downloaded ARCHIVEIT.warc.gz", self.seed_df, 0, "WARC_API_Errors")
        self.seed_df = self.seed_df.fillna("BLANK")

        # Test that the dataframe has the correct values.
        actual_dataframe = [self.seed_df.columns.tolist()] + self.seed_df.values.tolist()
        expected_dataframe = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                               "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                               "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                              [1111111, 12345, "1100000", 0.52, 1, "ARCHIVEIT.warc.gz",
                               "Successfully downloaded all metadata reports",
                               "No empty reports", "BLANK", "Successfully downloaded ARCHIVEIT.warc.gz",
                               "BLANK", "BLANK", "BLANK"],
                              [2222222, 12345, "2200000", 1.52, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                               "Successfully downloaded all metadata reports", "2222222_seedscope.csv",
                               "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]]
        self.assertEqual(actual_dataframe, expected_dataframe, "Problem with test for first message, dataframe values")

        # Test that the CSV has the correct values.
        csv_path = os.path.join(c.script_output, "seeds_log.csv")
        with open(csv_path, newline="") as open_file:
            reader = csv.reader(open_file)
            actual_csv = list(reader)
        expected_csv = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                         "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                        ["1111111", "12345", "1100000", "0.52", "1", "ARCHIVEIT.warc.gz",
                         "Successfully downloaded all metadata reports", "No empty reports",
                         "", "Successfully downloaded ARCHIVEIT.warc.gz", "", "", ""],
                        ["2222222", "12345", "2200000", "1.52", "2", "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                         "Successfully downloaded all metadata reports", "2222222_seedscope.csv", "", "", "", "", ""]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for first message, CSV values")

    def test_second_message(self):
        """
        Tests that the function returns the correct dataframe and correctly updates the CSV
        when there is already a message of this type in the dataframe.
        """
        log("2222222_collscope.csv", self.seed_df, 1, "Metadata_Report_Empty")
        self.seed_df = self.seed_df.fillna("BLANK")

        # Test that the dataframe has the correct values.
        actual_dataframe = [self.seed_df.columns.tolist()] + self.seed_df.values.tolist()
        expected_dataframe = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                               "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                               "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                              [1111111, 12345, "1100000", 0.52, 1, "ARCHIVEIT.warc.gz",
                               "Successfully downloaded all metadata reports", "No empty reports",
                               "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                              [2222222, 12345, "2200000", 1.52, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                               "Successfully downloaded all metadata reports",
                               "2222222_seedscope.csv; 2222222_collscope.csv",
                               "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]]
        self.assertEqual(actual_dataframe, expected_dataframe, "Problem with test for second message, dataframe values")

        # Test that the CSV has the correct values.
        csv_path = os.path.join(c.script_output, "seeds_log.csv")
        with open(csv_path, newline="") as open_file:
            reader = csv.reader(open_file)
            actual_csv = list(reader)
        expected_csv = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                         "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                        ["1111111", "12345", "1100000", "0.52", "1", "ARCHIVEIT.warc.gz",
                         "Successfully downloaded all metadata reports", "No empty reports", "", "", "", "", ""],
                        ["2222222", "12345", "2200000", "1.52", "2", "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                         "Successfully downloaded all metadata reports",
                         "2222222_seedscope.csv; 2222222_collscope.csv", "", "", "", "", ""]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for second message, CSV values")


if __name__ == '__main__':
    unittest.main()
