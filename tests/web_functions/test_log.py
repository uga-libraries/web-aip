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
        self.seed_df = pd.DataFrame([["1111111", "aip1", "title1", "magil", "0000", 12345, "1111111", 1,
                                      "ARCHIVEIT.warc.gz", "Successfully calculated seed metadata",
                                      "Successfully downloaded all metadata reports", "No additional information",
                                      np.NaN, np.NaN, np.NaN, 0.52],
                                     ["2222222", "aip2", "title2", "magil", "0000", 12345, "2222222", 2,
                                      "ARCHIVEIT.warc.gz,ARCHIVEIT-1.warc.gz",
                                      "Successfully calculated seed metadata",
                                      "Successfully downloaded all metadata reports",
                                      "Empty report aip2_seedscope.csv not saved",
                                      np.NaN, np.NaN, np.NaN, 0.52]],
                                    columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                             "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames",
                                             "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                             "Metadata_Report_Info", "WARC_API_Errors",
                                             "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        self.seed_df.to_csv(os.path.join(c.script_output, "seeds.csv"), index=False)

    def tearDown(self):
        """
        Deletes the spreadsheet created by each test.
        """
        os.remove(os.path.join(c.script_output, "seeds.csv"))

    def test_first_message(self):
        """
        Tests that the function returns the correct dataframe and correctly updates the CSV
        when there is not already a message of this type in the dataframe.
        """
        log("Successfully downloaded ARCHIVEIT.warc.gz", self.seed_df, 0, "WARC_API_Errors")
        self.seed_df = self.seed_df.fillna("BLANK")

        # Test that the dataframe has the correct values.
        actual_dataframe = [self.seed_df.columns.tolist()] + self.seed_df.values.tolist()
        expected_dataframe = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection",
                               "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                               "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                               "Size_GB"],
                              ["1111111", "aip1", "title1", "magil", "0000", 12345, "1111111", 1, "ARCHIVEIT.warc.gz",
                               "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
                               "No additional information", "Successfully downloaded ARCHIVEIT.warc.gz",
                               "BLANK", "BLANK", 0.52],
                              ["2222222", "aip2", "title2", "magil", "0000", 12345, "2222222", 2,
                               "ARCHIVEIT.warc.gz,ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                               "Successfully downloaded all metadata reports",
                               "Empty report aip2_seedscope.csv not saved", "BLANK", "BLANK", "BLANK", 0.52]]
        self.assertEqual(actual_dataframe, expected_dataframe, "Problem with test for first message, dataframe values")

        # Test that the CSV has the correct values.
        csv_path = os.path.join(c.script_output, "seeds.csv")
        with open(csv_path, newline="") as open_file:
            reader = csv.reader(open_file)
            actual_csv = list(reader)
        expected_csv = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection",
                         "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                         "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                         "Size_GB"],
                        ["1111111", "aip1", "title1", "magil", "0000", "12345", "1111111", "1", "ARCHIVEIT.warc.gz",
                         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
                         "No additional information", "Successfully downloaded ARCHIVEIT.warc.gz", "", "", "0.52"],
                        ["2222222", "aip2", "title2", "magil", "0000", "12345", "2222222", "2",
                         "ARCHIVEIT.warc.gz,ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                         "Successfully downloaded all metadata reports", "Empty report aip2_seedscope.csv not saved",
                         "", "", "", "0.52"]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for first message, CSV values")

    def test_second_message(self):
        """
        Tests that the function returns the correct dataframe and correctly updates the CSV
        when there is already a message of this type in the dataframe.
        """
        log("Seed column does not have login columns to redact", self.seed_df, 1, "Metadata_Report_Info")
        self.seed_df = self.seed_df.fillna("BLANK")

        # Test that the dataframe has the correct values.
        actual_dataframe = [self.seed_df.columns.tolist()] + self.seed_df.values.tolist()
        expected_dataframe = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection",
                               "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                               "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                               "Size_GB"],
                              ["1111111", "aip1", "title1", "magil", "0000", 12345, "1111111", 1, "ARCHIVEIT.warc.gz",
                               "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
                               "No additional information", "BLANK", "BLANK", "BLANK", 0.52],
                              ["2222222", "aip2", "title2", "magil", "0000", 12345, "2222222", 2,
                               "ARCHIVEIT.warc.gz,ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                               "Successfully downloaded all metadata reports",
                               "Empty report aip2_seedscope.csv not saved; "
                               "Seed column does not have login columns to redact",
                               "BLANK", "BLANK", "BLANK", 0.52]]
        self.assertEqual(actual_dataframe, expected_dataframe, "Problem with test for second message, dataframe values")

        # Test that the CSV has the correct values.
        csv_path = os.path.join(c.script_output, "seeds.csv")
        with open(csv_path, newline="") as open_file:
            reader = csv.reader(open_file)
            actual_csv = list(reader)
        expected_csv = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection",
                         "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                         "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                         "Size_GB"],
                        ["1111111", "aip1", "title1", "magil", "0000", "12345", "1111111", "1", "ARCHIVEIT.warc.gz",
                         "Successfully calculated seed metadata", "Successfully downloaded all metadata reports",
                         "No additional information", "", "", "", "0.52"],
                        ["2222222", "aip2", "title2", "magil", "0000", "12345", "2222222", "2",
                         "ARCHIVEIT.warc.gz,ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                         "Successfully downloaded all metadata reports",
                         "Empty report aip2_seedscope.csv not saved; "
                         "Seed column does not have login columns to redact",
                         "", "", "", "0.52"]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for second message, CSV values")


if __name__ == '__main__':
    unittest.main()