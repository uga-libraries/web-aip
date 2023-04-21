"""
Test for the reset_aip() function.
It deletes a seed folder and the information from that seed from seed_df and seeds.csv.
"""
import csv
import numpy as np
import os
import pandas as pd
import shutil
import unittest
import configuration as c
from web_functions import reset_aip


class MyTestCase(unittest.TestCase):

    def setUp(self):
        """
        Makes everything needed for test input.
        """
        # Makes a seed folder with some files in the current directory.
        # Only makes aip2, since it is the one being reset for the test.
        # These file are placeholders for what would usually be in the folder.
        os.mkdir("aip2")
        with open(os.path.join(os.getcwd(), "aip2", "metadata.csv"), "w") as file:
            file.write("Metadata Placeholder")
        with open(os.path.join(os.getcwd(), "aip2", "warc.gz"), "w") as warc:
            warc.write("WARC placeholder")

        # Makes seed_df with one completed seed and one that was in progress (later logging fields have no data).
        self.seed_df = pd.DataFrame([["1111111", "aip1", "title1", "magil", "0000", 12345, "1111111", 1,
                                      "ARCHIVEIT.warc.gz", "Successfully calculated seed metadata",
                                      "Success", "No additional information", "Success", "Success", "Success", 0.52],
                                     ["2222222", "aip2", "title2", "magil", "0000", 12345, "2222222", 2,
                                      "ARCHIVEIT.warc.gz,ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                                      "Success", "Empty report", "Success", "Success", "Error", 0.52]],
                                    columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                             "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames",
                                             "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                             "Metadata_Report_Info", "WARC_API_Errors",
                                             "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])

        # Makes a log, seeds.csv, in the script output directory.
        self.seed_df.to_csv(os.path.join(c.script_output, "seeds.csv"), index=False)

    def tearDown(self):
        """
        Deletes the seed folder, if present, and the seeds.csv file.
        The function should delete the seed folder, but if there is an error with the function, it might not.
        """
        if os.path.exists(os.path.join(os.getcwd(), "aip2")):
            shutil.rmtree(os.path.join(os.getcwd(), "aip2"))
        os.remove(os.path.join(c.script_output, "seeds.csv"))

    def test_reset_aip(self):
        """
        Tests that the function correctly deletes the seed folder, updates seeds_df,
        and updates seeds.csv.
        """
        reset_aip("aip2", self.seed_df)

        # Test that the seed folder was deleted.
        seed_path = os.path.exists(os.path.join(os.getcwd(), "aip2"))
        self.assertEqual(seed_path, False, "Problem with test that the seed folder was deleted")

        # Test that the dataframe has the correct values.
        self.seed_df = self.seed_df.fillna("BLANK")
        actual_dataframe = [self.seed_df.columns.tolist()] + self.seed_df.values.tolist()
        expected_dataframe = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection",
                               "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                               "Size_GB"],
                              ["1111111", "aip1", "title1", "magil", "0000", 12345, "1111111", 1,
                               "ARCHIVEIT.warc.gz", "Successfully calculated seed metadata",
                               "Success", "No additional information", "Success", "Success", "Success", 0.52],
                              ["2222222", "aip2", "title2", "magil", "0000", 12345, "2222222", 2,
                               "ARCHIVEIT.warc.gz,ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                               "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.52]]
        self.assertEqual(actual_dataframe, expected_dataframe, "Problem with test for dataframe values")

        # Test that the CSV has the correct values.
        csv_path = os.path.join(c.script_output, "seeds.csv")
        with open(csv_path, newline="") as open_file:
            reader = csv.reader(open_file)
            actual_csv = list(reader)
        expected_csv = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection",
                         "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                         "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors",
                         "Size_GB"],
                        ["1111111", "aip1", "title1", "magil", "0000", "12345", "1111111", "1",
                         "ARCHIVEIT.warc.gz", "Successfully calculated seed metadata",
                         "Success", "No additional information", "Success", "Success", "Success", "0.52"],
                        ["2222222", "aip2", "title2", "magil", "0000", "12345", "2222222", "2",
                         "ARCHIVEIT.warc.gz,ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                         "", "", "", "", "", "0.52"]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for CSV values")


if __name__ == '__main__':
    unittest.main()
