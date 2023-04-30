"""
Test for the reset_aip() function.
It deletes a seed folder and the information from that seed from seed_df and seeds_log.csv.
"""
import csv
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
        # Only makes seed 2222222, since it is the one being reset for the test.
        # These file are placeholders for what would usually be in the folder.
        os.mkdir("2222222")
        with open(os.path.join(os.getcwd(), "2222222", "metadata.csv"), "w") as file:
            file.write("Metadata Placeholder")
        with open(os.path.join(os.getcwd(), "2222222", "ARCHIVEIT.warc"), "w") as warc:
            warc.write("WARC unzip correctly placeholder")
        with open(os.path.join(os.getcwd(), "2222222", "ARCHIVEIT1.warc.open"), "w") as warc1:
            warc1.write("WARC unzip error placeholder")

        # Makes seed_df with one completed seed and one that was in progress (later logging fields have no data).
        self.seed_df = pd.DataFrame([["1111111", "12345", "1000000", 0.521, 1, "ARCHIVEIT.warc.gz",
                                      "Success", "No empty reports", "Success", "Success", "Success", "Success"],
                                     ["2222222", "12345", "2000000", 0.522, 2, "ARCHIVEIT.warc.gz;ARCHIVEIT-1.warc.gz",
                                      "Success", "seed.csv", "Success", "Success", "Success", "Error"]],
                                    columns=["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                             "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                             "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                             "WARC_Unzip_Errors"])

        # Makes a log, seeds_log.csv, in the script output directory.
        self.seed_df.to_csv(os.path.join(c.script_output, "seeds_log.csv"), index=False)

    def tearDown(self):
        """
        Deletes the seed folder, if present, and the seeds_log.csv file.
        The function should delete the seed folder, but if there is an error with the function, it might not.
        """
        if os.path.exists(os.path.join(os.getcwd(), "2222222")):
            shutil.rmtree(os.path.join(os.getcwd(), "2222222"))
        os.remove(os.path.join(c.script_output, "seeds_log.csv"))

    def test_reset_aip(self):
        """
        Tests that the function correctly deletes the seed folder, updates seeds_df,
        and updates seeds_log.csv.
        """
        reset_aip("2222222", self.seed_df)

        # Test that the seed folder was deleted.
        seed_path = os.path.exists(os.path.join(os.getcwd(), "2222222"))
        self.assertEqual(seed_path, False, "Problem with test that the seed folder was deleted")

        # Test that the dataframe has the correct values.
        self.seed_df = self.seed_df.fillna("BLANK")
        actual_dataframe = [self.seed_df.columns.tolist()] + self.seed_df.values.tolist()
        expected_dataframe = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                               "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                               "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"],
                              ["1111111", "12345", "1000000", 0.521, 1, "ARCHIVEIT.warc.gz",
                               "Success", "No empty reports", "Success", "Success", "Success", "Success"],
                              ["2222222", "12345", "2000000", 0.522, 2, "ARCHIVEIT.warc.gz;ARCHIVEIT-1.warc.gz",
                               "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]]
        self.assertEqual(actual_dataframe, expected_dataframe, "Problem with test for dataframe values")

        # Test that the CSV has the correct values.
        csv_path = os.path.join(c.script_output, "seeds_log.csv")
        with open(csv_path, newline="") as open_file:
            reader = csv.reader(open_file)
            actual_csv = list(reader)
        expected_csv = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction", "WARC_API_Errors",
                         "WARC_Fixity_Errors", "WARC_Unzip_Errors"],
                        ["1111111", "12345", "1000000", "0.521", "1", "ARCHIVEIT.warc.gz",
                         "Success", "No empty reports", "Success", "Success", "Success", "Success"],
                        ["2222222", "12345", "2000000", "0.522", "2", "ARCHIVEIT.warc.gz;ARCHIVEIT-1.warc.gz",
                         "", "", "", "", "", ""]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for CSV values")


if __name__ == '__main__':
    unittest.main()
