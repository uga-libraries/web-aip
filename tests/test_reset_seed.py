"""
Test for the reset_seed() function.
It deletes a seed folder and the information from that seed from seed_df and seeds_log.csv.
"""
import numpy as np
import os
import pandas as pd
import shutil
import unittest
import configuration as config
from web_functions import reset_seed


class TestResetSeed(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folder, if present, and the seeds_log.csv file.
        The function should delete the seed folder, but if there is an error with the function, it might not.
        """
        if os.path.exists(os.path.join(os.getcwd(), "2222222")):
            shutil.rmtree(os.path.join(os.getcwd(), "2222222"))
        os.remove(os.path.join(config.script_output, "seeds_log.csv"))

    def test_reset_seed(self):
        """
        Tests that the function correctly deletes the seed folder, updates seeds_df,
        and updates seeds_log.csv.
        """
        # Makes everything needed for test input:
        # a folder with placeholders for downloaded files, seed_df, and seeds_log.csv
        os.mkdir("2222222")
        with open(os.path.join(os.getcwd(), "2222222", "metadata.csv"), "w") as file:
            file.write("Metadata Placeholder")
        with open(os.path.join(os.getcwd(), "2222222", "ARCHIVEIT.warc"), "w") as warc:
            warc.write("WARC unzip correctly placeholder")
        with open(os.path.join(os.getcwd(), "2222222", "ARCHIVEIT1.warc.open"), "w") as warc1:
            warc1.write("WARC unzip error placeholder")
        columns_list = ["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames", 
                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction", 
                        "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"]
        seed_df = pd.DataFrame([["aip-1", "1111111", "12345", "1000000", 0.521, 1, "ARCHIVEIT.warc.gz", "Success",
                                 "No empty reports", "Success", "Success", "Success", "Success", np.nan],
                                ["aip-2", "2222222", "12345", "2000000", 0.522, 2,
                                 "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz", "Success", "seed.csv", "Success", "Success",
                                 "Success", "Error", np.nan]], columns=columns_list)
        seed_df.to_csv(os.path.join(config.script_output, "seeds_log.csv"), index=False)

        # Runs the function being tested.
        reset_seed("2222222", seed_df)

        # Test that the seed folder was deleted.
        seed_path = os.path.exists(os.path.join(os.getcwd(), "2222222"))
        self.assertEqual(seed_path, False, "Problem with test that the seed folder was deleted")

        # Test that the dataframe has the correct values.
        seed_df = seed_df.fillna("")
        actual_df = [seed_df.columns.tolist()] + seed_df.values.tolist()
        expected_df = [["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                        "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                        "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                       ["aip-1", "1111111", "12345", "1000000", 0.521, 1, "ARCHIVEIT.warc.gz",
                        "Success", "No empty reports", "Success", "Success", "Success", "Success", ""],
                       ["aip-2", "2222222", "12345", "2000000", 0.522, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                        "", "", "", "", "", "", ""]]
        self.assertEqual(actual_df, expected_df, "Problem with test for dataframe values")

        # Test that the CSV has the correct values.
        df = pd.read_csv(os.path.join(config.script_output, "seeds_log.csv"))
        df.fillna("", inplace=True)
        actual_csv = [df.columns.tolist()] + df.values.tolist()
        expected_csv = [["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                         "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                        ["aip-1", 1111111, 12345, 1000000, 0.521, 1, "ARCHIVEIT.warc.gz",
                         "Success", "No empty reports", "Success", "Success", "Success", "Success", ""],
                        ["aip-2", 2222222, 12345, 2000000, 0.522, 2, "ARCHIVEIT.warc.gz|ARCHIVEIT-1.warc.gz",
                         "", "", "", "", "", "", ""]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for CSV values")


if __name__ == '__main__':
    unittest.main()
