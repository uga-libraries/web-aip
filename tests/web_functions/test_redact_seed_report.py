"""
Tests for the redact_seed_report() function.
If the login columns are present, it replaces the values with REDACTED.

The reports for testing are not downloaded from the Partner API,
because it is not consistent about if the login columns are included for the same seed.
"""
import csv
import numpy as np
import os
import pandas as pd
import shutil
import unittest
from web_functions import redact_seed_report


class MyTestCase(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folders, and the seed.csv files within them.
        """
        for directory in (os.path.join(os.getcwd(), "redact_aip"), os.path.join(os.getcwd(), "no_redact_aip")):
            if os.path.exists(directory):
                shutil.rmtree(directory)

    def test_no_redaction(self):
        """
        Tests that the function does not change seed.csv, and updates the log,
        when there are no login columns to redact.
        """
        # Input needed for the test: seed_df has the progress of the script so far,
        # a folder named with the AIP ID and a seeds.csv file inside the AIP folder.
        # The seeds.csv file only has a few of the actual columns, since only logins are needed for testing.
        seed_df = pd.DataFrame([["1234567", "no_redact_aip", "No Redaction", "test", "no_redact_aip", 123465,
                                 "900000", 1, "ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 0.01]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames",
                                        "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                        "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        os.mkdir("no_redact_aip")
        seed_csv_path = os.path.join(os.getcwd(), "no_redact_aip", "no_redact_aip_seed.csv")
        with open(seed_csv_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["canonical_url", "collection", "seed_type"])
            writer.writerow(["www.noredact.com", 123456, "test"])

        redact_seed_report("no_redact_aip", seed_df, 0)

        # Test that seeds.csv has not changed.
        report_df = pd.read_csv(seed_csv_path)
        actual = [report_df.columns.tolist()] + report_df.values.tolist()
        expected = [["canonical_url", "collection", "seed_type"],
                    ["www.noredact.com", 123456, "test"]]
        self.assertEqual(actual, expected, "Problem with test for no redaction, seed.csv")

        # Test that the log has been updated.
        actual_info = seed_df["Metadata_Report_Info"][0]
        expected_info = "Seed report does not have login columns to redact"
        self.assertEqual(actual_info, expected_info, "Problem with test for no redaction log")

    def test_redaction(self):
        """
        Tests that the function updates seed.csv when there are login columns to redact.
        """
        # Input needed for the test: seed_df has the progress of the script so far,
        # a folder named with the AIP ID and a seeds.csv file inside the AIP folder.
        # The seeds.csv file only has a few of the actual columns, since only logins are needed for testing.
        seed_df = pd.DataFrame([["1234567", "redact_aip", "Redaction", "test", "redact_aip", 123465,
                                 "900000", 1, "ARCHIVEIT-1.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 0.01]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames",
                                        "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                        "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        os.mkdir("redact_aip")
        seed_csv_path = os.path.join(os.getcwd(), "redact_aip", "redact_aip_seed.csv")
        with open(seed_csv_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["canonical_url", "collection", "login_password", "login_username", "seed_type"])
            writer.writerow(["www.noredact.com", 123456, "PASS", "USER", "test"])

        redact_seed_report("redact_aip", seed_df, 0)

        # Test that seeds.csv has not changed.
        report_df = pd.read_csv(seed_csv_path)
        actual = [report_df.columns.tolist()] + report_df.values.tolist()
        expected = [["canonical_url", "collection", "login_password", "login_username", "seed_type"],
                    ["www.noredact.com", 123456, "REDACTED", "REDACTED", "test"]]
        self.assertEqual(actual, expected, "Problem with test for redaction")


if __name__ == '__main__':
    unittest.main()
