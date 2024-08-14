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
import configuration as config
from web_functions import redact_seed_report


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


class TestRedactSeedReport(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folders, the seed.csv files within them, and the seeds_log.csv.
        """
        if os.path.exists(os.path.join(os.getcwd(), "1234567")):
            shutil.rmtree(os.path.join(os.getcwd(), "1234567"))
        os.remove(os.path.join(config.script_output, "seeds_log.csv"))

    def test_error_no_report(self):
        """
        Tests that the function correctly updates the log when there is no seeds.csv report.
        This only happens if there was an error with downloading seeds.csv.
        """
        # Input needed for the test: seed_df has the progress of the script so far,
        # and a folder named with the seed ID.
        seed_df = make_df(["aip-1", "1234567", 123465, "900000", 0.01, 1, "ARCHIVEIT-1.warc.gz",
                           np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
        os.mkdir("1234567")

        redact_seed_report("1234567", "aip-1", seed_df, 0)

        # Test that the log has been updated.
        actual = seed_df.at[0, 'Seed_Report_Redaction']
        expected = "No seeds.csv to redact"
        self.assertEqual(actual, expected, "Problem with test for error: no report")

    def test_no_redaction(self):
        """
        Tests that the function does not change seed.csv, and updates the log,
        when there are no login columns to redact.
        """
        # Input needed for the test: seed_df has the progress of the script so far,
        # a folder named with the Seed ID and a seeds_log.csv file inside the AIP folder.
        # The seeds_log.csv file only has a few of the actual columns, since only logins are needed for testing.
        seed_df = make_df(["aip-1", "1234567", 123465, "900000", 0.01, 1, "ARCHIVEIT-1.warc.gz",
                           np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
        os.mkdir("1234567")
        seed_csv_path = os.path.join(os.getcwd(), "1234567", "aip-1_seed.csv")
        with open(seed_csv_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["canonical_url", "collection", "seed_type"])
            writer.writerow(["www.noredact.com", 123456, "test"])

        redact_seed_report("1234567", "aip-1", seed_df, 0)

        # Test that seed report has not changed.
        report_df = pd.read_csv(seed_csv_path)
        actual = [report_df.columns.tolist()] + report_df.values.tolist()
        expected = [["canonical_url", "collection", "seed_type"],
                    ["www.noredact.com", 123456, "test"]]
        self.assertEqual(actual, expected, "Problem with test for no redaction, seed report")

        # Test that the log has been updated.
        actual_info = seed_df.at[0, 'Seed_Report_Redaction']
        expected_info = "No login columns to redact"
        self.assertEqual(actual_info, expected_info, "Problem with test for no redaction, log")

    def test_redaction(self):
        """
        Tests that the function updates seed.csv when there are login columns to redact.
        """
        # Input needed for the test: seed_df has the progress of the script so far,
        # a folder named with the Seed ID and a seeds_log.csv file inside the AIP folder.
        # The seeds_log.csv file only has a few of the actual columns, since only logins are needed for testing.
        seed_df = make_df(["aip-1", "1234567", 123465, "900000", 0.01, 1, "ARCHIVEIT-1.warc.gz",
                           np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
        os.mkdir("1234567")
        seed_csv_path = os.path.join(os.getcwd(), "1234567", "aip-1_seed.csv")
        with open(seed_csv_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["canonical_url", "collection", "login_password", "login_username", "seed_type"])
            writer.writerow(["www.noredact.com", 123456, "PASS", "USER", "test"])

        redact_seed_report("1234567", "aip-1", seed_df, 0)

        # Test that seed report has changed.
        report_df = pd.read_csv(seed_csv_path)
        actual = [report_df.columns.tolist()] + report_df.values.tolist()
        expected = [["canonical_url", "collection", "login_password", "login_username", "seed_type"],
                    ["www.noredact.com", 123456, "REDACTED", "REDACTED", "test"]]
        self.assertEqual(actual, expected, "Problem with test for redaction, seed report")

        # Test that the log has been updated.
        actual_info = seed_df.at[0, 'Seed_Report_Redaction']
        expected_info = "Successfully redacted"
        self.assertEqual(actual_info, expected_info, "Problem with test for redaction, log")


if __name__ == '__main__':
    unittest.main()
