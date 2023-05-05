"""
Tests for the add_completeness function.
It calculates the value for Complete based on other data in seed_df for a seed.

To save time, fake data is supplied in seed_df for fields that are not used in these tests
and errors are combined which would not be given the workflow so the combinations can be tested all at once.
"""
import numpy as np
import pandas as pd
import unittest
from web_functions import add_completeness


class MyTestCase(unittest.TestCase):

    def test_all_correct(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there are no errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 2.0, 2, "name.warc.gz;name2.warc.gz",
                                 "Successfully downloaded all metadata reports",
                                 "No empty reports", "Successfully redacted",
                                 "Successfully downloaded name.warc.gz; Successfully downloaded name2.warc.gz",
                                 "Successfully verified name.warc.gz fixity on 2023-05-05; "
                                 "Successfully verified name2.warc.gz fixity on 2023-05-05",
                                 "Successfully unzipped name.warc.gz; Successfully unzipped name2.warc.gz", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "Successfully completed"
        self.assertEqual(actual, expected, "Problem with test for all correct")

    def test_all_errors(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there are every error for all four types.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "aip-id_seed.csv API Error 404; "
                                 "Error: crawl job was not downloaded so can't get crawl definition id",
                                 "No empty reports", "Successfully redacted",
                                 "API Error 500: can't get info about name.warc.gz;"
                                 "Index Error: cannot get the WARC URL or MD5 for name.warc.gz;"
                                 "API Error 4040: can't download name.warc.gz",
                                 "Error: fixity for name.warc.gz cannot be extracted from md5deep output;"
                                 "Error: fixity for name.warc.gz changed and it was deleted",
                                 "Error unzipping name.warc.gz: file not found; "
                                 "Error unzipping name.warc.gz: unzipped to '.gz.open' file", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "Metadata_Report_Errors; WARC_API_Errors; WARC_Fixity_Errors; WARC_Unzip_Errors"
        self.assertEqual(actual, expected, "Problem with test for all four error types")

    def test_metadata_api(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an API error in Metadata_Report_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "1000000_seed.csv API Error 500",
                                 "No empty reports", "Successfully redacted",
                                 "Successfully downloaded name.warc.gz",
                                 "Successfully verified name.warc.gz fixity on 2023-05-05",
                                 "Successfully unzipped name.warc.gz", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "Metadata_Report_Errors"
        self.assertEqual(actual, expected, "Problem with test for Metadata_Report_Errors, API")

    def test_metadata_crawl_def(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an error from not being able to calculate the crawl definition id in Metadata_Report_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "Error: crawl job was not downloaded so can't get crawl definition id",
                                 "No empty reports", "Successfully redacted",
                                 "Successfully downloaded name.warc.gz",
                                 "Successfully verified name.warc.gz fixity on 2023-05-05",
                                 "Successfully unzipped name.warc.gz", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "Metadata_Report_Errors"
        self.assertEqual(actual, expected, "Problem with test for Metadata_Report_Errors, crawl definition id")

    def test_warc_api_download(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an API error from downloading in WARC_API_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "Successfully downloaded all metadata reports",
                                 "No empty reports", "Successfully redacted",
                                 "API Error 404: can't download name.warc.gz",
                                 "Successfully verified name.warc.gz fixity on 2023-05-05",
                                 "Successfully unzipped name.warc.gz", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "WARC_API_Errors"
        self.assertEqual(actual, expected, "Problem with test for WARC_API_Errors, API error from download")

    def test_warc_api_index(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an index error from get_info() in WARC_API_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "Successfully downloaded all metadata reports",
                                 "No empty reports", "Successfully redacted",
                                 "Index Error: cannot get the WARC URL or MD5 for name.warc.gz",
                                 "Successfully verified name.warc.gz fixity on 2023-05-05",
                                 "Successfully unzipped name.warc.gz", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "WARC_API_Errors"
        self.assertEqual(actual, expected, "Problem with test for WARC_API_Errors, index error during get info")

    def test_warc_api_info(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an API error from get_info() in WARC_API_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "Successfully downloaded all metadata reports",
                                 "No empty reports", "Successfully redacted",
                                 "API Error 500: can't get info about name.warc.gz",
                                 "Successfully verified name.warc.gz fixity on 2023-05-05",
                                 "Successfully unzipped name.warc.gz", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "WARC_API_Errors"
        self.assertEqual(actual, expected, "Problem with test for WARC_API_Errors, API error during get info")

    def test_warc_fixity_change(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an error from a change in fixity in WARC_Fixity_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "Successfully downloaded all metadata reports",
                                 "No empty reports", "Successfully redacted",
                                 "Successfully downloaded name.warc.gz",
                                 "Error: fixity for name.warc.gz changed and it was deleted",
                                 "Successfully unzipped name.warc.gz", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "WARC_Fixity_Errors"
        self.assertEqual(actual, expected, "Problem with test for WARC_Fixity_Errors, change in fixity")

    def test_warc_fixity_md5deep(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an error from not being able to extract the fixity from MD5deep output in WARC_Fixity_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "Successfully downloaded all metadata reports",
                                 "No empty reports", "Successfully redacted",
                                 "Successfully downloaded name.warc.gz",
                                 "Error: fixity for name.warc.gz cannot be extracted from md5deep output",
                                 "Successfully unzipped name.warc.gz", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "WARC_Fixity_Errors"
        self.assertEqual(actual, expected, "Problem with test for WARC_Fixity_Errors, can't extract from MD5deep")

    def test_warc_unzip_open(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an error form unzipping to .gz.open in WARC_Unzip_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "Successfully downloaded all metadata reports",
                                 "No empty reports", "Successfully redacted",
                                 "Successfully downloaded name.warc.gz",
                                 "Successfully verified name.warc.gz fixity on 2023-05-05",
                                 "Error unzipping name.warc.gz: unzipped to '.gz.open' file", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "WARC_Unzip_Errors"
        self.assertEqual(actual, expected, "Problem with test for WARC_Unzip_Errors, unzipped to .gz.open")

    def test_warc_unzip_tool(self):
        """
        Tests that the function updates the Complete column of the log correctly
        if there is an error from the unzipping tool in WARC_Fixity_Errors.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([["aip-id", 1000000, 12345, "1234567", 1.0, 1, "name.warc.gz",
                                 "Successfully downloaded all metadata reports",
                                 "No empty reports", "Successfully redacted",
                                 "Successfully downloaded name.warc.gz",
                                 "Successfully verified name.warc.gz fixity on 2023-05-05",
                                 "Error unzipping name.warc.gz: file not found", np.nan]],
                               columns=["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs",
                                        "WARC_Filenames", "Metadata_Report_Errors", "Metadata_Report_Empty",
                                        "Seed_Report_Redaction", "WARC_API_Errors", "WARC_Fixity_Errors",
                                        "WARC_Unzip_Errors", "Complete"])
        add_completeness(0, seed_df)

        # Tests that Complete was updated.
        actual = seed_df['Complete'][0]
        expected = "WARC_Unzip_Errors"
        self.assertEqual(actual, expected, "Problem with test for WARC_Unzip_Errors, error from unzipping tool")


if __name__ == '__main__':
    unittest.main()
