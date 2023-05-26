"""
Tests for the seed_data() function.
It returns a dataframe with seed information and also saves the data to a spreadsheet.

The tests convert the dataframe to a list and fill blank cells with a string for easier comparisons.
For the CSV, the test only checks that it exists, not its contents, since those are identical to the dataframe.
"""
import os
import unittest
import configuration as config
from web_functions import seed_data


class TestSeedData(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the spreadsheet created by each test.
        """
        os.remove(os.path.join(config.script_output, "seeds_log.csv"))

    def test_error_no_metadata(self):
        """
        Tests that the function returns the correct dataframe and creates a csv
        with a date range for seeds without metadata.
        """
        seed_df = seed_data("2019-06-03", "2019-06-04")

        # Test that the dataframe has the correct values.
        seed_df = seed_df.fillna("BLANK")
        actual = [seed_df.columns.tolist()] + seed_df.values.tolist()
        expected = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                     "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction", "WARC_API_Errors",
                     "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                    ["2016223", "12249", "918473|918474", 0.001, 2,
                     "ARCHIVEIT-12249-ONE_TIME-JOB918473-SEED2016223-20190603193416006-00000-h3.warc.gz|"
                     "ARCHIVEIT-12249-ONE_TIME-JOB918474-SEED2016223-20190603193421515-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]]
        self.assertEqual(actual, expected, "Problem with test for error no metadata, dataframe values")

        # Test that the CSV was created.
        csv_path_exists = os.path.exists(os.path.join(config.script_output, "seeds_log.csv"))
        self.assertEqual(csv_path_exists, True, "Problem with test for error no metadata, CSV creation")

    def test_hargrett(self):
        """
        Tests that the function returns the correct dataframe and creates a csv
        with a date range for Hargrett seeds.
        """
        seed_df = seed_data("2020-06-08", "2020-06-09")

        # Test that the dataframe has the correct values.
        seed_df = seed_df.fillna("BLANK")
        actual = [seed_df.columns.tolist()] + seed_df.values.tolist()
        expected = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                     "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction", "WARC_API_Errors",
                     "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                    ["2030942", "12181", "1177700", 0.053, 1,
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2084785", "12181", "1177700", 0.106, 1,
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2187482", "12181", "1177700", 3.624, 4,
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz|"
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz|"
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz|"
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2270486", "12912", "1176433", 0.06, 1,
                     "ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]]
        self.assertEqual(actual, expected, "Problem with test for Hargrett, dataframe values")

        # Test that the CSV was created.
        csv_path_exists = os.path.exists(os.path.join(config.script_output, "seeds_log.csv"))
        self.assertEqual(csv_path_exists, True, "Problem with test for Hargrett, CSV creation")

    def test_magil(self):
        """
        Tests that the function returns the correct dataframe and creates a csv
        with a date range for MAGIL seeds.
        """
        seed_df = seed_data("2022-10-25", "2022-10-27")

        # Test that the dataframe has the correct values.
        seed_df = seed_df.fillna("BLANK")
        actual = [seed_df.columns.tolist()] + seed_df.values.tolist()
        expected = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                     "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction", "WARC_API_Errors",
                     "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                    ["2514060", "15678", "1691025", 0.012, 1,
                     "ARCHIVEIT-15678-TEST-JOB1691025-SEED2514060-20221014143847623-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2527198", "15678", "1691033", 0.016, 1,
                     "ARCHIVEIT-15678-TEST-JOB1691033-SEED2527198-20221014143741584-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2527200", "15678", "1691026", 0.088, 1,
                     "ARCHIVEIT-15678-TEST-JOB1691026-SEED2527200-20221014140418701-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2529627", "15678", "1676002", 0.085, 1,
                     "ARCHIVEIT-15678-TEST-JOB1676002-SEED2529627-20220918140010019-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2529629", "15678", "1676316", 0.659, 1,
                     "ARCHIVEIT-15678-TEST-JOB1676316-SEED2529629-20220919135032204-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2529631", "15678", "1676317", 0.184, 1,
                     "ARCHIVEIT-15678-TEST-JOB1676317-SEED2529631-20220919135146371-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2529634", "15678", "1672420", 1.145, 2,
                     "ARCHIVEIT-15678-TEST-JOB1672420-SEED2529634-20220911193609257-00000-h3.warc.gz|"
                     "ARCHIVEIT-15678-TEST-JOB1672420-SEED2529634-20220913205209833-00001-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2529642", "15678", "1672436", 0.874, 1,
                     "ARCHIVEIT-15678-TEST-JOB1672436-SEED2529642-20220911194518522-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2529660", "15678", "1675831", 0.1, 1,
                     "ARCHIVEIT-15678-TEST-JOB1675831-SEED2529660-20220917193734423-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"]]
        self.assertEqual(actual, expected, "Problem with test for MAGIL, dataframe values")

        # Test that the CSV was created.
        csv_path_exists = os.path.exists(os.path.join(config.script_output, "seeds_log.csv"))
        self.assertEqual(csv_path_exists, True, "Problem with test for MAGIL, CSV creation")

    def test_russell(self):
        """
        Tests that the function returns the correct dataframe and creates a csv
        with a date range for Russell seeds.
        """
        seed_df = seed_data("2021-04-13", "2021-04-14")

        # Test that the dataframe has the correct values.
        seed_df = seed_df.fillna("BLANK")
        actual = [seed_df.columns.tolist()] + seed_df.values.tolist()
        expected = [["Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                     "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction", "WARC_API_Errors",
                     "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                    ["2444045", "12265", "1365540", 0.989, 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2444045-20210216160638068-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2481700", "12265", "1365540", 0.751, 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481700-20210216163119142-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2481701", "12265", "1365540", 0.778, 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481701-20210216163428706-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2481702", "12265", "1365540", 1.143, 2,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481702-20210216163706632-00000-h3.warc.gz|"
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481702-20210219082614225-00001-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2481703", "12265", "1365540", 0.774, 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481703-20210216165052332-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
                    ["2481704", "12265", "1365540", 0.802, 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481704-20210216165149447-00000-h3.warc.gz",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK"],
]
        self.assertEqual(actual, expected, "Problem with test for Russell, dataframe values")

        # Test that the CSV was created.
        csv_path_exists = os.path.exists(os.path.join(config.script_output, "seeds_log.csv"))
        self.assertEqual(csv_path_exists, True, "Problem with test for Russell, CSV creation")


if __name__ == '__main__':
    unittest.main()
