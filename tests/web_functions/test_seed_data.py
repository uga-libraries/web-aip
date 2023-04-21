"""
Tests for the seed_data() function.
It returns a dataframe with seed information and also saves the data to a spreadsheet.

The tests convert the dataframe to a list and fill blank cells with a string for easier comparisons.
For the CSV, the test only checks that it exists, not its contents, since those are identical to the dataframe.
"""
import os
import unittest
import configuration as c
from web_functions import seed_data


class MyTestCase(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the spreadsheet created by each test.
        """
        os.remove(os.path.join(c.script_output, "seeds.csv"))

    def test_error_no_metadata(self):
        """
        Tests that the function returns the correct dataframe and creates a csv
        with a date range for seeds without metadata.
        """
        seed_df = seed_data("2019-06-03", "2019-06-04")

        # Test that the dataframe has the correct values.
        seed_df = seed_df.fillna("BLANK")
        actual = [seed_df.columns.tolist()] + seed_df.values.tolist()
        expected = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                     "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                     "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"],
                    ["2016223", "BLANK", "Testing", "BLANK", "BLANK", 12249, "918473,918474", 2,
                     "ARCHIVEIT-12249-ONE_TIME-JOB918473-SEED2016223-20190603193416006-00000-h3.warc.gz,"
                     "ARCHIVEIT-12249-ONE_TIME-JOB918474-SEED2016223-20190603193421515-00000-h3.warc.gz",
                     "Couldn't get all required metadata values from the seed report. Will not download files.",
                     "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.0]]
        self.assertEqual(actual, expected, "Problem with test for error no metadata, dataframe values")

        # Test that the CSV was created.
        csv_path_exists = os.path.exists(os.path.join(c.script_output, "seeds.csv"))
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
        expected = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                     "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                     "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"],
                    ["2187482", "harg-0000-web-202006-0001", "Student Government Association Facebook",
                     "hargrett", "0000", 12181, "1177700", 4,
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601191649570-00000-h3.warc.gz,"
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601195532797-00002-h3.warc.gz,"
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601194617707-00001-h3.warc.gz,"
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2187482-20200601200733748-00003-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 3.62],
                    ["2030942", "harg-0000-web-202006-0002", "UGA NAACP Twitter",
                     "hargrett", "0000", 12181, "1177700", 1,
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2030942-20200601191649636-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.05],
                    ["2084785", "harg-0000-web-202006-0003", "Zeta Pi Chapter of Alpha Phi Alpha Twitter",
                     "hargrett", "0000", 12181, "1177700", 1,
                     "ARCHIVEIT-12181-TEST-JOB1177700-SEED2084785-20200601191752355-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.11],
                    ["2270486", "harg-0000-web-202006-0004", "UGA Commencement",
                     "hargrett", "0000", 12912, "1176433", 1,
                     "ARCHIVEIT-12912-TEST-JOB1176433-SEED2270486-20200529202848582-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.06]]
        self.assertEqual(actual, expected, "Problem with test for Hargrett, dataframe values")

        # Test that the CSV was created.
        csv_path_exists = os.path.exists(os.path.join(c.script_output, "seeds.csv"))
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
        expected = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                     "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                     "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"],
                    ["2527198", "magil-ggp-2527198-2022-10", "Georgia Technology Authority",
                     "magil", "0000", 15678, "1691033", 1,
                     "ARCHIVEIT-15678-TEST-JOB1691033-SEED2527198-20221014143741584-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.02],
                    ["2514060", "magil-ggp-2514060-2022-10", "Georgia Board of Health Care Workforce",
                     "magil", "0000", 15678, "1691025", 1,
                     "ARCHIVEIT-15678-TEST-JOB1691025-SEED2514060-20221014143847623-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.01],
                    ["2527200", "magil-ggp-2527200-2022-10", "Georgia Vocational Rehabilitation Agency",
                     "magil", "0000", 15678, "1691026", 1,
                     "ARCHIVEIT-15678-TEST-JOB1691026-SEED2527200-20221014140418701-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.09],
                    ["2529631", "magil-ggp-2529631-2022-10", "Georgia Commission on Equal Opportunity",
                     "magil", "0000", 15678, "1676317", 1,
                     "ARCHIVEIT-15678-TEST-JOB1676317-SEED2529631-20220919135146371-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.18],
                    ["2529629", "magil-ggp-2529629-2022-10", "Georgia Board of Pharmacy",
                     "magil", "0000", 15678, "1676316", 1,
                     "ARCHIVEIT-15678-TEST-JOB1676316-SEED2529629-20220919135032204-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.66],
                    ["2529627", "magil-ggp-2529627-2022-10", "Georgia Aviation Authority",
                     "magil", "0000", 15678, "1676002", 1,
                     "ARCHIVEIT-15678-TEST-JOB1676002-SEED2529627-20220918140010019-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.09],
                    ["2529660", "magil-ggp-2529660-2022-10",
                     "Georgia Governor's Office of Disability Services Ombudsman",
                     "magil", "0000", 15678, "1675831", 1,
                     "ARCHIVEIT-15678-TEST-JOB1675831-SEED2529660-20220917193734423-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.1],
                    ["2529642", "magil-ggp-2529642-2022-10", "Georgia Department of Community Supervision",
                     "magil", "0000", 15678, "1672436", 1,
                     "ARCHIVEIT-15678-TEST-JOB1672436-SEED2529642-20220911194518522-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.87],
                    ["2529634", "magil-ggp-2529634-2022-10",
                     "Georgia Office of Attorney General Consumer Protection Division",
                     "magil", "0000", 15678, "1672420", 2,
                     "ARCHIVEIT-15678-TEST-JOB1672420-SEED2529634-20220911193609257-00000-h3.warc.gz,"
                     "ARCHIVEIT-15678-TEST-JOB1672420-SEED2529634-20220913205209833-00001-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 1.15]]
        self.assertEqual(actual, expected, "Problem with test for MAGIL, dataframe values")

        # Test that the CSV was created.
        csv_path_exists = os.path.exists(os.path.join(c.script_output, "seeds.csv"))
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
        expected = [["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection", "Job_ID",
                     "WARCs", "WARC_Filenames", "Seed_Metadata_Errors", "Metadata_Report_Errors",
                     "Metadata_Report_Info", "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"],
                    ["2481702", "rbrl-494-web-202104-0001", "Bob Trammell for State House - Posts | Facebook",
                     "russell", "rbrl-494", 12265, "1365540", 2,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481702-20210216163706632-00000-h3.warc.gz,"
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481702-20210219082614225-00001-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 1.14],
                    ["2481701", "rbrl-494-web-202104-0002", "Bob Trammell for State House - Videos | Facebook",
                     "russell", "rbrl-494", 12265, "1365540", 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481701-20210216163428706-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.78],
                    ["2444045", "rbrl-494-web-202104-0003", "Bob Trammell for State House | Facebook", "russell",
                     "rbrl-494", 12265, "1365540", 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2444045-20210216160638068-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.99],
                    ["2481704", "rbrl-494-web-202104-0004", "Bob Trammell for State House - Photos | Facebook",
                     "russell", "rbrl-494", 12265, "1365540", 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481704-20210216165149447-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.8],
                    ["2481703", "rbrl-494-web-202104-0005", "Bob Trammell for State House - Events | Facebook",
                     "russell", "rbrl-494", 12265, "1365540", 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481703-20210216165052332-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.77],
                    ["2481700", "rbrl-494-web-202104-0006", "Bob Trammell for State House - About | Facebook",
                     "russell", "rbrl-494", 12265, "1365540", 1,
                     "ARCHIVEIT-12265-TEST-JOB1365540-SEED2481700-20210216163119142-00000-h3.warc.gz",
                     "Successfully calculated seed metadata", "BLANK", "BLANK", "BLANK", "BLANK", "BLANK", 0.75]]
        self.assertEqual(actual, expected, "Problem with test for Russell, dataframe values")

        # Test that the CSV was created.
        csv_path_exists = os.path.exists(os.path.join(c.script_output, "seeds.csv"))
        self.assertEqual(csv_path_exists, True, "Problem with test for Russell, CSV creation")


if __name__ == '__main__':
    unittest.main()
