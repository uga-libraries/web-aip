"""
Tests the output of the ait_download.py script.
It downloads metadata and WARCs for the seeds saved during the specified date range.
Multiple date ranges are used to test the main data variation while limiting the amount downloaded.
"""
import pandas as pd
import os
import shutil
import subprocess
import unittest
import configuration as config


def consistent_seeds_log(csv_path):
    """
    Reads the seeds_log.csv produced by the test into a dataframe,
    and makes changes to data that can vary between tests to allow comparison to expected results.
    Returns the dataframe.
    """
    # Reads the CSV into a dataframe.
    df = pd.read_csv(csv_path)

    # Replaces blanks with an empty string.
    df.fillna("", inplace=True)

    # Replaces the value of WARC_Fixity_Errors, which includes timestamps,
    # with the number of WARCs that were successfully verified.
    df['WARC_Fixity_Errors'] = df['WARC_Fixity_Errors'].str.count("Successfully")

    # If Seed_Report_Redaction has no login columns, replaces with the other standard message of success.
    # The same seed sometimes has the login columns and sometimes does not.
    mask = df['Seed_Report_Redaction'] == "No login columns to redact"
    df.loc[mask, 'Seed_Report_Redaction'] = "Successfully redacted"

    return df


class MyTestCase(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the preservation folder with all the downloaded seed contents and the CSV files created by the tests.
        """
        shutil.rmtree(os.path.join(config.script_output, "preservation_download"))
        os.remove(os.path.join(config.script_output, "completeness_check.csv"))
        os.remove(os.path.join(config.script_output, "seeds_log.csv"))

    def test_multi_warc_seed(self):
        """
        Tests the full script with a data range that has 1 Hargrett seed with 3 WARCs and multiple crawl jobs.
        Results for testing are the contents of the three CSVs made by the script.
        """
        script_path = os.path.join("..", "ait_download.py")
        subprocess.run(f"python {script_path} 2020-04-30 2020-05-15", shell=True)

        # Test for metadata.csv
        metadata_df = pd.read_csv(os.path.join(config.script_output, "preservation_download", "metadata.csv"))
        actual_metadata = [metadata_df.columns.tolist()] + metadata_df.values.tolist()
        expected_metadata = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                             ["hargrett", "harg-0000", 2173769, "harg-0000-web-202005-0001",
                              "Coronavirus (COVID-19) Information and Resources website", 1]]
        self.assertEqual(actual_metadata, expected_metadata, "Problem with test for multi WARC seed, metadata.csv")

        # Test for seeds_log.csv
        seeds_df = consistent_seeds_log(os.path.join(config.script_output, "seeds_log.csv"))
        expected_seeds = [seeds_df.columns.tolist()] + seeds_df.values.tolist()

        actual_seeds = [["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                         "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                         "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                        ["harg-0000-web-202005-0001", 2173769, 12912, "1154002|1148757|1143415", 0.007, 3,
                         "ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz|"
                         "ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz|"
                         "ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         "Successfully downloaded all metadata reports", "harg-0000-web-202005-0001_seedscope.csv",
                         "Successfully redacted",
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; "
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; "
                         "Successfully downloaded ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         3,
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1154002-SEED2173769-20200513225747964-00000-h3.warc.gz; "
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1148757-SEED2173769-20200506223640988-00000-h3.warc.gz; "
                         "Successfully unzipped ARCHIVEIT-12912-WEEKLY-JOB1143415-SEED2173769-20200430010118013-00000-h3.warc.gz",
                         "Successfully completed"]]
        self.assertEqual(expected_seeds, actual_seeds, "Problem with test for multi WARC seed, seeds_log.csv")

        # Test for completeness_check.csv
        cc_df = pd.read_csv(os.path.join(config.script_output, "completeness_check.csv"))
        expected_cc = [cc_df.columns.tolist()] + cc_df.values.tolist()
        actual_cc = [["Seed", "AIP", "Seed Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                      "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                     [2173769, "harg-0000-web-202005-0001", True, True, True, True, False, 1, 3, True, True]]
        self.assertEqual(expected_cc, actual_cc, "Problem with test for multi WARC seed, completeness_check.csv")

    def test_one_warc_seeds(self):
        """
        Tests the full script with a data range that has 2 Russell seeds, each with 1 WARC.
        Results for testing are the contents of the three CSVs made by the script.
        """
        script_path = os.path.join("..", "ait_download.py")
        subprocess.run(f"python {script_path} 2019-07-12 2019-07-13", shell=True)

        # Test for metadata.csv
        metadata_df = pd.read_csv(os.path.join(config.script_output, "preservation_download", "metadata.csv"))
        actual_metadata = [metadata_df.columns.tolist()] + metadata_df.values.tolist()
        expected_metadata = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                             ["russell", "rbrl-498", 2027707, "rbrl-498-web-201907-0001",
                              "Open Records with Deborah Gonzalez", 1],
                             ["russell", "rbrl-377", 2027776, "rbrl-377-web-201907-0001",
                              "Southeast ADA Center: Your Regional Resource for the "
                              "Americans with Disabilities Act (ADA)", 1]]
        self.assertEqual(actual_metadata, expected_metadata, "Problem with test for one WARC seeds, metadata.csv")

        # Test for seeds_log.csv
        seeds_df = consistent_seeds_log(os.path.join(config.script_output, "seeds_log.csv"))
        actual_seeds = [seeds_df.columns.tolist()] + seeds_df.values.tolist()

        expected_seeds = [["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                           "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                           "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                          ["rbrl-498-web-201907-0001", 2027707, 12265, 943048, 0.007, 1,
                           "ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                           "Successfully downloaded all metadata reports", "No empty reports", "Successfully redacted",
                           "Successfully downloaded ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                           1,
                           "Successfully unzipped ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                           "Successfully completed"],
                          ["rbrl-377-web-201907-0001", 2027776, 12264, 943446, 0.096, 1,
                           "ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                           "Successfully downloaded all metadata reports", "rbrl-377-web-201907-0001_seedscope.csv",
                           "Successfully redacted",
                           "Successfully downloaded ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                           1,
                           "Successfully unzipped ARCHIVEIT-12264-TEST-JOB943446-SEED2027776-20190710131748634-00000-h3.warc.gz",
                           "Successfully completed"]]
        self.assertEqual(actual_seeds, expected_seeds, "Problem with test for one WARC seeds, seeds_log.csv")

        # Test for completeness_check.csv
        cc_df = pd.read_csv(os.path.join(config.script_output, "completeness_check.csv"))
        expected_cc = [cc_df.columns.tolist()] + cc_df.values.tolist()
        actual_cc = [["Seed", "AIP", "Seed Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                      "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                     [2027776, "rbrl-377-web-201907-0001", True, True, True, True, False, 1, 1, True, True],
                     [2027707, "rbrl-498-web-201907-0001", True, True, True, True, True, 1, 1, True, True]]
        self.assertEqual(expected_cc, actual_cc, "Problem with test for one WARC seeds, completeness_check.csv")

    def test_restart(self):
        """
        Tests the full script with a data range that has 4 MAGIL seeds, simulating a restart after an error.
        Results for testing are the contents of the three CSVs made by the script.
        """
        # Copies files from the test folder which would be present if the script had run once
        # and been interrupted while the 3rd seed (2529683) was in progress.
        shutil.copytree(os.path.join(os.getcwd(), "script", "preservation_download"),
                        os.path.join(config.script_output, "preservation_download"))
        shutil.copy2(os.path.join(os.getcwd(), "script", "seeds_log.csv"), config.script_output)

        # Runs the script
        script_path = os.path.join("..", "ait_download.py")
        subprocess.run(f"python {script_path} 2023-04-21 2023-05-02", shell=True)

        # Test for metadata.csv
        metadata_df = pd.read_csv(os.path.join(config.script_output, "preservation_download", "metadata.csv"))
        actual_metadata = [metadata_df.columns.tolist()] + metadata_df.values.tolist()
        expected_metadata = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                             ["magil", "magil-0000", 2520379, "magil-ggp-2520379-2023-05",
                              "Georgia Department of Natural Resources Wildlife Resources Division", 1],
                             ["magil", "magil-0000", 2529671, "magil-ggp-2529671-2023-05",
                              "Georgia Real Estate Commission & Appraisers Board", 1],
                             ["magil", "magil-0000", 2529676, "magil-ggp-2529676-2023-05",
                              "Georgia State Board of Accountancy", 1],
                             ["magil", "magil-0000", 2529683, "magil-ggp-2529683-2023-05",
                              "Georgia State Finance Commission", 1]]
        self.assertEqual(actual_metadata, expected_metadata, "Problem with test for restart, metadata.csv")

        # Test for seeds_log.csv
        # Changes column with time stamps and convert blanks to empty strings
        # to allow comparison to consistent expected values.
        seeds_df = consistent_seeds_log(os.path.join(config.script_output, "seeds_log.csv"))
        actual_seeds = [seeds_df.columns.tolist()] + seeds_df.values.tolist()
        expected_seeds = [["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                           "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                           "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"],
                          ["magil-ggp-2520379-2023-05", 2520379, 15678, 1789230, 7.434, 7,
                           "ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230416150811551-00000-sl63gmud.warc.gz|"
                           "ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417184230631-00001-sl63gmud.warc.gz|"
                           "ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417185737629-00002-sl63gmud.warc.gz|"
                           "ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417193251948-00003-sl63gmud.warc.gz|"
                           "ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417201414622-00004-sl63gmud.warc.gz|"
                           "ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417223344837-00005-sl63gmud.warc.gz|"
                           "ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230419134031254-00000-dwz98uv7.warc.gz",
                           "Successfully downloaded all metadata reports",
                           "magil-ggp-2520379-2023-05_seedscope.csv; magil-ggp-2520379-2023-05_collscope.csv",
                           "Successfully redacted",
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230416150811551-00000-sl63gmud.warc.gz; "
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417184230631-00001-sl63gmud.warc.gz; "
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417185737629-00002-sl63gmud.warc.gz; "
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417193251948-00003-sl63gmud.warc.gz; "
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417201414622-00004-sl63gmud.warc.gz; "
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417223344837-00005-sl63gmud.warc.gz; "
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230419134031254-00000-dwz98uv7.warc.gz",
                           7,
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230416150811551-00000-sl63gmud.warc.gz; "
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417184230631-00001-sl63gmud.warc.gz; "
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417185737629-00002-sl63gmud.warc.gz; "
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417193251948-00003-sl63gmud.warc.gz; "
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417201414622-00004-sl63gmud.warc.gz; "
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230417223344837-00005-sl63gmud.warc.gz; "
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1789230-0-SEED2520379-20230419134031254-00000-dwz98uv7.warc.gz",
                           "Successfully completed"],
                          ["magil-ggp-2529671-2023-05", 2529671, 15678, 1791478, 0.028, 1,
                           "ARCHIVEIT-15678-TEST-JOB1791478-0-SEED2529671-20230420155417222-00000-mntg8u5v.warc.gz",
                           "Successfully downloaded all metadata reports",
                           "magil-ggp-2529671-2023-05_seedscope.csv; magil-ggp-2529671-2023-05_collscope.csv",
                           "Successfully redacted",
                           "API error 404: can't downloaded ARCHIVEIT-15678-TEST-JOB1791478-0-SEED2529671-20230420155417222-00000-mntg8u5v.warc.gz",
                           0, "", "WARC_Downloaded_Errors"],
                          ["magil-ggp-2529683-2023-05", 2529683, 15678, 1791489, 0.05, 2,
                           "ARCHIVEIT-15678-TEST-JOB1791489-0-SEED2529683-20230420161205384-00000-qix5zv0f.warc.gz|"
                           "ARCHIVEIT-15678-TEST-JOB1791489-0-SEED2529683-20230420230248436-00000-8bk2lsxt.warc.gz",
                           "Successfully downloaded all metadata reports",
                           "magil-ggp-2529683-2023-05_seedscope.csv; magil-ggp-2529683-2023-05_collscope.csv",
                           "Successfully redacted",
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1791489-0-SEED2529683-20230420161205384-00000-qix5zv0f.warc.gz; "
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1791489-0-SEED2529683-20230420230248436-00000-8bk2lsxt.warc.gz",
                           2,
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1791489-0-SEED2529683-20230420161205384-00000-qix5zv0f.warc.gz; "
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1791489-0-SEED2529683-20230420230248436-00000-8bk2lsxt.warc.gz",
                           "Successfully completed"],
                          ["magil-ggp-2529676-2023-05", 2529676, 15678, 1791480, 0.014, 1,
                           "ARCHIVEIT-15678-TEST-JOB1791480-0-SEED2529676-20230420155757131-00000-zrl3k481.warc.gz",
                           "Successfully downloaded all metadata reports",
                           "magil-ggp-2529676-2023-05_seedscope.csv; magil-ggp-2529676-2023-05_collscope.csv",
                           "Successfully redacted",
                           "Successfully downloaded ARCHIVEIT-15678-TEST-JOB1791480-0-SEED2529676-20230420155757131-00000-zrl3k481.warc.gz",
                           1,
                           "Successfully unzipped ARCHIVEIT-15678-TEST-JOB1791480-0-SEED2529676-20230420155757131-00000-zrl3k481.warc.gz",
                           "Successfully completed"]]
        self.assertEqual(actual_seeds, expected_seeds, "Problem with test for restart, seeds_log.csv")

        # Test for completeness_check.csv
        cc_df = pd.read_csv(os.path.join(config.script_output, "completeness_check.csv"))
        expected_cc = [cc_df.columns.tolist()] + cc_df.values.tolist()
        actual_cc = [["Seed", "AIP", "Seed Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                      "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                     [2529683, "magil-ggp-2529683-2023-05", True, True, False, True, False, 1, 1, True, True],
                     [2529676, "magil-ggp-2529676-2023-05", True, True, False, True, False, 1, 1, True, True],
                     [2529671, "magil-ggp-2529671-2023-05", True, True, False, True, False, 1, 1, False, True],
                     [2520379, "magil-ggp-2520379-2023-05", True, True, False, True, False, 1, 1, True, True]]
        self.assertEqual(expected_cc, actual_cc, "Problem with test for one WARC seeds, completeness_check.csv")


if __name__ == '__main__':
    unittest.main()
