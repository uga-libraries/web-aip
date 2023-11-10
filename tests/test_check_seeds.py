"""
Tests for the check_seeds() function.
It tests that everything which was expected was downloaded.

These are preliminary tests for completeness and most common errors.
More nuance is needed for each way something could be incomplete, but that will wait until this function is reworked.
"""
import numpy as np
import os
import pandas as pd
import unittest
import configuration as config
from web_functions import check_seeds


def make_df(df_rows):
    """
    Makes a dataframe with the provided row information. The column values are the same for all tests.
    Returns the dataframe.
    """
    column_list = ["AIP_ID", "Seed_ID", "AIT_Collection", "Job_ID", "Size_GB", "WARCs", "WARC_Filenames",
                   "Metadata_Report_Errors", "Metadata_Report_Empty", "Seed_Report_Redaction",
                   "WARC_Download_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Complete"]
    df = pd.DataFrame(df_rows, columns=column_list)
    return df


class MyTestCase(unittest.TestCase):

    def test_complete(self):
        """
        Test for when every expected seed folder is present, it has the correct contents,
        and there are no extra seed folders.
        """
        # Makes the data needed for the function input and runs the function.
        rows = [["rbrl-086-web-202002-0001", "2024639", "12265", "1010708", "0.04", "1",
                 "ARCHIVEIT-12265-TEST-JOB1010708-0-SEED2024639-20191021145231642-00000-qzcn0oa1.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["harg-0000-web-202007-0013", "2084816", "12912", "1006358", "1.48", "2",
                 "ARCHIVEIT-12912-TEST-JOB1006358-SEED2084816-20191008162543443-00000-h3.warc.gz|"
                 "ARCHIVEIT-12912-TEST-JOB1006358-SEED2084816-20191008170927609-00001-h3.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["rbrl-378-web-202002-0001", "2090402", "12939", "1011228", "0.03", "1",
                 "ARCHIVEIT-12939-TEST-JOB1011228-0-SEED2090402-20191022182104919-00000-dmqbhl41.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["rbrl-270-web-202002-0002", "2090407", "12939", "1010672", "1.84", "2",
                 "ARCHIVEIT-12939-TEST-JOB1010672-SEED2090407-20191022235750599-00001-h3.warc.gz|"
                 "ARCHIVEIT-12939-TEST-JOB1010672-SEED2090407-20191021141836733-00000-h3.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"]]
        seed_df = make_df(rows)
        seeds_directory = os.path.join("check_seeds", "preservation_download_complete")
        check_seeds("2019-10-30", "2019-10-22", seed_df, seeds_directory)

        # Test for the completeness log.
        df = pd.read_csv(os.path.join(config.script_output, "completeness_check.csv"))
        actual = [df.columns.tolist()] + df.values.tolist()
        expected = [["Seed", "AIP", "Seed Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                     "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                    [2090402, "rbrl-378-web-202002-0001", True, True, True, True, False, 1, 1, True, True],
                    [2090407, "rbrl-270-web-202002-0002", True, True, True, True, True, 1, 1, True, True],
                    [2024639, "rbrl-086-web-202002-0001", True, True, True, True, True, 1, 1, True, True],
                    [2084816, "harg-0000-web-202007-0013", True, True, True, True, False, 1, 1, True, True]]
        self.assertEqual(actual, expected, "Problem with test for complete")

    def test_extra(self):
        """
        Test for when every expected seed folder is present, it has the correct contents,
        but there are extra seed folders.
        """
        # Makes the data needed for the function input and runs the function.
        rows = [["rbrl-086-web-202002-0001", "2024639", "12265", "1010708", "0.04", "1",
                 "ARCHIVEIT-12265-TEST-JOB1010708-0-SEED2024639-20191021145231642-00000-qzcn0oa1.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["harg-0000-web-202007-0013", "2084816", "12912", "1006358", "1.48", "2",
                 "ARCHIVEIT-12912-TEST-JOB1006358-SEED2084816-20191008162543443-00000-h3.warc.gz|"
                 "ARCHIVEIT-12912-TEST-JOB1006358-SEED2084816-20191008170927609-00001-h3.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["rbrl-378-web-202002-0001", "2090402", "12939", "1011228", "0.03", "1",
                 "ARCHIVEIT-12939-TEST-JOB1011228-0-SEED2090402-20191022182104919-00000-dmqbhl41.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["rbrl-270-web-202002-0002", "2090407", "12939", "1010672", "1.84", "2",
                 "ARCHIVEIT-12939-TEST-JOB1010672-SEED2090407-20191022235750599-00001-h3.warc.gz|"
                 "ARCHIVEIT-12939-TEST-JOB1010672-SEED2090407-20191021141836733-00000-h3.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"]]
        seed_df = make_df(rows)
        seeds_directory = os.path.join("check_seeds", "preservation_download_extra")
        check_seeds("2019-10-30", "2019-10-22", seed_df, seeds_directory)

        # Test for the completeness log.
        df = pd.read_csv(os.path.join(config.script_output, "completeness_check.csv"))
        df.fillna("", inplace=True)
        actual = [df.columns.tolist()] + df.values.tolist()
        expected = [["Seed", "AIP", "Seed Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                     "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                    [2090402, "rbrl-378-web-202002-0001", "True", True, True, True, False, 1.0, 1.0, True, True],
                    [2090407, "rbrl-270-web-202002-0002", "True", True, True, True, True, 1.0, 1.0, True, True],
                    [2024639, "rbrl-086-web-202002-0001", "True", True, True, True, True, 1.0, 1.0, True, True],
                    [2084816, "harg-0000-web-202007-0013", "True", True, True, True, False, 1.0, 1.0, True, True],
                    [2000000, "", "Not expected", "", "", "", "", "", "", "", ""],
                    [2050000, "", "Not expected", "", "", "", "", "", "", "", ""],
                    [2100000, "", "Not expected", "", "", "", "", "", "", "", ""]]
        self.assertEqual(actual, expected, "Problem with test for extra seeds")

    def test_missing(self):
        """
        Test for when some of the expected seed folders are missing.
        Every seed folder that is present is expected and has the correct contents.
        """
        # Makes the data needed for the function input and runs the function.
        rows = [["rbrl-086-web-202002-0001", "2024639", "12265", "1010708", "0.04", "1",
                 "ARCHIVEIT-12265-TEST-JOB1010708-0-SEED2024639-20191021145231642-00000-qzcn0oa1.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["harg-0000-web-202007-0013", "2084816", "12912", "1006358", "1.48", "2",
                 "ARCHIVEIT-12912-TEST-JOB1006358-SEED2084816-20191008162543443-00000-h3.warc.gz|"
                 "ARCHIVEIT-12912-TEST-JOB1006358-SEED2084816-20191008170927609-00001-h3.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["rbrl-378-web-202002-0001", "2090402", "12939", "1011228", "0.03", "1",
                 "ARCHIVEIT-12939-TEST-JOB1011228-0-SEED2090402-20191022182104919-00000-dmqbhl41.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["rbrl-270-web-202002-0002", "2090407", "12939", "1010672", "1.84", "2",
                 "ARCHIVEIT-12939-TEST-JOB1010672-SEED2090407-20191022235750599-00001-h3.warc.gz|"
                 "ARCHIVEIT-12939-TEST-JOB1010672-SEED2090407-20191021141836733-00000-h3.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"]]
        seed_df = make_df(rows)
        seeds_directory = os.path.join("check_seeds", "preservation_download_missing")
        check_seeds("2019-10-30", "2019-10-22", seed_df, seeds_directory)

        # Test for the completeness log.
        df = pd.read_csv(os.path.join(config.script_output, "completeness_check.csv"))
        df.fillna("", inplace=True)
        actual = [df.columns.tolist()] + df.values.tolist()
        expected = [["Seed", "AIP", "Seed Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                     "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                    [2090402, "rbrl-378-web-202002-0001", False, "", "", "", "", "", "", "", ""],
                    [2090407, "rbrl-270-web-202002-0002", True, True, True, True, True, 1.0, 1.0, True, True],
                    [2024639, "rbrl-086-web-202002-0001", False, "", "", "", "", "", "", "", ""],
                    [2084816, "harg-0000-web-202007-0013", True, True, True, True, False, 1.0, 1.0, True, True]]
        self.assertEqual(actual, expected, "Problem with test for missing seeds")

    def test_not_complete(self):
        """
        Test for when every expected seed folder is present, but does not have the correct contents.
        It could be missing some things or have extras.
        There are no extra seed folders.
        """
        # Makes the data needed for the function input and runs the function.
        rows = [["rbrl-086-web-202002-0001", "2024639", "12265", "1010708", "0.04", "1",
                 "ARCHIVEIT-12265-TEST-JOB1010708-0-SEED2024639-20191021145231642-00000-qzcn0oa1.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["harg-0000-web-202007-0013", "2084816", "12912", "1006358", "1.48", "2",
                 "ARCHIVEIT-12912-TEST-JOB1006358-SEED2084816-20191008162543443-00000-h3.warc.gz|"
                 "ARCHIVEIT-12912-TEST-JOB1006358-SEED2084816-20191008170927609-00001-h3.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["rbrl-378-web-202002-0001", "2090402", "12939", "1011228", "0.03", "1",
                 "ARCHIVEIT-12939-TEST-JOB1011228-0-SEED2090402-20191022182104919-00000-dmqbhl41.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"],
                ["rbrl-270-web-202002-0002", "2090407", "12939", "1010672", "1.84", "2",
                 "ARCHIVEIT-12939-TEST-JOB1010672-SEED2090407-20191022235750599-00001-h3.warc.gz|"
                 "ARCHIVEIT-12939-TEST-JOB1010672-SEED2090407-20191021141836733-00000-h3.warc.gz",
                 "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "No Errors", "Complete"]]
        seed_df = make_df(rows)
        seeds_directory = os.path.join("check_seeds", "preservation_download_not_complete")
        check_seeds("2019-10-30", "2019-10-22", seed_df, seeds_directory)

        # Test for the completeness log.
        df = pd.read_csv(os.path.join(config.script_output, "completeness_check.csv"))
        actual = [df.columns.tolist()] + df.values.tolist()
        expected = [["Seed", "AIP", "Seed Folder Made", "coll.csv", "collscope.csv", "seed.csv", "seedscope.csv",
                     "crawldef.csv count", "crawljob.csv count", "WARC Count Correct", "All Expected File Types"],
                    [2090402, "rbrl-378-web-202002-0001", True, True, True, True, False, 1, 1, False, True],
                    [2090407, "rbrl-270-web-202002-0002", True, True, True, True, True, 1, 1, False, True],
                    [2024639, "rbrl-086-web-202002-0001", True, False, False, False, False, 0, 0, True, True],
                    [2084816, "harg-0000-web-202007-0013", True, True, True, True, False, 2.0, 3.0, True, False]]
        self.assertEqual(actual, expected, "Problem with test for not complete")


if __name__ == '__main__':
    unittest.main()
