"""
Tests for the download_crawl_definition() function.
It downloads all unique crawl definition reports.
"""
import numpy as np
import os
import pandas as pd
import shutil
import unittest
import configuration as config
from web_functions import download_crawl_definition, get_report


def csv_to_list(csv_path):
    """
    Reads a CSV into pandas and converts it to a list,
    with the header and each data row as a list within that list.
    Cells with no value are convert to empty strings for easier comparison.
    """
    df = pd.read_csv(csv_path)
    df.fillna("", inplace=True)
    row_list = [df.columns.tolist()] + df.values.tolist()
    return row_list


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


class TestDownloadCrawlDefinition(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folders and any reports within them,
        and the seeds_log.csv if it was made.
        """
        # Seed folders
        for directory in ("2027776", "2016223", "2202440", "2467332"):
            if os.path.exists(directory):
                shutil.rmtree(os.path.join(os.getcwd(), directory))

        # Seeds log
        log_path = os.path.join(config.script_output, "seeds_log.csv")
        if os.path.exists(log_path):
            os.remove(log_path)

    def test_error_no_job(self):
        """
        Tests that the function updates the log when there is no crawl job report.
        Causes the error by not running the function which downloads the crawl job report.
        """
        # Makes data needed as function input and runs the function.
        seed_df = make_df(["harg-0000-web-0001", 2202440, 12181, "1137665", 1.0, 1, "name.warc.gz",
                           np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2202440")
        download_crawl_definition("1137665", seed, seed_df, 0)

        # Test that the log was updated.
        actual = seed_df.at[0, 'Metadata_Report_Errors']
        expected = f"Error: crawl job 1137665 was not downloaded so can't get crawl definition id"
        self.assertEqual(actual, expected, "Problem with test for error/no job")

    def test_multi_id_different(self):
        """
        Tests that the function downloads the correct reports for a seed with
        multiple crawl job ids, each of which has a different crawl definition id.
        """
        # Makes data needed as function input,
        # including downloading the crawl job reports which are read by this function.
        seed_df = make_df(["rbrl-0000-web-0001", 2027776, 12264, "1718467|943446", 1.0, 1, "name.warc.gz",
                           np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2027776")
        get_report(seed, seed_df, 0, "id", "1718467", "crawl_job", f"{seed.AIP_ID}_1718467_crawljob.csv")
        get_report(seed, seed_df, 0, "id", "943446", "crawl_job", f"{seed.AIP_ID}_943446_crawljob.csv")

        # Runs the function for each crawl job. In production, download_metadata() repeats the function call.
        download_crawl_definition("1718467", seed, seed_df, 0)
        download_crawl_definition("943446", seed, seed_df, 0)

        # Test that the crawl definition 31104519042 report has the expected values.
        actual1 = csv_to_list(os.path.join(os.getcwd(), str(seed.Seed_ID), f"{seed.AIP_ID}_31104519042_crawldef.csv"))
        expected1 = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                      "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                      "recurrence_type", "test", "time_limit"],
                     [1468, False, "", 12264, "", 31104519042, "", "", "", False, False,
                      "ANNUAL", False, 432000]]
        self.assertEqual(actual1, expected1, "Problem with test for multi id/different, -9042")

        # Test that the crawl definition 31104250884 report have the expected values.
        actual2 = csv_to_list(os.path.join(os.getcwd(), str(seed.Seed_ID), f"{seed.AIP_ID}_31104250884_crawldef.csv"))
        expected2 = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                      "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                      "recurrence_type", "test", "time_limit"],
                     [1468, False, "", 12264, "", 31104250884, "", "TEST", "", False, False,
                      "NONE", True, 259200]]
        self.assertEqual(actual2, expected2, "Problem with test for multi id/different, -0884")

    def test_multi_id_different_and_same(self):
        """
        Tests that the function downloads the correct reports for a seed with
        multiple crawl job ids, some with a different crawl definition id and some with the same.
        """
        # Makes data needed as function input,
        # including downloading the crawl job reports which are read by this function.
        seed_df = make_df(["rbrl-0000-web-0001", 2467332, 12265, "1360420|1365539|1718490", 1.0, 1,
                           "name.warc.gz", np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2467332")
        get_report(seed, seed_df, 0, "id", "1360420", "crawl_job", f"{seed.AIP_ID}_1360420_crawljob.csv")
        get_report(seed, seed_df, 0, "id", "1365539", "crawl_job", f"{seed.AIP_ID}_1365539_crawljob.csv")
        get_report(seed, seed_df, 0, "id", "1718490", "crawl_job", f"{seed.AIP_ID}_1718490_crawljob.csv")

        # Runs the function for each crawl job. In production, download_metadata() repeats the function call.
        download_crawl_definition("1360420", seed, seed_df, 0)
        download_crawl_definition("1365539", seed, seed_df, 0)
        download_crawl_definition("1718490", seed, seed_df, 0)

        # Test that the crawl definition 31104392189 report has the expected values.
        actual1 = csv_to_list(os.path.join(os.getcwd(), str(seed.Seed_ID), f"{seed.AIP_ID}_31104392189_crawldef.csv"))
        expected1 = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                      "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                      "recurrence_type", "test", "time_limit"],
                     [1468, False, "", 12265, "", 31104392189, "", "", "", False, False,
                      "MONTHLY", False, 259200]]
        self.assertEqual(actual1, expected1, "Problem with test for multi id/different and same, -2189")

        # Test that the crawl definition 31104419857 report has the expected values.
        actual2 = csv_to_list(os.path.join(os.getcwd(), str(seed.Seed_ID), f"{seed.AIP_ID}_31104419857_crawldef.csv"))
        expected2 = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                      "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                      "recurrence_type", "test", "time_limit"],
                     [1468, False, "", 12265, "", 31104419857, "", "TEST", "", False, False,
                      "NONE", True, 259200]]
        self.assertEqual(actual2, expected2, "Problem with test for multi id/different and same, -9857")

    def test_multi_id_same(self):
        """
        Tests that the function downloads the correct reports for a seed with
        multiple crawl jobs ids, all with the same crawl definition id.
        """
        # Makes data needed as function input,
        # including downloading the crawl job reports which are read by this function.
        seed_df = make_df(["harg-0000-web-0001", 2016223, 12249, "918473|918474", 1.0, 1, "name.warc.gz",
                           np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2016223")
        get_report(seed, seed_df, 0, "id", "918473", "crawl_job", f"{seed.AIP_ID}_918473_crawljob.csv")
        get_report(seed, seed_df, 0, "id", "918474", "crawl_job", f"{seed.AIP_ID}_918474_crawljob.csv")

        # Runs the function for each crawl job. In production, download_metadata() repeats the function call.
        download_crawl_definition("918473", seed, seed_df, 0)
        download_crawl_definition("918474", seed, seed_df, 0)

        # Test that the crawl definition report has the expected values.
        actual = csv_to_list(os.path.join(os.getcwd(), str(seed.Seed_ID), f"{seed.AIP_ID}_31104242954_crawldef.csv"))
        expected = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                     "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                     "recurrence_type", "test", "time_limit"],
                    [1468, False, "", 12249, "", 31104242954, "", "ONE_TIME", "", False, False,
                     "NONE", False, 86400]]
        self.assertEqual(actual, expected, "Problem with test for multi id/same")

    def test_one_id(self):
        """
        Tests that the function downloads the correct reports for a seed with
        one crawl job id, which has one crawl definition id.
        """
        # Makes data needed as function input, including downloading the crawl job report, and runs the function.
        seed_df = make_df(["harg-0000-web-0001", 2202440, 12181, "1137665", 1.0, 1, "name.warc.gz",
                           np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir("2202440")
        get_report(seed, seed_df, 0, "id", "1137665", "crawl_job", f"{seed.AIP_ID}_1137665_crawljob.csv")
        download_crawl_definition("1137665", seed, seed_df, 0)

        # Test that the crawl definition report has the expected values.
        actual = csv_to_list(os.path.join(os.getcwd(), str(seed.Seed_ID), f"{seed.AIP_ID}_31104315076_crawldef.csv"))
        expected = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                     "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                     "recurrence_type", "test", "time_limit"],
                    [1468, False, "", 12181, "", 31104315076, "", "TEST", "", False, False,
                     "NONE", True, 86400]]
        self.assertEqual(actual, expected, "Problem with test for one id")


if __name__ == '__main__':
    unittest.main()
