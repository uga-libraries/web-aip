"""
Tests for the download_job_and_definition() function.
It downloads all unique crawl job and crawl definition reports.
"""
import csv
import numpy as np
import os
import pandas as pd
import shutil
import unittest
from web_functions import download_job_and_definition


def csvs_to_list(csv_path_list):
    """
    Returns a list of lists, where each list is a row in a csv.
    If there is more than one CSV, all rows are combined into a single list, including repeating the header.
    """
    row_list = []
    for csv_path in csv_path_list:
        df = pd.read_csv(csv_path)
        df = df.fillna("")
        row_list.append(df.columns.tolist())
        row_list.extend(df.values.tolist())
    return row_list


class MyTestCase(unittest.TestCase):

    def tearDown(self):
        """
        Deletes the seed folders and any reports within them.
        """
        for directory in ("harg-000-web-0001", "name", "name"):
            if os.path.exists(directory):
                shutil.rmtree(os.path.join(os.getcwd(), directory))

    def test_one_id(self):
        """
        Tests that the function downloads the correct reports for a seed with
        one crawl job id, which has one crawl definition id.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([[2202440, "harg-000-web-0001", "UGA Healthy Dawg - University Health Center",
                                 "Hargrett Rare Book & Manuscript Library", 0000, 12181, "1137665", 1,
                                 "name.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 1.0]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                        "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir(seed.AIP_ID)
        download_job_and_definition(seed, seed_df, 0)

        # Test that the crawl job report has the expected values.
        actual_job = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_1137665_crawljob.csv")])
        expected_job = [["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                         "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                         "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                         "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                         "warc_url_count"],
                        [1468, False, 12181, 31104315076, 0.15, 13042, 25682002, 375, 86417725,
                         "2020-04-22 19:23:48.038760", 1137665, 803566568, 12667, "2020-04-21 19:23:12.871109",
                         "FINISHED_TIME_LIMIT", "2020-04-24 18:01:25.790980", "SAVED", "sarmour", 809813,
                         "TEST_SAVED", 803566571, 13039]]
        self.assertEqual(actual_job, expected_job, "Problem with test for one job/one def, job report")

        # Test that the crawl definition report has the expected values.
        actual_def = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_31104315076_crawldef.csv")])
        expected_def = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                         "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                         "recurrence_type", "test", "time_limit"],
                        [1468, False, "", 12181, "", 31104315076, "", "TEST", "", False, False, "NONE", True, 86400]]
        self.assertEqual(actual_def, expected_def, "Problem with test for one job/one def, def report")


if __name__ == '__main__':
    unittest.main()
