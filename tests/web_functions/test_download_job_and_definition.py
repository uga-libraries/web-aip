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
        for directory in ("harg-000-web-0001", "rbrl-377-web-0001", "rbrl-496-web-0001", "test-web-0001"):
            if os.path.exists(directory):
                shutil.rmtree(os.path.join(os.getcwd(), directory))

    def test_error_no_job(self):
        """
        Tests that the function updates the log when there is not crawl job report.
        Causes the error by using a crawl job id that is not formatted correctly.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([[2202440, "harg-000-web-0001", "UGA Healthy Dawg - University Health Center",
                                 "Hargrett Rare Book & Manuscript Library", 0000, 12181, "job-id-error", 1,
                                 "name.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 1.0]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                        "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir(seed.AIP_ID)
        download_job_and_definition(seed, seed_df, 0)

        # Test that the log was updated.
        actual_errors = seed_df["Metadata_Report_Errors"][0]
        expected_errors = f"{seed.AIP_ID}_job-id-error_crawljob.csv API Error 500; " \
                          f"Crawl job was not downloaded so can't get crawl definition id"
        self.assertEqual(actual_errors, expected_errors, "Problem with test for error/no job, log")

    def test_multi_id_different(self):
        """
        Tests that the function downloads the correct reports for a seed with
        multiple crawl job ids, each of which has a different crawl definition id.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([[2027776, "rbrl-377-web-0001", "Southeast ADA Center: Your Regional Resource",
                                 "Richard B. Russell Library for Political Research and Studies", "rbrl-377", 12264,
                                 "1718467,943446", 1, "name.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 1.0]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                        "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir(seed.AIP_ID)
        download_job_and_definition(seed, seed_df, 0)

        # Test that the crawl job reports have the expected values.
        actual_job = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_1718467_crawljob.csv"),
                                   os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_943446_crawljob.csv")])
        expected_job = [["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                         "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                         "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                         "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                         "warc_url_count"],
                        [1468, False, 12264, 31104519042, 0.29, 59931, 533264465, 4094, 204292457,
                         "2022-12-05 02:22:57.508149", 1718467, 5045264302, 55837, "2022-12-02 15:34:45.822609",
                         "FINISHED", "", "", "", 5447781, "ANNUAL", 5045264368, 59927],
                        ["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                         "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                         "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                         "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                         "warc_url_count"],
                        [1468, False, 12264, 31104250884, 0.21, 4941, 18349119, 810, 23198745,
                         "2019-07-10 19:44:52.710000", 943446, 152599090, 4131, "2019-07-10 13:17:44.208000",
                         "FINISHED", "2019-07-12 19:09:30.842119", "SAVED", "bpieczko", 166941, "TEST_SAVED",
                         152599091, 4940]]
        self.assertEqual(actual_job, expected_job, "Problem with test for multi id/different, job report")

        # Test that the crawl definition reports have the expected values.
        actual_def = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_31104519042_crawldef.csv"),
                                   os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_31104250884_crawldef.csv")])
        expected_def = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                         "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                         "recurrence_type", "test", "time_limit"],
                        [1468, False, "", 12264, "", 31104519042, "", "", "", False, False, "ANNUAL", False, 432000],
                        ["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                         "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                         "recurrence_type", "test", "time_limit"],
                        [1468, False, "", 12264, "", 31104250884, "", "TEST", "", False, False, "NONE", True, 259200]]
        self.assertEqual(actual_def, expected_def, "Problem with test for multi id/different, def report")

    def test_multi_id_different_and_same(self):
        """
        Tests that the function downloads the correct reports for a seed with
        multiple crawl job ids, some with a different crawl definition id and some with the same.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([[2467332, "rbrl-496-web-0001", "Sen. Kelly Loeffler - Home | Facebook",
                                 "Richard B. Russell Library for Political Research and Studies", "rbrl-496", 12265,
                                 "1360420,1365539,1718490", 1, "name.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 1.0]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                        "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir(seed.AIP_ID)
        download_job_and_definition(seed, seed_df, 0)

        # Test that the crawl job reports have the expected values.
        actual_job = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_1360420_crawljob.csv"),
                                   os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_1365539_crawljob.csv"),
                                   os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_1718490_crawljob.csv")])
        expected_job = [["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                         "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                         "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                         "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                         "warc_url_count"],
                        [1468, True, 12265, 31104417189, 0.00, 72860, 1340201663, 56150, 37955462,
                         "2021-02-06 11:00:56.177270", 1360420, 937782182, 16126, "2021-02-05 18:41:34.734584",
                         "FINISHED", "2021-02-16 18:26:21.290625", "SAVED", "robert.lay", 2225339, "TEST_SAVED",
                         937782182, 72860],
                        ["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                         "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                         "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                         "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                         "warc_url_count"],
                        [1468, False, 12265, 31104419857, 0.33, 81312, 873026918, 4922, 249745286,
                         "2021-02-19 13:28:11.208663", 1365539, 16106701936, 76390, "2021-02-16 16:05:25.278295",
                         "FINISHED", "2021-02-19 15:42:42.285898", "SAVED", "robert.lay", 16581766, "TEST_SAVED",
                         16106702118, 81310],
                        ["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                         "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                         "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                         "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                         "warc_url_count"],
                        [1468, False, 12265, 31104392189, 0.45, 116611, 5288690472, 13949, 261240529,
                         "2022-12-05 18:03:41.510355", 1718490, 314651685233, 102662, "2022-12-02 16:07:28.471837",
                         "FINISHED_ABNORMAL", "", "", "", 312441773, "MONTHLY", 314651685263, 116581]]
        self.assertEqual(actual_job, expected_job, "Problem with test for multi id/different and same, job report")

        # Test that the crawl definition reports have the expected values.
        actual_def = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_31104392189_crawldef.csv"),
                                   os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_31104419857_crawldef.csv")])
        expected_def = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                         "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                         "recurrence_type", "test", "time_limit"],
                        [1468, False, "", 12265, "", 31104392189, "", "", "", False, False, "MONTHLY", False, 259200],
                        ["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                         "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                         "recurrence_type", "test", "time_limit"],
                        [1468, False, "", 12265, "", 31104419857, "", "TEST", "", False, False, "NONE", True, 259200]]
        self.assertEqual(actual_def, expected_def, "Problem with test for multi id/different and same, def report")

    def test_multi_id_same(self):
        """
        Tests that the function downloads the correct reports for a seed with
        multiple crawl jobs ids, all with the same crawl definition id.
        """
        # Makes data needed as function input and runs the function.
        seed_df = pd.DataFrame([[2016223, "test-web-0001", "Testing", "UGA", 0000, 12249, "918473,918474",
                                 1, "name.warc.gz", "Successfully calculated seed metadata",
                                 np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 1.0]],
                               columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                        "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                        "Metadata_Report_Errors", "Metadata_Report_Info", "WARC_API_Errors",
                                        "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        seed = [seed for seed in seed_df.itertuples()][0]
        os.mkdir(seed.AIP_ID)
        download_job_and_definition(seed, seed_df, 0)

        # Test that the crawl job report has the expected values.
        actual_job = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_918473_crawljob.csv"),
                                   os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_918474_crawljob.csv")])
        expected_job = [["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                         "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                         "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                         "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                         "warc_url_count"],
                        [1468, False, 12249, 31104242954, 0.17, 226, 680061, 64, 1310383, "2019-06-03 19:56:33.456000",
                         918473, 1128238, 162, "2019-06-03 19:34:10.965000", "FINISHED_ABORTED", "", "", "", 1765,
                         "ONE_TIME", 1128239, 225],
                        ["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                         "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                         "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                         "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                         "warc_url_count"],
                        [1468, False, 12249, 31104242954, 1.23, 38, 318450, 26, 30982, "2019-06-03 19:35:19.985000",
                         918474, 3778, 12, "2019-06-03 19:34:16.916000", "FINISHED_ABORTED", "", "", "",
                         314, "ONE_TIME", 3779, 37]]
        self.assertEqual(actual_job, expected_job, "Problem with test for multi id/same, job report")

        # Test that the crawl definition report has the expected values.
        actual_def = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_31104242954_crawldef.csv")])
        expected_def = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                         "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                         "recurrence_type", "test", "time_limit"],
                        [1468, False, "", 12249, "", 31104242954, "", "ONE_TIME", "", False, False, "NONE",
                         False, 86400]]
        self.assertEqual(actual_def, expected_def, "Problem with test for multi id/same, def report")

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
        self.assertEqual(actual_job, expected_job, "Problem with test for one id, job report")

        # Test that the crawl definition report has the expected values.
        actual_def = csvs_to_list([os.path.join(os.getcwd(), seed.AIP_ID, f"{seed.AIP_ID}_31104315076_crawldef.csv")])
        expected_def = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                         "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only",
                         "recurrence_type", "test", "time_limit"],
                        [1468, False, "", 12181, "", 31104315076, "", "TEST", "", False, False, "NONE", True, 86400]]
        self.assertEqual(actual_def, expected_def, "Problem with test for one id, def report")


if __name__ == '__main__':
    unittest.main()
