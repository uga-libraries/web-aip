"""
Tests for the get_report function.
It downloads the specified report using the Partner API and saves it as a CSV.
"""
import csv
import numpy as np
import os
import pandas as pd
import shutil
import unittest
import configuration as c
from web_functions import get_report


def csv_list(csv_path):
    """
    Reads a CSV and returns a list, with each row of the CSV as a list within that list.
    """
    with open(csv_path, newline="") as open_file:
        reader = csv.reader(open_file)
        return list(reader)


class MyTestCase(unittest.TestCase):

    def setUp(self):
        """
        Makes a dataframe, seed folder, and seed (has metadata for the one row in the dataframe)
        to use as a starting point for each test.
        """
        self.seed_df = pd.DataFrame([["2027707", "rbrl-498-web-2019-01-001", "Open Records with Deborah Gonzalez",
                                      "russell", "rbrl-498", 12265, "943048", 1,
                                      "ARCHIVEIT-12265-TEST-JOB943048-SEED2027707-20190709144234143-00000-h3.warc.gz",
                                      "Successfully calculated seed metadata",
                                      np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, 0.01]],
                                    columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection",
                                             "AIT_Collection", "Job_ID", "WARCs", "WARC_Filenames",
                                             "Seed_Metadata_Errors", "Metadata_Report_Errors",
                                             "Metadata_Report_Info", "WARC_API_Errors",
                                             "WARC_Fixity_Errors", "WARC_Unzip_Errors", "Size_GB"])
        for seed in self.seed_df.itertuples():
            self.seed = seed
            os.mkdir(self.seed.AIP_ID)

    def tearDown(self):
        """
        Deletes the seed folder and any of its contents from the tests.
        """
        shutil.rmtree(os.path.join(os.getcwd(), self.seed.AIP_ID))

    def test_collection(self):
        """
        Tests that the function downloads the correct collection report.
        The result for testing is the contents of the report.
        """
        csv_name = "rbrl-498-web-2019-01-001_coll.csv"
        get_report(self.seed, self.seed_df, 0, "id", "12265", "collection", csv_name)
        actual = csv_list(os.path.join(os.getcwd(), self.seed.AIP_ID, csv_name))
        expected = [["account", "created_by", "created_date", "deleted", "id", "image", "last_updated_by",
                     "last_updated_date", "metadata.Collector.0.id", "metadata.Collector.0.value",
                     "metadata.Date.0.id", "metadata.Date.0.value", "metadata.Description.0.id",
                     "metadata.Description.0.value", "metadata.Identifier.0.id", "metadata.Identifier.0.value",
                     "metadata.Title.0.id", "metadata.Title.0.value", "name", "oai_exported",
                     "publicly_visible", "state", "topics"],
                    ["1468", "bpieczko", "2019-06-07 13:53:19.132354+00:00", "False", "12265", "2883884", "ahanson",
                     "2020-07-27 14:24:29.521230+00:00", "5035337",
                     "Richard B. Russell Library for Political Research and Studies", "5035338", "Captured 2019-",
                     "5035357", "This collection contains websites documenting political activity in the state of "
                                "Georgia including those created by political candidates, elected officials, and "
                                "political parties.",
                     "5962149", "https://wayback.archive-it.org/12265/*/https://www.youtube.com/channel/"
                                "UC-LF69SBOSgT1S1-yy-VgaA/videos?view=0&sort=dd&shelf_id=0",
                     "5035336", "Georgia Politics", "Georgia Politics", "False",
                     "True", "ACTIVE", ""]]
        self.assertEqual(actual, expected, "Problem with test for collection")

    def test_collection_scope(self):
        """
        Tests that the function downloads the correct collection scope report.
        The result for testing is the contents of the report.
        """
        csv_name = "rbrl-498-web-2019-01-001_collscope.csv"
        get_report(self.seed, self.seed_df, 0, "collection", "12265", "scope_rule", csv_name)
        actual = csv_list(os.path.join(os.getcwd(), self.seed.AIP_ID, csv_name))
        expected = [["abstract_scope_rule", "account", "collection", "created_by", "created_date", "enabled",
                     "host", "id", "last_updated_by", "last_updated_date", "scope_rule_template", "seed", "type",
                     "url_match", "value"],
                    ["", "1468", "12265", "bpieczko", "2019-10-18 13:57:58.967060+00:00", "True", "soundcloud.com",
                     "1001752", "bpieczko", "2019-10-18 13:57:58.967117+00:00", "", "", "IGNORE_ROBOTS", "", ""],
                    ["", "1468", "12265", "robert.lay", "2021-02-10 18:20:52.571273+00:00", "True", "fbcdn.net",
                     "1678549", "robert.lay", "2021-02-10 18:20:52.571321+00:00", "", "", "IGNORE_ROBOTS", "", ""],
                    ["", "1468", "12265", "robert.lay", "2021-02-10 18:20:52.610905+00:00", "True",
                     "www.instagram.com", "1678550", "robert.lay", "2021-02-10 18:20:52.610961+00:00", "", "",
                     "IGNORE_ROBOTS", "", ""]]
        self.assertEqual(actual, expected, "Problem with test for collection scope")

    def test_collection_scope_empty(self):
        """
        Tests that the function does not download an empty collection scope report.
        This is one of two reports (the other is seed scope) that does not always have data.
        """
        csv_name = "magil-ggp-2783596-2022-11_collscope.csv"
        get_report(self.seed, self.seed_df, 0, "collection", "15678", "scope_rule", csv_name)

        # Test that the file was not made.
        csv_path_exists = os.path.exists(os.path.join(os.getcwd(), self.seed.AIP_ID, csv_name))
        self.assertEqual(csv_path_exists, False, "Problem with test for collection scope is empty, is file made")

        # Test that the log information in seed_df was updated.
        actual_info = self.seed_df["Metadata_Report_Info"][0]
        expected_info = f"Empty report {csv_name} not saved"
        self.assertEqual(actual_info, expected_info, "Problem with test for seed scope is empty, log")

    def test_crawl_definition(self):
        """
        Tests that the function downloads the correct crawl definition report.
        The result for testing is the contents of the report.
        """
        csv_name = "rbrl-498-web-2019-01-001_31104250630_crawldef.csv"
        get_report(self.seed, self.seed_df, 0, "id", "31104250630", "crawl_definition", csv_name)
        actual = csv_list(os.path.join(os.getcwd(), self.seed.AIP_ID, csv_name))
        expected = [["account", "brozzler", "byte_limit", "collection", "document_limit", "id", "machine_count",
                     "one_time_subtype", "patch_for_qa_job_id", "patch_ignore_robots", "pdfs_only", "recurrence_type",
                     "test","time_limit"],
                    ["1468", "False", "", "12265", "", "31104250630", "", "TEST", "", "False", "False", "NONE",
                     "True", "259200"]]
        self.assertEqual(actual, expected, "Problem with test for crawl definition")

    def test_crawl_job(self):
        """
        Tests that the function downloads the correct crawl job report.
        The result for testing is the contents of the report.
        """
        csv_name = "rbrl-498-web-2019-01-001_943048_crawljob.csv"
        get_report(self.seed, self.seed_df, 0, "id", "943048", "crawl_job", csv_name)
        actual = csv_list(os.path.join(os.getcwd(), self.seed.AIP_ID, csv_name))
        expected = [["account", "brozzler", "collection", "crawl_definition", "doc_rate", "downloaded_count",
                     "duplicate_bytes", "duplicate_count", "elapsed_ms", "end_date", "id", "novel_bytes",
                     "novel_count", "original_start_date", "status", "test_crawl_save_date", "test_crawl_state",
                     "test_crawl_state_changed_by", "total_data_in_kbs", "type", "warc_content_bytes",
                     "warc_url_count"],
                    ["1468", "False", "12265", "31104250630", "1.25", "313", "258029", "16", "251381",
                     "2019-07-09 14:47:05.750000", "943048", "9461096", "297", "2019-07-09 14:42:30.426000",
                     "FINISHED", "2019-07-12 19:00:42.393776", "SAVED", "bpieczko", "9491", "TEST_SAVED",
                     "9461097", "312"]]
        self.assertEqual(actual, expected, "Problem with test for crawl job")

    def test_seed(self):
        """
        Tests that the function downloads the correct seed report.
        The result for testing is the contents of the report.
        """
        csv_name = "rbrl-498-web-2019-01-001_seed.csv"
        get_report(self.seed, self.seed_df, 0, "id", "2027707", "seed", csv_name)
        actual = csv_list(os.path.join(os.getcwd(), self.seed.AIP_ID, csv_name))
        expected = [["active", "canonical_url", "collection", "crawl_definition", "created_by", "created_date",
                     "deleted", "http_response_code", "id", "last_checked_http_response_code", "last_updated_by",
                     "last_updated_date", "metadata.Collector.0.id",
                     "metadata.Collector.0.value", "metadata.Creator.0.id", "metadata.Creator.0.value",
                     "metadata.Date.0.id", "metadata.Date.0.value", "metadata.Description.0.id",
                     "metadata.Description.0.value", "metadata.Identifier.0.id", "metadata.Identifier.0.value",
                     "metadata.Language.0.id", "metadata.Language.0.value", "metadata.Language.1.id",
                     "metadata.Language.1.value", "metadata.Relation.0.id", "metadata.Relation.0.value",
                     "metadata.Rights.0.id", "metadata.Rights.0.value", "metadata.Title.0.id",
                     "metadata.Title.0.value", "publicly_visible", "seed_groups.0.account",
                     "seed_groups.0.collections.0", "seed_groups.0.id", "seed_groups.0.name",
                     "seed_groups.0.visibility", "seed_type", "url", "valid"],
                    ["True", "https://openrecords.podbean.com/", "12265", "31104392189", "bpieczko",
                     "2019-07-09 14:38:48.931071+00:00", "False", "", "2027707", "", "robert.lay",
                     "2022-10-21 19:43:26.083542+00:00", "4591702",
                     "Richard B. Russell Library for Political Research and Studies", "4591701", "Gonzalez, Deborah",
                     "4660749", "Captured 2019-", "4660889", "Podcast series hosted by Deborah Gonzalez.", "4596520",
                     "https://wayback.archive-it.org/12265/*/https://openrecords.podbean.com/", "4970089", "Spanish",
                     "4970088", "English", "6885114", "RBRL/498: Deborah Gonzalez Papers", "4769389",
                     "In Copyright: http://rightsstatements.org/vocab/InC/1.0/", "4591700",
                     "Open Records with Deborah Gonzalez", "True", "1468", "12265", "12978", "Deborah Gonzalez",
                     "PUBLIC", "normal", "https://openrecords.podbean.com/", ""]]
        self.assertEqual(actual, expected, "Problem with test for seed")

    def test_seed_scope(self):
        """
        Tests that the function downloads the correct seed scope report.
        The result for testing is the contents of the report.
        """
        csv_name = "rbrl-498-web-2019-01-001_seedscope.csv"
        get_report(self.seed, self.seed_df, 0, "seed", "2027707", "scope_rule", csv_name)
        actual = csv_list(os.path.join(os.getcwd(), self.seed.AIP_ID, csv_name))
        expected = [["abstract_scope_rule", "account", "collection", "created_by", "created_date", "enabled",
                     "host", "id", "last_updated_by", "last_updated_date", "scope_rule_template", "seed",
                     "type", "url_match", "value"],
                    ["", "1468", "", "bpieczko", "2019-07-09 14:39:43.257364+00:00", "True", "", "933838", "bpieczko",
                     "2019-07-09 14:39:43.257394+00:00", "", "2027707", "BLOCK_URL", "STRING_MATCH",
                     "http://openrecords.podbean.com/"]]
        self.assertEqual(actual, expected, "Problem with test for collection scope")

    def test_seed_scope_empty(self):
        """
        Tests that the function does not download an empty seed scope report.
        This is one of two reports (the other is collection scope) that does not always have data.
        """
        csv_name = "magil-ggp-2783596-2022-11_seedscope.csv"
        get_report(self.seed, self.seed_df, 0, "seed", "2783596", "scope_rule", csv_name)

        # Test that the file was not made.
        csv_path_exists = os.path.exists(os.path.join(os.getcwd(), self.seed.AIP_ID, csv_name))
        self.assertEqual(csv_path_exists, False, "Problem with test for seed scope is empty, is file made")

        # Test that the log information in seed_df was updated.
        actual_info = self.seed_df["Metadata_Report_Info"][0]
        expected_info = f"Empty report {csv_name} not saved"
        self.assertEqual(actual_info, expected_info, "Problem with test for seed scope is empty, log")


if __name__ == '__main__':
    unittest.main()
