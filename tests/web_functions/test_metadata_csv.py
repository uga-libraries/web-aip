"""
Tests for the aip_df = metadata_csv() function.
It creates a file named metadata.csv in the preservation_download folder to use with the general-aip.py script.
"""
import os
import pandas as pd
import shutil
import unittest
import configuration as c
from web_functions import metadata_csv


class MyTestCase(unittest.TestCase):

    def setUp(self):
        """
        Makes the preservation_download directory where the metadata.csv file should be saved.
        """
        os.mkdir(os.path.join(c.script_output, "preservation_download"))

    def tearDown(self):
        """
        Deletes the preservation_download directory and the metadata.csv file it contains.
        """
        shutil.rmtree(os.path.join(c.script_output, "preservation_download"))

    def test_api_error_all(self):
        """
        Tests that the function creates the correct metadata.csv and returns the correct dataframe
        when all of the seeds have an API error.
        """
        aip_df = metadata_csv(["error-one", "error-two"], "2019-01-02")

        # Test for the contents of metadata.csv
        actual_csv_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_csv = [actual_csv_df.columns.tolist()] + actual_csv_df.values.tolist()
        expected_csv = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                        ["API error 500", "TBD", "error-one", "TBD-web-201901-0001", "TBD", 1],
                        ["API error 500", "TBD", "error-two", "TBD-web-201901-0002", "TBD", 1]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for API error, all errors, metadata.csv")
        
        # Test for the returned dataframe.
        actual_aip = [aip_df.columns.tolist()] + aip_df.values.tolist()
        expected_aip = [["Seed_ID", "AIP_ID"],
                        ["error-one", "TBD-web-201901-0001"],
                        ["error-two", "TBD-web-201901-0002"]]
        self.assertEqual(actual_aip, expected_aip, "Problem with test for API error, all errors, aip")

    def test_api_error_one(self):
        """
        Tests that the function creates the correct metadata.csv and returns the correct dataframe
        when one seed has an API error and the other is correct.
        """
        aip_df = metadata_csv(["error-one", "2529685"], "2019-01-02")

        # Test for the contents of metadata.csv
        actual_csv_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_csv = [actual_csv_df.columns.tolist()] + actual_csv_df.values.tolist()
        expected_csv = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                        ["magil", "magil-0000", "2529685", "magil-ggp-2529685-2019-01",
                         "Teachers Retirement System of Georgia", 1],
                        ["API error 500", "TBD", "error-one", "TBD-web-201901-0001", "TBD", 1]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for API, one error and one correct, metadata.csv")

        # Test for the returned dataframe.
        actual_aip = [aip_df.columns.tolist()] + aip_df.values.tolist()
        expected_aip = [["Seed_ID", "AIP_ID"],
                        ["2529685", "magil-ggp-2529685-2019-01"],
                        ["error-one", "TBD-web-201901-0001"]]
        self.assertEqual(actual_aip, expected_aip, "Problem with test for API, one error and one correct, aip")

    def test_diff_collector(self):
        """
        Tests that the function creates the correct metadata.csv and returns the correct dataframe
        when the seed does not have one of the three expected_csv collector values.
        """
        aip_df = metadata_csv(["2141624"], "2019-01-02")

        # Test for the contents of metadata.csv
        actual_csv_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_csv = [actual_csv_df.columns.tolist()] + actual_csv_df.values.tolist()
        expected_csv = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                        ["TBD: unexpected collector value", "TBD: unexpected department value", 2141624,
                         "TBD: unexpected department value-web-201901-0001",
                         "TBD: could not get title from Archive-It", 1]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for different collector, metadata.csv")

        # Test for the returned dataframe.
        actual_aip = [aip_df.columns.tolist()] + aip_df.values.tolist()
        expected_aip = [["Seed_ID", "AIP_ID"],
                        ["2141624", "TBD: unexpected department value-web-201901-0001"]]
        self.assertEqual(actual_aip, expected_aip, "Problem with test for different collector, aip")

    def test_multiple_collection(self):
        """
        Tests that the function creates the correct metadata.csv and returns the correct dataframe
        when there is more than one seed from the same collection.
        """
        aip_df = metadata_csv(["2184360", "2184592", "2912234", "2912237", "2912238"], "2019-01-02")

        # Test for the contents of metadata.csv
        actual_csv_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_csv = [actual_csv_df.columns.tolist()] + actual_csv_df.values.tolist()
        expected_csv = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                        ["hargrett", "harg-0000", 2184360, "harg-0000-web-201901-0001", "UGA Today", 1],
                        ["hargrett", 'harg-0000', 2184592, "harg-0000-web-201901-0002", "UGA Twitter", 1],
                        ["russell", "rbrl-513", 2912234, "rbrl-513-web-201901-0001", "GALEO (@GALEOorg) / Twitter", 1],
                        ["russell", "rbrl-513", 2912237, "rbrl-513-web-201901-0002", "GALEO - YouTube", 1],
                        ["russell", "rbrl-513", 2912238, "rbrl-513-web-201901-0003", "GALEO | Facebook", 1]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for multiple seeds in a collection, metadata.csv")

        # Test for the returned dataframe.
        actual_aip = [aip_df.columns.tolist()] + aip_df.values.tolist()
        expected_aip = [["Seed_ID", "AIP_ID"],
                        ["2184360", "harg-0000-web-201901-0001"],
                        ["2184592", "harg-0000-web-201901-0002"],
                        ["2912234", "rbrl-513-web-201901-0001"],
                        ["2912237", "rbrl-513-web-201901-0002"],
                        ["2912238", "rbrl-513-web-201901-0003"]]
        self.assertEqual(actual_aip, expected_aip, "Problem with test for multiple seeds in a collection, aip")

    def test_no_collector_no_title(self):
        """
        Tests that the function creates the correct metadata.csv and returns the correct dataframe
        when the seed does not have a collector or title.
        Both are required fields, so the only seeds without one are lacking both. Otherwise, would test separate.
        """
        aip_df = metadata_csv(["2503951"], "2019-01-02")

        # Test for the contents of metadata.csv
        actual_csv_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_csv = [actual_csv_df.columns.tolist()] + actual_csv_df.values.tolist()
        expected_csv = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                        ["TBD: no collector in Archive-It", "TBD: unexpected department value", 2503951,
                         "TBD: unexpected department value-web-201901-0001",
                         "TBD: could not get title from Archive-It", 1]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for no collector or title, metadata.csv")

        # Test for the returned dataframe.
        actual_aip = [aip_df.columns.tolist()] + aip_df.values.tolist()
        expected_aip = [["Seed_ID", "AIP_ID"],
                        ["2503951", "TBD: unexpected department value-web-201901-0001"]]
        self.assertEqual(actual_aip, expected_aip, "Problem with test for no collector or title, aip")

    def test_no_relation(self):
        """
        Tests that the function creates the correct metadata.csv and returns the correct dataframe
        when the seeds do not have a Relation in Archive-It.
        """
        aip_df = metadata_csv(["2030942", "2527198", "2912227"], "2019-01-02")

        # Test for the contents of metadata.csv
        actual_csv_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_csv = [actual_csv_df.columns.tolist()] + actual_csv_df.values.tolist()
        expected_csv = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                        ["magil", "magil-0000", 2527198, "magil-ggp-2527198-2019-01", "Georgia Technology Authority", 1],
                        ["hargrett", "harg-0000", 2030942, "harg-0000-web-201901-0001", "UGA NAACP Twitter", 1],
                        ["russell", "rbrl-000", 2912227, "rbrl-000-web-201901-0001", "Rep. Carolyn Bourdeaux | Facebook", 1]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for no relation, metadata.csv")

        # Test for the returned dataframe.
        actual_aip = [aip_df.columns.tolist()] + aip_df.values.tolist()
        expected_aip = [["Seed_ID", "AIP_ID"],
                        ["2527198", "magil-ggp-2527198-2019-01"],
                        ["2030942", "harg-0000-web-201901-0001"],
                        ["2912227", "rbrl-000-web-201901-0001"]]
        self.assertEqual(actual_aip, expected_aip, "Problem with test for no relation, aip")

    def test_one_seed(self):
        """
        Tests that the function creates the correct metadata.csv and returns the correct dataframe
        when there is only one seed.
        """
        aip_df = metadata_csv(["2024247"], "2019-01-02")

        # Test for the contents of metadata.csv
        actual_csv_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_csv = [actual_csv_df.columns.tolist()] + actual_csv_df.values.tolist()
        expected_csv = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                        ["hargrett", "harg-0000", 2024247, "harg-0000-web-201901-0001", "Infusion Magazine website", 1]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for one seed, metadata.csv")

        # Test for the returned dataframe.
        actual_aip = [aip_df.columns.tolist()] + aip_df.values.tolist()
        expected_aip = [["Seed_ID", "AIP_ID"],
                        ["2024247", "harg-0000-web-201901-0001"]]
        self.assertEqual(actual_aip, expected_aip, "Problem with test for one seed, aip")

    def test_seed_report_all(self):
        """
        Tests that the function creates the correct metadata.csv and returns the correct dataframe
        when seeds have all possible values in the seed report (Collector, Related, Title).
        """
        aip_df = metadata_csv(["2018084", "2028466"], "2019-01-02")

        # Test for the contents of metadata.csv
        actual_csv_df = pd.read_csv(os.path.join(c.script_output, "preservation_download", "metadata.csv"))
        actual_csv = [actual_csv_df.columns.tolist()] + actual_csv_df.values.tolist()
        expected_csv = [["Department", "Collection", "Folder", "AIP_ID", "Title", "Version"],
                        ["russell", "rbrl-447", 2018084, "rbrl-447-web-201901-0001",
                         "K7MOA Legacy Voteview Website", 1],
                        ["russell", "rbrl-043", 2028466, "rbrl-043-web-201901-0001",
                         "Charlayne Hunter-Gault Twitter", 1]]
        self.assertEqual(actual_csv, expected_csv, "Problem with test for all fields, metadata.csv")

        # Test for the returned dataframe.
        actual_aip = [aip_df.columns.tolist()] + aip_df.values.tolist()
        expected_aip = [["Seed_ID", "AIP_ID"],
                        ["2018084", "rbrl-447-web-201901-0001"],
                        ["2028466", "rbrl-043-web-201901-0001"]]
        self.assertEqual(actual_aip, expected_aip, "Problem with test for all fields, aip")


if __name__ == '__main__':
    unittest.main()
