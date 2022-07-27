"""Purpose: This script tests the functionality of the seed_data function in web_functions.py, including
error handling and data calculations. It uses a local copy of the seed_data function to be able to generate errors.

This is a separate test from the rest of the workflow for making AIPs to make it easier to create the variations
and develop this function, which is not dependent on the data from the previous part of the script.

Usage: python path/test_seed_data.py
"""
import os
import pandas as pd
import re
import requests
import configuration as c
import web_functions as web


def seed_data(date_start, date_end):
    """Uses WASAPI and the Partner API to get information about each seed to include in the download.
    Returns the data as a dataframe and also saves it to a CSV in the script output folder to use for the log
    and for splitting big downloads or restarting jobs if the script breaks."""

    # Starts a dataframe for storing seed level data about the WARCs in this download.
    # Includes columns that will be used for logging steps prior to using the general-aip script functions.
    seed_df = pd.DataFrame(columns=["Seed_ID", "AIP_ID", "Title", "Department", "UGA_Collection", "AIT_Collection",
                                    "Job_ID", "Size_GB", "WARCs", "WARC_Filenames", "Seed_Metadata_Errors",
                                    "Metadata_Report_Errors", "Metadata_Report_Info",
                                    "WARC_API_Errors", "WARC_Fixity_Errors", "WARC_Unzip_Errors"])

    # Uses WASAPI to get information about all WARCs in this download, based on the date limits.
    # WASAPI is the only API that allows limiting by date.
    filters = {"store-time-after": date_start, "store-time-before": date_end, "page_size": 1000}
    warcs = requests.get(c.wasapi, params=filters, auth=(c.username, c.password))

    # If there was an error with the API call, quits the script.
    if not warcs.status_code == 200:
        print(f"\nAPI error {warcs.status_code} when getting WARC data.")
        print(f"Ending script (this information is required). Try script again later.")
        exit()

    # Converts the WARC data from json to a Python object and organizes it in the dataframe by seed.
    # Includes seed, Archive-It collection, job, size in GB, number of WARCs, and list of WARC filenames.
    py_warcs = warcs.json()
    for warc_info in py_warcs["files"]:

        # Calculates the seed id, which is a portion of the WARC filename.
        regex_seed = re.match(r"^.*-SEED(\d+)-", warc_info["filename"])
        seed_identifier = regex_seed.group(1)

        # If the seed is already in the dataframe, adds to the size, WARC count, and WARC filenames.
        # If the seed is new, gets the data needed about the seed and adds it to the dataframe.
        if seed_identifier in seed_df.Seed_ID.values:
            seed_df.loc[seed_df.Seed_ID == seed_identifier, "Size_GB"] += round(warc_info["size"]/1000000000, 2)
            seed_df.loc[seed_df.Seed_ID == seed_identifier, "WARCs"] += 1
            seed_df.loc[seed_df.Seed_ID == seed_identifier, "WARC_Filenames"] += f',{warc_info["filename"]}'
        else:
            seed_info = {"Seed_ID": seed_identifier, "AIT_Collection": warc_info["collection"],
                         "Job_ID": warc_info["crawl"], "Size_GB": round(warc_info["size"]/1000000000, 2),
                         "WARCs": 1, "WARC_Filenames": warc_info["filename"]}
            seed_df = seed_df.append(seed_info, ignore_index=True)

    # Gets a list of the seeds and uses the Partner API (seed report) to get additional information.
    # Includes: seed title, related archival collection, and department (all part of the metadata)
    # and constructs the AIP ID. Adds these fields to the dataframe.
    seed_list = seed_df["Seed_ID"].to_list()
    for seed in seed_list:

        # If any data is missing, the try/except notes the error and the seed will be skipped when making AIPs.
        try:
            # Row index is used to save the data to the correct row in the dataframe.
            row_index = seed_df.index[seed_df["Seed_ID"] == seed].tolist()[0]

            # Gets the seed metadata from the Archive-It Partner API and logs an error if the connection fails.
            seed_report = requests.get(f"{c.partner_api}/seed?id={seed}", auth=(c.username, c.password))
            if not seed_report.status_code == 200:
                web.log(f"API error {seed_report.status_code}: can't make this AIP", seed_df, row_index, "Seed_Metadata_Errors")
                continue
            py_seed_report = seed_report.json()

            # Gets the AIP title from the seed metadata.
            seed_df.loc[row_index, "Title"] = py_seed_report[0]["metadata"]["Title"][0]["value"]

            # Gets the department name from the seed metadata and translates it to the ARCHive group code.
            dept_to_group = {"Hargrett Rare Book & Manuscript Library": "hargrett",
                             "Map and Government Information Library": "magil",
                             "Richard B. Russell Library for Political Research and Studies": "russell"}
            department = dept_to_group[py_seed_report[0]["metadata"]["Collector"][0]["value"]]
            seed_df.loc[row_index, "Department"] = department

            # Gets the related archival collection from the seed metadata, reformatting if necessary.
            if department == "hargrett":
                try:
                    regex_collection = re.match("^Hargrett (.*):", py_seed_report[0]["metadata"]["Relation"][0]["value"])
                    seed_df.loc[row_index, "UGA_Collection"] = regex_collection.group(1)
                except (KeyError, AttributeError):
                    seed_df.loc[row_index, "UGA_Collection"] = "0000"
            elif department == "magil":
                seed_df.loc[row_index, "UGA_Collection"] = "0000"
            elif department == "russell":
                try:
                    regex_collection = re.match(r"^RBRL/(\d{3})", py_seed_report[0]["metadata"]["Relation"][0]["value"])
                    seed_df.loc[row_index, "UGA_Collection"] = "rbrl-" + regex_collection.group(1)
                except (KeyError, AttributeError):
                    seed_df.loc[row_index, "UGA_Collection"] = "rbrl-000"

            # Constructs the AIP ID according to department specifications.
            # Also temporarily adds a column to the dataframe which counts the number of seeds per collection,
            # and calculates the year and month of the download, both of which are needed for AIP IDs.
            seed_df["coll_sequence"] = seed_df.groupby(["UGA_Collection"]).cumcount() + 1
            year, month, day = date_end.split("-")
            if department == "hargrett":
                seed_df.loc[row_index, "AIP_ID"] = f'harg-{seed_df.at[row_index, "UGA_Collection"]}-web-{year}{month}-{format(seed_df.at[row_index, "coll_sequence"], "04d")}'
            elif department == "magil":
                seed_df.loc[row_index, "AIP_ID"] = f"magil-ggp-{seed}-{year}-{month}"
            elif department == "russell":
                seed_df.loc[row_index, "AIP_ID"] = f'{seed_df.at[row_index, "UGA_Collection"]}-web-{year}{month}-{format(seed_df.at[row_index, "coll_sequence"], "04d")}'
            seed_df.drop(["coll_sequence"], axis=1, inplace=True)
        except:
            seed_df.loc[row_index, "Seed_Metadata_Errors"] = "Couldn't get all required metadata values from the seed report. Will not download files or make AIP."

    # If there were no errors (the row has no value in the Seed_Metadata_Errors column),
    # updates the dataframe to show success.
    seed_df["Seed_Metadata_Errors"] = seed_df["Seed_Metadata_Errors"].fillna("Successfully calculated seed metadata")

    # Saves the dataframe as a CSV in the script output folder for splitting or restarting a batch.
    # Returns the dataframe for when the entire group will be downloaded as one batch.
    seed_df.to_csv(os.path.join(c.script_output, "seeds.csv"), index=False)
    return seed_df


# The start and end dates that the test script requires to give predictable results.
# In the actual script, these are arguments.
date_start = "2022-03-20"
date_end = "2022-03-25"

# Runs the function being tested by this script.
# Produces a dataframe with metadata about each seed to include in the batch.
seed_df = seed_data(date_start, date_end)
