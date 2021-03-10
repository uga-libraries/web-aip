"""
Download individual files from crawled websites using the URLs from the File Types lists.
Designed for PDF-only crawls, where publications need to be saved and cataloged separately.

Prior to running the script, download the File Type List for every file type and every crawl to include and
save the CSVs to a single folder. These files are the script input.

Future development ideas:
    * Save all URLs to a CSV for staff review to remove unwanted things prior to download?
    * In test, some showed as duplicate in the CSV, although is only in the CSV once. Skip because is in another crawl?
"""

# Usage: python /path/download_files.py /path/input_directory

import csv
import os
import re
import sys

# Gets the path to the input folder and makes it the current directory.
# If the path is missing or not valid, prints and error and quits the script.
try:
    input_directory = sys.argv[1]
    os.chdir(input_directory)
except (IndexError, FileNotFoundError, NotADirectoryError):
    print("The required argument input_directory is missing or not a valid directory.")
    print("Script usage: python /path/download_files.py /path/input_directory")
    exit()


# Gets the document URLs from each CSV in the input folder and saves them to a list.
download_urls = []
for input_csv in os.listdir("."):

    # TODO: this is for testing only. Remove when done.
    if input_csv == "skip":
        continue

    # Reads each row in the CSV file.
    with open(input_csv) as csvfile:
        data = csv.reader(csvfile)

        # Verify the first column name in the first row is "url".
        # If not, this CSV is not formatted as expected. Prints an error and starts the next CSV.
        header = next(data)
        if not header[0] == "url":
            print("This file is not formatted correctly and will be skipped:", input_csv)
            break

        # Gets the URL, which is the first item in the row, and saves to the URLS list.
        for row in data:
            download_urls.append(row[0])


# Downloads the document for each URL in the list and saves it to a folder named with the seed.
for url in download_urls:

    # Calculates the seed from the url, which is the first part of the path minus the http:// or https://.
    # If the seed cannot be calculated, prints an error and does not try to download this url.
    try:
        regex = re.match("^https?://(.*?)/", url)
        seed = regex.group(1)
        print(seed)
    except AttributeError:
        print("Could not calculate seed from this URL and will not download:", url)

    # Makes a folder for the seed, if it does not already exist, for saving the file to.
    # TODO: test if the destination folder needs to be made for wget to work.
    if not os.path.exists(seed):
        os.makedirs(seed)
