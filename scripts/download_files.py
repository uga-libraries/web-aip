"""
Download individual files from crawled websites using the URLs from the File Types lists.
Designed for PDF-only crawls, where publications need to be saved and cataloged separately.

Prior to running the script, download the File Type List for every file type and every crawl to include and
save the CSVs to a single folder. These files are the script input.

Future development ideas:
    * Save all URLs to a CSV for staff review to remove unwanted things prior to download?
    * In test, some showed as duplicate in the CSV, although is only in the CSV once. Skip because is in another crawl?
    * Expand beyond PDF? Need to change how renamed.
    * Need to be able to download from more than one collection at a time?
    * Want any logging, summary statistics about how many were downloaded, and/or showing progress?
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
    if os.path.isdir(input_csv):
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
    except AttributeError:
        print("Could not calculate seed from this URL and will not download:", url)

    # Makes a folder for the seed, if it does not already exist, for saving the PDF to.
    # TODO: Changes the current directory to this folder so the downloaded PDF is saved to it.
    if not os.path.exists(seed):
        os.makedirs(seed)

    # Calculates the desired name for the file. Generally, this is the last part of the URL plus .pdf.
    # If the last part of the URL is download, gets the previous part of the URL.
    # If the last part of the URL is pdf, removes that before adding .pdf extension.
    # TODO there may be other generic naming conventions to address as well
    if url.endswith("download"):
        regex = re.match("(.*)/download", url)
        filename = regex.group(1) + ".pdf"
    else:
        regex = re.match(".*/(.*)", url)
        if url.endswith(".pdf") or url.endswith(".PDF"):
            filename = regex.group(1)
        elif url.endswith("pdf") or url.endswith("PDF"):
            filename = regex.group(1)[:-3] + ".pdf"
        else:
            filename = regex.group(1) + ".pdf"

    # Calculates the URL in Archive-It by adding the Wayback URL, the collection, and 3 for the most recent capture.

    # Downloads the PDF to the seed's directory, named with the desired name.

