"""
Download individual files from crawled websites using the URLs from the File Types lists.
Designed for PDF-only crawls, where publications need to be saved and cataloged separately.

Prior to running the script, download the File Type List for every file type and every crawl to include and
save the CSVs to a single folder. These files are the script input.

Dependency: wget https://www.gnu.org/software/wget/

Future development ideas:
    * Save all URLs to a CSV for staff review to remove unwanted things prior to download?
    * Expand beyond PDF? Need to change how renamed.
    * Need to be able to download from more than one collection at a time?
    * Want any logging, summary statistics about how many were downloaded, and/or showing progress?
"""

# Usage: python /path/download_files.py /path/input_directory archiveit_collection_id

import csv
import os
import re
import subprocess
import sys

# Gets the path to the input folder from the script argument and makes it the current directory.
# If the path is missing or not valid, prints an error and quits the script.
try:
    input_directory = sys.argv[1]
    os.chdir(input_directory)
except (IndexError, FileNotFoundError, NotADirectoryError):
    print("The required argument input_directory is missing or not a valid directory.")
    print("Script usage: python /path/download_files.py /path/input_directory archiveit_collection_id")
    exit()

# Gets the Archive-It collection id from the script argument, which is used for making the Archive-It URL.
# If the id is missing, prints an error and quits the script.
# TODO: could also check if the id is a number.
try:
    collection = sys.argv[2]
except IndexError:
    print("The required argument Archive-It collection id is missing.")
    print("Script usage: python /path/download_files.py /path/input_directory archiveit_collection_id")
    exit()

# Gets the document URLs from each CSV in the input folder and saves them to a dictionary.
# The dictionary keys are the seeds and values are lists of URLS for that seeds.
download_urls = {}
for input_csv in os.listdir("."):

    # TODO: this is for testing only. Remove when done.
    if os.path.isdir(input_csv):
        continue

    # Reads each row in the CSV file.
    with open(input_csv) as csvfile:
        data = csv.reader(csvfile)

        # Verify the headers have the expected values. If not, prints an error and starts the next CSV.
        header = next(data)
        if not header == ["url", "size", "is_duplicate", "seed"]:
            print("This file is not formatted correctly and will be skipped:", input_csv)
            continue

        # Add each URL to the download_urls dictionary if it is not a duplicate in Archive-It or the dictionary.
        for row in data:

            # If the URL is a duplicate from Archive-It, skip it and start the next URL.
            if row[2] == "1":
                continue

            # Add the seed to download_urls dictionary if not already there, with this URL as its first value.
            # Otherwise, adds the URL to the download_urls dictionary list for its seed.
            # TODO: is it possible for one url to be repeated but be for different documents?
            if not row[3] in download_urls:
                download_urls[row[3]] = [row[0]]
            else:
                download_urls[row[3]].append(row[0])


# Downloads the document for each URL in the list and saves it to a folder named with the seed.
# TODO: Iterate over seeds in the dictionary before iterate over the urls
for url in download_urls:

    # Calculates the seed from the url, which is the first part of the path minus the http:// or https://.
    # If the seed cannot be calculated, prints an error and does not try to download this url.
    # TODO: seed is from dictionary
    try:
        regex = re.match("^https?://(.*?)/", url)
        seed = regex.group(1)
    except AttributeError:
        print("Could not calculate seed from this URL and will not download:", url)
        continue

    # Makes a folder for the seed, if it does not already exist, for saving the PDF to.
    # Changes the current directory to the seed folder so the downloaded PDF is saved to it.
    # TODO: do this once per seed, not once per url
    if not os.path.exists(seed):
        os.makedirs(seed)
    os.chdir(os.path.join(input_directory, seed))

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
    # TODO: this may only work on Windows because of the direction of the slashes.
    # Can't use os.path.join because url already has slashes in it.
    archiveit_url = f"https://wayback.archive-it.org/{collection}/3/{url}"

    # Downloads the PDF to the seed's directory, named with the desired name.
    subprocess.run(f'wget -O "{filename}" "{archiveit_url}"', shell=True)

