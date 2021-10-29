"""
Download individual PDFs from crawled websites saved in Archive-It using the URLs from the File Types lists.
Designed for PDF-only crawls, where publications need to be saved and cataloged separately.

Prior to running the script, download the File Type List for PDFs from Archive-It for every crawl to include and
save the CSVs to a single folder. These files are the script input.

Dependency: wget https://www.gnu.org/software/wget/

Future development ideas:
    * Save all URLs to a CSV for staff review to remove unwanted things prior to download?
    * Adapt script for other formats besides PDF? Just need to change how files are renamed.
    * Need to be able to download from more than one collection at once?
    * Want any logging, summary statistics about how many were downloaded, and/or showing script progress?
"""

# Usage: python /path/download_files.py /path/input_directory archiveit_collection_id

# WARNING: THIS SCRIPT IS A PROOF OF CONCEPT AND HAS BEEN MINIMALLY TESTED

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
try:
    collection = sys.argv[2]
except IndexError:
    print("The required argument Archive-It collection id is missing.")
    print("Script usage: python /path/download_files.py /path/input_directory archiveit_collection_id")
    exit()

# Gets the PDF URLs from each CSV in the input folder and saves them to a dictionary.
# The dictionary keys are the seeds and values are a list of URLS for each seed.
download_urls = {}
for input_csv in os.listdir("."):
    with open(input_csv) as csvfile:
        data = csv.reader(csvfile)

        # Verifies the header has the expected values. If not, prints an error and starts the next CSV.
        header = next(data)
        if not header == ["url", "size", "is_duplicate", "seed"]:
            print("This file is not formatted correctly and will be skipped:", input_csv)
            continue

        # Adds the seed to the download_urls dictionary if it is not already present.
        # Adds each URL to the download_urls dictionary if it is not a duplicate in Archive-It (value of 1 in the CSV).
        for row in data:
            url, size, is_duplicate, seed = row
            if is_duplicate == "1":
                continue
            if seed in download_urls:
                download_urls[seed].append(url)
            else:
                download_urls[seed] = [url]

# For each seed, downloads the PDF for each URL in the list and saves it to a folder named with the seed.
for seed in download_urls.keys():

    # Makes a version of the seed URL which can be used for a folder name.
    # Removes http:// or https:// from the beginning if present, / from the end if present, and replaces other / with _
    # TODO: try again with regular expressions. I wasn't getting the optional / at the end to work right the first try.
    # TODO: are there other characters which could be a problem?
    seed_directory_name = seed
    if seed_directory_name.startswith("http://"):
        seed_directory_name = seed.replace("http://", "")
    elif seed_directory_name.startswith("https://"):
        seed_directory_name = seed.replace("https://", "")
    if seed_directory_name.endswith("/"):
        seed_directory_name = seed_directory_name[:-1]
    seed_directory_name = seed_directory_name.replace("/", "_")

    # Makes a folder for the seed and makes it the current directory so wget can save the PDFs there.
    # TODO: add error handling for if the name has illegal characters.
    os.makedirs(os.path.join(input_directory, seed_directory_name))
    os.chdir(os.path.join(input_directory, seed_directory_name))

    # Saves the PDF, with the desired file name, to the seed folder.
    for url in download_urls[seed]:

        # Makes the desired name for the file. Generally, this is the last part of the URL plus ".pdf".
        # If the last part of the URL is download, gets the previous part of the URL instead.
        # TODO there may be other generic naming conventions to address as well.
        if url.endswith("download"):
            regex = re.match("(.*)/download", url)
            filename = regex.group(1) + ".pdf"
        else:
            regex = re.match(".*/(.*)", url)
            # If the last part of the URL already has a pdf extension, does not add ".pdf" to it.
            if url.endswith(".pdf") or url.endswith(".PDF"):
                filename = regex.group(1)
            # If the last part of the URL is pdf without the period, removes the "pdf" before adding the .pdf extension.
            elif url.endswith("pdf") or url.endswith("PDF"):
                filename = regex.group(1)[:-3] + ".pdf"
            else:
                filename = regex.group(1) + ".pdf"

        # Makes the URL in Archive-It by adding the Wayback URL, the collection id, and 3 for the most recent capture.
        # TODO: this may only work on Windows because of the direction of the slashes.
        # Can't use os.path.join because url already has slashes in it.
        archiveit_url = f"https://wayback.archive-it.org/{collection}/3/{url}"

        # Saves the PDF to the seed's directory, named with the desired name.
        # TODO: Can end up with more than one different file that has the same name, and only one is saved.
        #   wget non-clobber will not download if already present, rather than download with a rename.
        # TODO: prints a lot to the terminal. Make it quiet? Or like to see the script progress?
        # TODO: error handling
        subprocess.run(f'wget -O "{filename}" "{archiveit_url}"', shell=True)
