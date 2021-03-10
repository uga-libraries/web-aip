"""
Download individual files from crawled websites using the URLs from the File Types lists.
Designed for PDF-only crawls, where publications need to be saved and cataloged separately.

Prior to running the script, download the File Type List for every file type and every crawl to include and
save the CSVs to a single folder. These files are the script input.

Future development idea: want to be able to review the URLS and remove unwanted things prior to download?
Could combine the URLs to a single CSV for staff review and do the download from that CSV.
"""

# Usage: python /path/download_files.py /path/input_folder

import csv
import os
import sys

# Gets the path to the input folder and makes it the current directory.
# If the path is missing or not valid, prints and error and quits the script.

# Gets the document URLs from each CSV in the input folder and saves them to a list.

# Downloads the document for each URL in the list and saves it to a folder named with the seed URL.

