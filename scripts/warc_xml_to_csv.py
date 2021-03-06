# Convert WARC information downloaded from WASAPI as XML into a CSV for analyzing WARC information.

# Script input is downloaded from https://warcs.archive-it.org/wasapi/v1/webdata.
# Copy information from all result pages into a single document.

# Script usage: python /path/warc_xml_to_csv.py /path/warc.xml

import csv
import os
import re
import sys
import xml.etree.ElementTree as et


# Gets the path to the XML file to be converted from the script argument.
try:
    warc_xml = sys.argv[1]
except IndexError:
    print("Including the path to the XML file as a script argument.")
    print("Script usage: python /path/warc_xml_to_csv.py /path/warc.xml")
    exit()

# Reads the XML file.
try:
    tree = et.parse(warc_xml)
except FileNotFoundError:
    print("The path to the XML file is not correct:", warc_xml)
    print("Script usage: python /path/warc_xml_to_csv.py /path/warc.xml")
    exit()

# Starts a CSV file, with a header, for the WARC data in the same folder as the XML file.
warc_xml_folder = os.path.dirname(os.path.abspath(warc_xml))
warc_csv = open(os.path.join(warc_xml_folder, "converted_warc_xml.csv"), "w", newline="")
csv_writer = csv.writer(warc_csv)
csv_writer.writerow(["filename", "collection", "seed", "crawl-time", "crawl-start", "store-time"])

# Gets the data for each WARC from the XML file.
root = tree.getroot()
files = root.find("files")
for warc in files.findall("list-item"):
    filename = warc.find("filename").text
    collection = warc.find("collection").text
    crawl_time = warc.find("crawl-time").text
    crawl_start = warc.find("crawl-start").text
    store_time = warc.find("store-time").text

    # Adds a space before the dates so they keep their original formatting when the CSV is opened in Excel.
    crawl_time = " " + crawl_time
    crawl_start = " " + crawl_start
    store_time = " " + store_time

    # Gets the seed id, which is the numbers in the WARC filename between "-SEED" and "-".
    try:
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', filename)
        seed_id = regex_seed_id.group(1)
    except AttributeError:
        seed_id = "COULD NOT CALCULATE"

    # Saves the WARC data as a row in the CSV.
    csv_writer.writerow([filename, collection, seed_id, crawl_time, crawl_start, store_time])

warc_csv.close()
