# Convert WARC information downloaded from WASAPI as XML into a CSV
# for analyzing WARC information separate from using API filters.

# usage: python /path/warc_xml_to_csv.py /path/warc.xml

import csv
import re
import sys
import xml.etree.ElementTree as et

# Get path to the xml file to be converted from the script argument.
try:
    warc_xml = sys.argv[1]
except IndexError:
    print("Script argument is missing")
    exit()

# Read the xml file.
try:
    tree = et.parse(warc_xml)
except FileNotFoundError:
    print("Provided path to the warc xml is not correct.")

# Start a csv for data
warc_csv = open("converted_warc_xml.csv", "w", newline="")
csv_writer = csv.writer(warc_csv)
csv_writer.writerow(["filename", "collection", "seed", "crawl-time", "crawl-start", "store-time"])

# Get the data for each WARC that is of interest from the xml file.
root = tree.getroot()
files = root.find("files")
for warc in files.findall("list-item"):
    filename = warc.find("filename").text
    collection = warc.find("collection").text
    crawl_time = warc.find("crawl-time").text
    crawl_start = warc.find("crawl-start").text
    store_time = warc.find("store-time").text

    # Adds a space before the dates so they keep their original formatting in Excel.
    crawl_time = " " + crawl_time
    crawl_start = " " + crawl_start
    store_time = " " + store_time

    # Calculates seed id, which is a portion of the WARC filename between "-SEED" and "-".
    try:
        regex_seed_id = re.match(r'^.*-SEED(\d+)-', filename)
        seed_id = regex_seed_id.group(1)
    except AttributeError:
        seed_id = "COULD NOT CALCULATE"

    # Save the data to a csv for further analysis.
    csv_writer.writerow([filename, collection, seed_id, crawl_time, crawl_start, store_time])

warc_csv.close()