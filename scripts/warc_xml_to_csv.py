"""Convert WARC information downloaded from WASAPI as XML into a CSV for analyzing WARC information.
The resulting CSV is saved in the same folder as the input XML.

To make the script input (warc.xml):
    1. Log into your Archive-It account.
    2. Downloaded XML from https://warcs.archive-it.org/wasapi/v1/webdata (under "GET").
    3. Copy the <root> section to a plain text document and click back on your web browser.
    4. If there is a URL under "next", go to the next page.
    5. Get the XML for that page and copy all <list-item> elements to the end of the <files> section.
    6. Repeat 4-5 until next is null. Save the plain text document as an XML file."""

# Script usage: python path\\warc_xml_to_csv.py path\\warc.xml

import csv
import os
import re
import requests
import sys
import xml.etree.ElementTree as et

import configuration as c


def get_title(seed):
    """Uses the Partner API to get the seed report for this seed, which includes the seed title.
    Returns the title or an error message to put in the CSV in place of the title."""

    # Gets the seed report using the Partner API.
    seed_report = requests.get(f'{c.partner_api}/seed?id={seed}', auth=(c.username, c.password))
    if not seed_report.status_code == 200:
        return "API Error for seed report"

    # Reads the seed report and extracts the title.
    py_seed_report = seed_report.json()
    try:
        seed_title = py_seed_report[0]["metadata"]["Title"][0]["value"]
        return seed_title
    except (KeyError, IndexError):
        return "No title in Archive-It"


# Gets the path to the XML file to be converted from the script argument.
try:
    WARC_XML = sys.argv[1]
except IndexError:
    print("Including the path to the XML file as a script argument.")
    print("Script usage: python path\\warc_xml_to_csv.py path\\warc.xml")
    sys.exit()

# Reads the XML file.
try:
    TREE = et.parse(WARC_XML)
except FileNotFoundError:
    print("The path to the XML file is not correct:", WARC_XML)
    print("Script usage: python path\\warc_xml_to_csv.py path\\warc.xml")
    sys.exit()

# Starts a CSV file, with a header, for the WARC data in the same folder as the XML file.
WARC_XML_FOLDER = os.path.dirname(os.path.abspath(WARC_XML))
WARC_CSV = open(os.path.join(WARC_XML_FOLDER, "converted_warc_xml.csv"), "w", newline="")
CSV_WRITER = csv.writer(WARC_CSV)
CSV_WRITER.writerow(["WARC Filename", "AIT Collection", "Seed", "Job", "Date (store-time",
                     "Size (GB)", "Crawl Def", "AIP", "AIP Title"])

# Gets the data for each WARC from the XML file.
ROOT = TREE.getroot()
FILES = ROOT.find("files")
for warc in FILES.findall("list-item"):
    filename = warc.find("filename").text
    size = warc.find("size").text
    collection = warc.find("collection").text
    job_id = warc.find("crawl").text
    store_time = warc.find("store-time").text

    # Adds a space before the date to keep the original formatting if the file is opened in Excel.
    store_time = " " + store_time

    # Gets the seed id from the WARC filename.
    try:
        regex = re.match(r'^.*?SEED(\d+)-', filename)
        seed_id = regex.group(1)
    except AttributeError:
        seed_id = "COULD NOT CALCULATE"

    # Converts the size from bytes to GB, rounded to 2 decimal places.
    size = round(float(size) / 1000000000, 2)

    # Uses the seed id and the Partner API to get the AIP title.
    title = get_title(seed_id)

    # Saves the WARC data as a row in the CSV.
    CSV_WRITER.writerow([filename, collection, seed_id, job_id, store_time, size, "Crawl Def", "", title])

WARC_CSV.close()
