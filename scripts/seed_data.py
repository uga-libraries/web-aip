# Purpose: extract information from the seed metadata to define the aip id, aip title, and crawl definition id and saves them in a dictionary for use by other scripts.

import datetime
import re
import requests
from warc_data import py_warcs
from web_variables import api, u, p


# Variable is used to detect if there was an error in the api call.
# Updated to "error" if an api call has a status that isn't 200.
# Causes website_preservation.py to quit since didn't get the needed data.

seed_status = ''


# Starts a dictionary for storing the number of seeds per collection.
# Used to generate a sequential number for the aip id.

seed_count = {}


# Starts a dictionary for storing information that other scripts will use.
# The key is the seed id and the value is a list with the aip id, aip title, and crawl def id.

seed_data = {}


# The date of the current download (today's date), formatted YYYYMM.
# Used in the aip id to differentiate the aip from other downloads of the same seed.

year = datetime.date.today().year
month = format(datetime.date.today().month, '02d')
download = str(year) + str(month)


# Iterates through data for each warc.
# Data was saved to a python object py_warcs in warc_data.py

for data in py_warcs['files']:

    # Calculates the seed id, which is a portion of the warc filename.
    # Stops processing this warc and starts the next if the filename doesn't match expected pattern.

    try:
        regex = re.match('^.*-SEED(\d+)-', data['filename'])
        seed_id = regex.group(1)

    except:
        print(f"\nCannot calculate seed id for {data['filename']}")
        print('It will not be included in this preservation download.')
        continue


    # Stops processing this warc and starts the next if have already assigned an aip id to that seed.
    # Happens when a seed has multiple warcs.

    if seed_id in seed_data:
        continue


    # Uses the Partner API to get data about the seed.

    seed_data = requests.get(f'{api}/seed?id={seed_id}', auth=(u, p))


    # If there was an error with the api call, updates seed_status and quits this script.

    if not seed_data.status_code == 200:
        seed_status = 'error'
        quit()


    # Converts the seed data from json to a Python object.

    py_seed_data = seed_data.json()


    # Constructs the aip id from the seed data.

    for item in py_seed_data:

        # Gets the title for the seed from the Title field.
        # Stops processing this seed and starts the next if the title is missing. It is required.

        try:
            aip_title = item['metadata']['Title'][0]['value']

        except:
            print(f"\nTitle missing for seed {item['id']}, {item['url']}")
            continue


        # Gets the department from the Collector field.
        # Gets the collection number from the Relation field if present (field is optional).
        #     The pattern for the collection number depends on the department.
        # Stops processing this seed and starts the next if there is no department. It is required.

        try:
            department = item['metadata']['Collector'][0]['value']
        except:
            continue
 
        # Hargrett department and collection number.
        # Relation should be formatted Hargrett collection-number: Title.
        # Assigns collection number 0000 if no relation is present.

        if department.startswith('Hargrett'):
            dept_code = 'harg'
            try:
                regex = re.match('^Hargrett (.*):', item['metadata']['Relation'][0]['value'])
                related_collection = regex.group(1)
            except:
                related_collection = '0000'

        # Russell department and collection number.
        # Relation should be formatted RBRL/collection-id: Title. 
        # Even if the collection id includes letters as well, only get the three numbers.
        # Assigns collection number 000 if no relation is present.

        elif department.startswith('Richard B. Russell'):
            dept_code = 'rbrl'
            try:
                regex = re.match('^RBRL\/(\d{3})', item['metadata']['Relation'][0]['value'])
                related_collection = regex.group(1)
            except:
                related_collection = '000'


        # Stops processing this seed and starts the next if the department isn't Hargrett or Russell.
        # Others use the UGA Archive-It account but not this preservation workflow.

        else:
            continue
            

        # Updates the count for this collection in the seed_count dictionary.
        # If the collection isn't there, adds it with a count of 0 and immediately adds 1.
        # If the collection is there, adds one to the count.

        seed_count[related_collection] = seed_count.get(related_collection, 0) + 1


        # Constructs the aip id for the seed.
        # Includes formatting the sequential number (seed count) to four digits.

        sequential = format(seed_count[related_collection], '04d')
        aip_id = f"{dept_code}-{related_collection}-web-{download}-{sequential}"


        # Saves aip id, aip title, and crawl definition id to the seed_data dictionary.
        # This only contains information about seeds that had no errors and were fully processed.
        # This dictionary will be imported into website_preservation.py.

        seed_data[seed_id] = [aip_id, aip_title, item['crawl_definition']]

