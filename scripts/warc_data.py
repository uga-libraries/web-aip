# Purpose: gets data about warcs to include in this download from WASAPI for use by other scripts.

import requests
from web_variables import api, u, p, last_download


# Variable is used to detect if there was an error in the api call.
# Updated to "error" if an api call has a status that isn't 200.
# Causes website_preservation.py to quit since didn't get the needed data.

warc_status = ''


# ---------------------------------------------------------------------
# PART ONE: MAKES A LIST OF HARGRETT AND RUSSEL ARCHIVE-IT COLLECTIONS.
# Used to skip collections by other departments (e.g. BMAC or DLG).
# ---------------------------------------------------------------------

# Starts an empty list to store the collections.

collections = []


# Uses the Partner API to get the data from the seed table.
# Limit of -1 means will get data on all seeds, no matter how many there are.

seeds = requests.get(f'{api}/seed?limit=-1', auth=(u, p))


# If there was an error with the API call, update warc_status and quit script.

if not seeds.status_code == 200:
    warc_status = 'error'
    quit()


# Converts the seed data from json to a Python object.

py_seeds = seeds.json()


# Iterates over data about each seed.

for data in py_seeds:

    # Gets the department name from the Collector field.
    # If there is no information in the field, stops processing this seed and starts the next.

    try:
        dept = data['metadata']['Collector'][0]['value']

    except:
        continue


    # Checks if the department is Hargrett or Russell.

    if dept.startswith('Hargrett') or dept.startswith('Richard B. Russell'):

        # Adds the Archive-It collection id to the list if it isn't there already.

        if data['collection'] not in collections:
            collections.append(data['collection'])


# ---------------------------------------------
# PART TWO: GETS DATA ABOUT WARCS USING WASAPI
# ---------------------------------------------

# Get data about WARCs for inclusion in this download:
#     * Made since the last preservation download.
#     * Part of a Hargrett or Russell collection.

filters = {'crawl-start-after':last_download, 'collection':collections, 'page_size':100}

warcs = requests.get('https://warcs.archive-it.org/wasapi/v1/webdata', 
params=filters, auth=(u, p))


# If there was an error with the api call, update warc_status and quit script.

if not warcs.status_code == 200:
    warc_status = 'error'
    quit()


# Converts the warc data from json to a Python object.
# This will be imported into seed_data.py and website_preservation.py.

py_warcs = warcs.json()
