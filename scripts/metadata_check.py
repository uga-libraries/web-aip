# Purpose: Generate reports of required collection and seed metadata fields to check for completeness prior to
# downloading the WARCs and metadata for preservation. Hargrett and Russell reports are saved separately.

# Usage: python /path/metadata_check.py /path/output_directory

import csv
import os
import requests
import sys

# Gets Archive-It account credentials from the configuration file.
import configuration as c


def get_metadata_value(data, field):
    """Looks up the value of a field in the Archive-It API output for a particular collection or seed. If the field
    is not in the output, returns the string NONE instead."""
    # TODO: if there is more than one value for the same metadata field, this only gets the value of one. See seed language for example.
    try:
        return data['metadata'][field][0]['value']
    except KeyError:
        return 'NONE'


# Makes the folder where the reports will be saved (provided by user) the current directory.
output_directory = sys.argv[1]
os.chdir(output_directory)


# PART ONE: COLLECTION REPORTS

# Gets the Archive-It collection report with data on all the collections.
collections = requests.get(f'{c.partner_api}/collection?limit=-1', auth=(c.username, c.password))

# If there was an error with the API call, quits the script.
if not collections.status_code == 200:
    print('Error with Archive-It API connection when getting collection report', collections.status_code)
    exit()

# Saves the collection data as a Python object.
py_collections = collections.json()

# Iterates over the metadata for each collection.
for coll_data in py_collections:

    # Constructs the URL of the collection's metadata page to make it easy to edit a record.
    collection_metadata_page = f"{c.inst_page}/collections/{coll_data['id']}/metadata"

    # Gets the values of the required metadata fields (or 'NONE' if the field has no metadata).
    coll_collector = get_metadata_value(coll_data, 'Collector')
    coll_date = get_metadata_value(coll_data, 'Date')
    coll_description = get_metadata_value(coll_data, 'Description')
    coll_title = get_metadata_value(coll_data, 'Title')

    # If this is the first collection for this department, makes a CSV with a header row for collection metadata.
    if not os.path.exists(f'{coll_collector}_collections_metadata.csv'):
        with open(f'{coll_collector}_collections_metadata.csv', 'w', newline='') as output:
            collection_header = ['Collection ID', 'Collection Name', 'Collector', 'Date', 'Description', 'Title',
                                 'Collection Page']
            header_write = csv.writer(output)
            header_write.writerow(collection_header)

    # Adds the collection metadata as a row to the department's CSV.
    with open(f'{coll_collector}_collections_metadata.csv', 'a', newline='') as output:
        row_write = csv.writer(output)
        row_write.writerow([coll_data['id'], coll_data['name'], coll_collector, coll_date, coll_description, coll_title,
                            collection_metadata_page])


# PART TWO: SEED REPORTS

# Gets the Archive-It seed report with data on all the seeds.
seeds = requests.get(f'{c.partner_api}/seed?limit=-1', auth=(c.username, c.password))

# If there was an error with the API call, quits the script.
if not seeds.status_code == 200:
    print('Error with Archive-It API connection when getting seed report', seeds.status_code)
    exit()

# Saves the seed data as a Python object.
py_seeds = seeds.json()

# Iterates over the metadata for each seed.
for seed_data in py_seeds:

    # Constructs the URL of the seed's metadata page to make it easy to edit a record.
    seed_metadata_page = f"{c.inst_page}/collections/{seed_data['collection']}/seeds/{seed_data['id']}/metadata"

    # Gets the values of the required metadata fields (or 'NONE' if the field has no metadata).
    collector = get_metadata_value(seed_data, 'Collector')
    creator = get_metadata_value(seed_data, 'Creator')
    date = get_metadata_value(seed_data, 'Date')
    identifier = get_metadata_value(seed_data, 'Identifier')
    language = get_metadata_value(seed_data, 'Language')
    rights = get_metadata_value(seed_data, 'Rights')
    title = get_metadata_value(seed_data, 'Title')

    # If this is the first seed for this department, makes a CSV with a header row for seed metadata.
    if not os.path.exists(f'{collector}_seeds_metadata.csv'):
        with open(f'{collector}_seeds_metadata.csv', 'w', newline='') as output:
            seed_header = ['Seed ID', 'URL', 'Collector', 'Creator', 'Date', 'Identifier', 'Language', 'Rights',
                           'Title', 'Metadata Page']
            header_write = csv.writer(output)
            header_write.writerow(seed_header)

    # Adds the seed metadata as a row to the department's CSV.
    with open(f'{collector}_seeds_metadata.csv', 'a', newline='') as output:
        row_write = csv.writer(output)
        row_write.writerow([seed_data['id'], seed_data['url'], collector, creator, date, identifier, language, rights,
                            title, seed_metadata_page])
