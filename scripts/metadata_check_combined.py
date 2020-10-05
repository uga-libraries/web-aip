# Purpose: Generate reports of required collection and seed metadata fields to check for completeness prior to
# downloading the WARCs and metadata for preservation. Information for all departments is saved in one report.

# Usage: python /path/metadata_check_combined.py /path/output_directory

import csv
import os
import requests
import sys

# Gets Archive-It account credentials from the configuration file.
import configuration as c


def get_metadata_value(data, field):
    """Looks up the value(s) of a field in the Archive-It API output for a particular collection or seed. If the
    field is not in the output, returns the string MISSING instead. """

    try:
        # Makes a list of all the values for that data field. Some fields may repeat, e.g. language.
        values_list = []
        for value in data['metadata'][field]:
            values_list.append(value['value'])

        # Converts the list to a string.
        # When there are multiple values in the list, each value is separated by a semicolon.
        values = '; '.join(values_list)
        return values

    except KeyError:
        return 'MISSING'


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

# Makes a CSV to save results to, including adding header rows.
with open('collections_metadata.csv', 'w', newline='') as output:
    collection_header = ['Collection ID', 'Collection Name', 'Collector', 'Date', 'Description', 'Title', 'Collection Page']
    write = csv.writer(output)
    write.writerow(collection_header)

    # Iterates over the metadata for each collection.
    for coll_data in py_collections:

        # Constructs the URL of the collection's metadata page to make it easy to edit a record.
        collection_metadata_page = f"{c.inst_page}/collections/{coll_data['id']}/metadata"

        # Gets the values of the required metadata fields (or 'NONE' if the field has no metadata).
        coll_collector = get_metadata_value(coll_data, 'Collector')
        coll_date = get_metadata_value(coll_data, 'Date')
        coll_description = get_metadata_value(coll_data, 'Description')
        coll_title = get_metadata_value(coll_data, 'Title')

        # Adds a row to the CSV with the metadata for the collection.
        write.writerow([coll_data['id'], coll_data['name'], coll_collector, coll_date, coll_description, coll_title,
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

# Makes CSVs (one per department) to save results to, including adding header rows.
with open('seeds_metadata.csv', 'w', newline='') as output:
    seed_header = ['Seed ID', 'URL', 'Collector', 'Creator', 'Date', 'Identifier', 'Language', 'Rights', 'Title', 'Metadata Page']
    write = csv.writer(output)
    write.writerow(seed_header)

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

        # Adds a row to the appropriate CSV with the metadata for the seed.
        write.writerow([seed_data['id'], seed_data['url'], collector, creator, date, identifier, language, rights,
                        title, seed_metadata_page])
