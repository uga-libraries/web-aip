# Purpose: Generate reports of required collection and seed metadata fields to check for completeness prior to
# downloading the WARCs and metadata for preservation. Separate reports are made for each department (collector) so
# the results can be shared with the responsible archivist.

# Usage: python /path/metadata_check_department.py [/path/output_directory]
# If the path for the output is not provided, the script uses the script output path from the configuration file.

# Ideas for improvement:
# add limit by date to see just this download;
# differentiate between active and inactive collections.

import csv
import os
import requests
import sys

# Gets Archive-It account credentials and the script output path from the configuration file.
import configuration as c


def get_metadata_value(data, field):
    """Looks up the value(s) of a field in the Archive-It API output for a particular collection or seed. If the
    field is not in the output, returns the string MISSING instead."""

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


# Gets the folder where the metadata reports should be saved.
# The path may be provided by the user with a script argument or may come from the configuration file.
try:
    output_directory = sys.argv[1]
except IndexError:
    output_directory = c.script_output

# Changes the current directory to the folder where the reports will be saved.
# If this cannot be done, prints an error for the user and quits the script.
try:
    os.chdir(output_directory)
except (FileNotFoundError, NotADirectoryError):
    print(f'The output directory "{output_directory}" is not a valid directory. Please try again.')
    print('Script usage: python /path/metadata_check_department.py [/path/output_directory]')
    exit()


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

    # Constructs the URL of the collection's metadata page. Included in the report to make it easy to edit a record.
    collection_metadata_page = f"{c.inst_page}/collections/{coll_data['id']}/metadata"

    # Gets the values of the required metadata fields (or 'MISSING' if the field has no metadata).
    coll_collector = get_metadata_value(coll_data, 'Collector')
    coll_date = get_metadata_value(coll_data, 'Date')
    coll_description = get_metadata_value(coll_data, 'Description')
    coll_title = get_metadata_value(coll_data, 'Title')

    # If this is the first collection for this department, makes a CSV with a header row for the collection metadata.
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

    # Constructs the URL of the seed's metadata page. Included in the report to make it easy to edit a record.
    seed_metadata_page = f"{c.inst_page}/collections/{seed_data['collection']}/seeds/{seed_data['id']}/metadata"

    # Gets the values of the required metadata fields (or 'MISSING' if the field has no metadata).
    collector = get_metadata_value(seed_data, 'Collector')
    creator = get_metadata_value(seed_data, 'Creator')
    date = get_metadata_value(seed_data, 'Date')
    identifier = get_metadata_value(seed_data, 'Identifier')
    language = get_metadata_value(seed_data, 'Language')
    rights = get_metadata_value(seed_data, 'Rights')
    title = get_metadata_value(seed_data, 'Title')

    # If this is the first seed for this department, makes a CSV with a header row for the seed metadata.
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
