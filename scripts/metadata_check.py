# Purpose: Generate reports of required collection and seed metadata fields to check for completeness prior to
# downloading the WARCs and metadata for preservation. Hargrett and Russell reports are saved separately.

import csv
import os
import requests

# Gets Archive-It account credentials from the configuration file.
import configuration as c

def get_metadata_value(data, field):
    """Looks up the value of a field in the Archive-It API output for a particular collection or seed. If the field
    is not in the output, returns the string NONE instead."""
    try:
        return data['metadata'][field][0]['value']
    except KeyError:
        return 'NONE'


# All seeds from these collections (BMAC, DLG, and tests) will be excluded from the reports.
skip_collections = [11071, 12249, 12274, 12470]

# Folder where the reports should be saved.
output_directory = 'C:/Users/amhan/Documents/Work_From_Home/web'
os.chdir(output_directory)

# PART ONE: COLLECTION REPORTS
print("Making collection metadata reports.")

# Gets the Archive-It collection report with data on all the collections.
collections = requests.get(f'{c.partner_api}/collection?limit=-1', auth=(c.username, c.password))

# If there was an error with the API call, quits the script.
if not collections.status_code == 200:
    print('Error with Archive-It API connection when getting collection report', collections.status_code)
    exit()

# Saves the collection data as a Python object.
py_collections = collections.json()

# Makes CSVs (one per department) to save results to, including adding header rows.
with open('hargrett_collections_metadata.csv', 'w', newline='') as harg_output, open('russell_collections_metadata.csv', 'w', newline='') as rbrl_output, open('none_collections_metadata.csv', 'w', newline='') as none_output:
    collection_header = ['Collection ID', 'Collection Name', 'Collector', 'Date', 'Description', 'Title', 'Collection Page']
    harg_write = csv.writer(harg_output)
    harg_write.writerow(collection_header)
    rbrl_write = csv.writer(rbrl_output)
    rbrl_write.writerow(collection_header)
    none_write = csv.writer(none_output)
    none_write.writerow(collection_header)

    # Iterates over the metadata for each collection.
    for coll_data in py_collections:

        # Does not check collections if they are not Hargrett or Russell.
        if coll_data['id'] in skip_collections:
            continue

        # Constructs the URL of the collection's metadata page to make it easy to edit a record.
        collection_metadata_page = f"{c.inst_page}/collections/{coll_data['id']}/metadata"

        # Gets the values of the required metadata fields (or 'NONE' if the field has no metadata).
        coll_collector = get_metadata_value(coll_data, 'Collector')
        coll_date = get_metadata_value(coll_data, 'Date')
        coll_description = get_metadata_value(coll_data, 'Description')
        coll_title = get_metadata_value(coll_data, 'Title')

        # Adds a row to the appropriate CSV with the metadata for the collection.
        if coll_collector == 'Hargrett Rare Book & Manuscript Library':
            harg_write.writerow([coll_data['id'], coll_data['name'], coll_collector, coll_date, coll_description,
                                 coll_title, collection_metadata_page])
        elif coll_collector == 'NONE':
            none_write.writerow([coll_data['id'], coll_data['name'], coll_collector, coll_date, coll_description,
                                 coll_title, collection_metadata_page])
        else:
            rbrl_write.writerow([coll_data['id'], coll_data['name'], coll_collector, coll_date, coll_description,
                                 coll_title, collection_metadata_page])

# PART TWO: SEED REPORTS
print("Making seed metadata reports.")

# Gets the Archive-It seed report with data on all the seeds.
seeds = requests.get(f'{c.partner_api}/seed?limit=-1', auth=(c.username, c.password))

# If there was an error with the API call, quits the script.
if not seeds.status_code == 200:
    print('Error with Archive-It API connection when getting seed report', seeds.status_code)
    exit()

# Saves the seed data as a Python object.
py_seeds = seeds.json()

# Makes CSVs (one per department) to save results to, including adding header rows.
with open('hargrett_seeds_metadata.csv', 'w', newline='') as harg_output, open('russell_seeds_metadata.csv', 'w', newline='') as rbrl_output, open('none_seeds_metadata.csv', 'w', newline='') as none_output:
    seed_header = ['Seed ID', 'URL', 'Collector', 'Creator', 'Date', 'Identifier', 'Language', 'Rights', 'Title', 'Metadata Page']
    harg_write = csv.writer(harg_output)
    harg_write.writerow(seed_header)
    rbrl_write = csv.writer(rbrl_output)
    rbrl_write.writerow([seed_header])
    none_write = csv.writer(none_output)
    none_write.writerow([seed_header])

    # Iterates over the metadata for each seed.
    for seed_data in py_seeds:

        # Does not check seeds from collections if they are not Hargrett or Russell.
        if seed_data['collection'] in skip_collections:
            continue

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
        if collector == 'Hargrett Rare Book & Manuscript Library':
            harg_write.writerow([seed_data['id'], seed_data['url'], collector, creator, date, identifier, language,
                                 rights, title, seed_metadata_page])
        elif collector == 'NONE':
            none_write.writerow([seed_data['id'], seed_data['url'], collector, creator, date, identifier, language,
                                 rights, title, seed_metadata_page])
        else:
            rbrl_write.writerow([seed_data['id'], seed_data['url'], collector, creator, date, identifier, language,
                                 rights, title, seed_metadata_page])
