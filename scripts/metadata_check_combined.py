# Purpose: Generate reports of required collection and seed metadata fields to check for completeness prior to
# downloading the WARCs and metadata for preservation. Information for all departments is saved in one report so the
# library metadata can be reviewed in aggregate.

# Usage: python /path/metadata_check_combined.py /path/output_directory [all_fields]
#   Include "all_fields" as an optional second argument to include optional as well as required fields.

# Ideas for improvement: use default location from configuration file for output directory if none supplied; add
# limit by date to see just this download; differentiate between active and inactive collections.

import csv
import os
import requests
import sys

# Gets Archive-It account credentials from the configuration file.
import configuration as c


def get_metadata_value(data, field):
    """Looks up the value(s) of a field in the Archive-It API output for a particular collection or seed. If the
    field is not in the output, returns the string NONE instead. """

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
        return 'NONE'


# Changes the current directory to the folder where the reports will be saved, which is provided by user.
# If this cannot be done, prints an error for the user and quits the script.
try:
    output_directory = sys.argv[1]
    os.chdir(output_directory)
except (IndexError, FileNotFoundError):
    print('The required output_directory was either missing or is not a valid directory. Please try again')
    print('Script usage: python /path/metadata_check_combined.py /path/output_directory [all_fields]')
    exit()

# If the optional argument was provided, sets a variable optional to True.
# If the second argument is not the expected value, prints an error for the user and quits the script.
optional = False
if len(sys.argv) == 3:
    if sys.argv[2] == "all_fields":
        optional = True
    else:
        print('The provided value for the second argument is not the expected value of "all_fields".')
        print('Script usage: python /path/metadata_check_combined.py /path/output_directory [all_fields]')
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

# Makes a CSV with a header row to save the collection metadata to.
with open('collections_metadata.csv', 'w', newline='') as output:

    # Makes the header row, with different vales depending on if the optional fields are included.
    if optional:
        header = ['Collection ID', 'Collection Name', 'Collector [required]', 'Creator', 'Date [required]',
                  'Description [required]', 'Identifier', 'Language', 'Relation', 'Rights', 'Subject',
                  'Title [required]', 'Collection Page']
    else:
        header = ['Collection ID', 'Collection Name', 'Collector', 'Date', 'Description', 'Title', 'Collection Page']
    write = csv.writer(output)
    write.writerow(header)

    # Iterates over the metadata for each collection.
    for coll_data in py_collections:

        # Constructs the URL of the collection's metadata page. Included in the report to make it easy to edit a record.
        coll_metadata = f"{c.inst_page}/collections/{coll_data['id']}/metadata"

        # Gets the values of the required metadata fields (or 'NONE' if the field has no metadata).
        coll_collector = get_metadata_value(coll_data, 'Collector')
        coll_date = get_metadata_value(coll_data, 'Date')
        coll_description = get_metadata_value(coll_data, 'Description')
        coll_title = get_metadata_value(coll_data, 'Title')

        # Gets the values of the optional metadata fields, if desired.
        if optional:
            coll_creator = get_metadata_value(coll_data, 'Creator')
            coll_identifier = get_metadata_value(coll_data, 'Identifier')
            coll_language = get_metadata_value(coll_data, 'Language')
            coll_relation = get_metadata_value(coll_data, 'Relation')
            coll_rights = get_metadata_value(coll_data, 'Rights')
            coll_subject = get_metadata_value(coll_data, 'Subject')

        # Constructs the row with the collection's data, including optional metadata fields if desired.
        if optional:
            row = [coll_data['id'], coll_data['name'], coll_collector, coll_creator, coll_date, coll_description,
                   coll_identifier, coll_language, coll_relation, coll_rights, coll_subject, coll_title, coll_metadata]
        else:
            row = [coll_data['id'], coll_data['name'], coll_collector, coll_date, coll_description, coll_title, coll_metadata]

        # Adds the collection metadata as a row to the CSV.
        write.writerow(row)


# PART TWO: SEED REPORTS

# Gets the Archive-It seed report with data on all the seeds.
seeds = requests.get(f'{c.partner_api}/seed?limit=-1', auth=(c.username, c.password))

# If there was an error with the API call, quits the script.
if not seeds.status_code == 200:
    print('Error with Archive-It API connection when getting seed report', seeds.status_code)
    exit()

# Saves the seed data as a Python object.
py_seeds = seeds.json()

# Makes a CSV with a header row to save the seed metadata to.
with open('seeds_metadata.csv', 'w', newline='') as output:

    # Makes the header row, with different vales depending on if the optional fields are included.
    if optional:
        header = ['Seed ID', 'URL', 'Collector [Required]', 'Creator [Required]', 'Date [Required]', 'Description',
                  'Identifier [Required]', 'Language [Required]', 'Relation', 'Rights [Required]', 'Subject',
                  'Title [Required]', 'Metadata Page']
    else:
        header = ['Seed ID', 'URL', 'Collector', 'Creator', 'Date', 'Identifier', 'Language', 'Rights', 'Title',
                  'Metadata Page']
    write = csv.writer(output)
    write.writerow(header)

    # Iterates over the metadata for each seed.
    for seed_data in py_seeds:

        # Constructs the URL of the seed's metadata page. Included in the report to make it easy to edit a record.
        seed_metadata = f"{c.inst_page}/collections/{seed_data['collection']}/seeds/{seed_data['id']}/metadata"

        # Gets the values of the required metadata fields (or 'NONE' if the field has no metadata).
        collector = get_metadata_value(seed_data, 'Collector')
        creator = get_metadata_value(seed_data, 'Creator')
        date = get_metadata_value(seed_data, 'Date')
        identifier = get_metadata_value(seed_data, 'Identifier')
        language = get_metadata_value(seed_data, 'Language')
        rights = get_metadata_value(seed_data, 'Rights')
        title = get_metadata_value(seed_data, 'Title')

        # Gets the values of the optional metadata fields, if desired.
        if optional:
            description = get_metadata_value(seed_data, 'Description')
            relation = get_metadata_value(seed_data, 'Relation')
            subject = get_metadata_value(seed_data, 'Subject')

        # Constructs the row with the collection's data, including optional metadata fields if desired.
        if optional:
            row = [seed_data['id'], seed_data['url'], collector, creator, date, description, identifier, language,
                   relation, rights, subject, title, seed_metadata]
        else:
            row = [seed_data['id'], seed_data['url'], collector, creator, date, identifier, language, rights, title,
                   seed_metadata]

        # Adds the seed metadata as a row to the CSV.
        write.writerow(row)
