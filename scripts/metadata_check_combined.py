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


def make_csv(json, report_type, include_optional):
    """Saves the values from the desired fields (required only or all) for either the collections or seeds to a CSV. """

    with open(f'{report_type}_metadata.csv', 'w', newline='') as output:

        # Makes the header row, with different vales depending on if the optional fields are included.
        # TODO: this is for collection. Need to include seed required/not required as well.
        if optional:
            header = ['Collection ID', 'Collection Name', 'Collector [required]', 'Creator', 'Date [required]',
                      'Description [required]', 'Identifier', 'Language', 'Relation', 'Rights', 'Subject',
                      'Title [required]', 'Collection Page']
        else:
            header = ['Collection ID', 'Collection Name', 'Collector', 'Date', 'Description', 'Title', 'Collection Page']
        write = csv.writer(output)
        write.writerow(header)

        # Iterates over the metadata for each item.
        for data in json:

            # Constructs the URL of the Archive-It metadata page.
            # Included in the report to make it easy to edit a record.
            # TODO: this is for a collection. Needs to work for seed.
            metadata = f"{c.inst_page}/collections/{data['id']}/metadata"

            # Gets the values of the required metadata fields (or 'NONE' if the field has no metadata).
            # TODO: these are what are required for a collection. Needs to work for seed.
            collector = get_metadata_value(data, 'Collector')
            date = get_metadata_value(data, 'Date')
            description = get_metadata_value(data, 'Description')
            title = get_metadata_value(data, 'Title')

            # Gets the values of the optional metadata fields, if desired.
            # TODO: these are what is optional for a collection. Needs to work for seed.
            if optional:
                creator = get_metadata_value(data, 'Creator')
                identifier = get_metadata_value(data, 'Identifier')
                language = get_metadata_value(data, 'Language')
                relation = get_metadata_value(data, 'Relation')
                rights = get_metadata_value(data, 'Rights')
                subject = get_metadata_value(data, 'Subject')

            # Constructs the row of metadata to save to the CSV, including optional metadata fields if desired.
            # TODO: these are what is required or optional for a collection. Needs to work for seed.
            if optional:
                row = [data['id'], data['name'], collector, creator, date, description, identifier, language, relation, rights, subject, title, metadata]
            else:
                row = [data['id'], data['name'], collector, date, description, title, metadata]

            # Adds the metadata as a row to the CSV.
            write.writerow(row)


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

# Saves the collection data to a CSV.
make_csv(py_collections, "collection", optional)


# PART TWO: SEED REPORTS

# Gets the Archive-It seed report with data on all the seeds.
seeds = requests.get(f'{c.partner_api}/seed?limit=-1', auth=(c.username, c.password))

# If there was an error with the API call, quits the script.
if not seeds.status_code == 200:
    print('Error with Archive-It API connection when getting seed report', seeds.status_code)
    exit()

# Saves the seed data as a Python object.
py_seeds = seeds.json()

# Saves the seed data to a CSV.
make_csv(py_seeds, "seed", optional)