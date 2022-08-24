"""Purpose: a batch of web AIPs was created with an error to the file naming convention of one of the
metadata files. The seed id was used as the prefix for the crawl job instead of the AIP ID. This script
will update the AIPs to have the correct naming convention"""

# usage: python crawljob-fix.py aips_directory

import re
import sys
from aip_functions import *

# Make the aips_directory the current directory.
# This is the parent folder for all bags you wish to change.
os.chdir(sys.argv[1])

# Make log. Not using all the fields, just the ones this script touches.
# It will be added to the log already created from downloading the AIP contents.
log("header")

# Make output directories (in parent folder of aips_directory).
make_output_directories()

# Navigate through each of the AIPs in the aips_directory (there are other folders as well) and update.
for folder in os.listdir("."):

    # Skip folders that are not AIPs.
    if not folder.endswith("_bag"):
        continue

    print(f"\nUpdating {folder}")

    # Calculates the AIP ID and seed ID from the folder name.
    regex = re.match("(magil-ggp-(\d{7})-2022-08)_bag", folder)
    aip_id = regex.group(1)
    seed_id = regex.group(2)

    # Makes an AIP class instance. Supplies default values for ones that aren't used by this script.
    aip = AIP(sys.argv[1], "magil", "0000", folder, aip_id, "title", 1, True)

    # Finds and renames the crawljob file, replacing the seed ID in the name with the AIP ID.
    # Can't just construct the full path because the filename includes the crawl definition, which is unknown.
    # Using the wrong field in the log since this will be moved to a web log field (report info) later.
    for file in os.listdir(f"{folder}/data/metadata"):
        if file.endswith("_crawldef.csv"):
            new_name = file.replace(seed_id, aip_id)
            os.replace(f"{folder}/data/metadata/{file}", f"{folder}/data/metadata/{new_name}")
            aip.log["MetadataError"] = f"Updated crawl definition filename to {new_name}."

    # Update the bag manifest and validate the bag. Logs the result.
    # Can't use the bag function because need to do the updating differently.
    # Will stop processing the bag if it isn't valid.
    bag = bagit.Bag(folder)
    bag.save(manifests=True)
    try:
        bag.validate()
    except bagit.BagValidationError as errors:
        aip.log["BagValid"] = "Updated bag not valid (see log in AIP folder)"
        aip.log["Complete"] = "Error during processing."
        log(aip.log)
        move_error('bag_not_valid', f'{aip.id}_bag')
        with open(f"../errors/bag_not_valid/{aip.id}_bag_validation.txt", "w") as validation_log:
            if errors.details:
                for error_type in errors.details:
                    validation_log.write(str(error_type) + "\n")
            else:
                validation_log.write(str(errors))
        continue

    aip.log["BagValid"] = f"Updated bag valid on {datetime.datetime.now()}"

    # Tars and zips the aip.
    if aip.folder_name in os.listdir('.'):
        package(aip)

    # Adds the packaged AIP to the MD5 manifest in the aips-to-ingest folder.
    if aip.folder_name in os.listdir('.'):
        manifest(aip)
