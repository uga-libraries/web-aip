"""Purpose: remake web AIPs after the initial attempt with the web_aip_download.py script has errors,
such as download WARCs that have fixity changes and were deleted. After manually downloading the missing files,
this script will remake the AIPs so we don't have to re-download all the correct content.

Prior to running the script, make a metadata.csv file with the expected metadata for the general-aip script
and any additional metadata you need to verify the new contents of the AIP."""

# usage: python update_web_aip.py aips_directory

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

# Reads the metadata.csv (skipping the header) and updates each AIP folder in that CSV.
# Any AIPs missing from metadata.csv will not be updated and any extras in the metadata.csv will cause an error.
open_metadata = open("metadata.csv")
read_metadata = csv.reader(open_metadata)
next(read_metadata)
for row in read_metadata:

    # Saves each value from the metadata.csv for this AIP to a variable.
    # Everything through version is standard for metadata.csv.
    # Anything after that is needed for this particular update.
    department, collection, folder, aip_id, title, version, warc_name, ait_md5 = row

    # Makes an AIP class instance with the standard metadata.csv values.
    aip = AIP(sys.argv[1], department, collection, folder, aip_id, title, version, True)

    # Verify the updated AIP.
    # In this case, checks that the zipped WARCs which were re-downloaded have the correct MD5.
    # Uses the Objects Folder part of the log as a temporary place for this information.
    # When updates are done, all log information for this batch will be merged.
    warc_path = f"{folder}/data/objects/{warc_name}"
    md5deep_output = subprocess.run(f'"{c.MD5DEEP}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)
    try:
        regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(md5deep_output.stdout))
        downloaded_warc_md5 = regex_md5.group(1)
    except AttributeError:
        aip.log["ObjectsError"] = f"Fixity for {warc_name} cannot be extracted from md5deep output: {md5deep_output.stdout}"
        log(aip.log)
        continue
    if not ait_md5 == downloaded_warc_md5:
        os.remove(warc_path)
        aip.log["ObjectsError"] = f"Fixity for {warc_name} changed and it was deleted"
        continue
    else:
        aip.log["ObjectsError"] = f"Successfully verified {warc_name} fixity on {datetime.datetime.now()}"

    # Unzips the new WARC.
    # Uses the Metadata Folder part of the log as a temporary place for this information.
    # When updates are done, all log information for this batch will be merged.
    unzip_output = subprocess.run(f'"C:/Program Files/7-Zip/7z.exe" x "{warc_path}" -o"{aip.folder_name}/data/objects"',
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True)
    if unzip_output.stderr == b'':
        os.remove(warc_path)
        aip.log["MetadataError"] = f"Successfully unzipped {warc_name}"
    else:
        aip.log["MetadataError"] = f"Error unzipping {warc_name}"
        log(aip.log)
        continue

    # Since the contents of the objects folder are changed with the addition of new WARCs,
    # makes FITS XML and preservation.xml again. The old ones are already deleted.
    # This is a copy of extract_metadata() with an adjustment of the paths since in a bag already.
    # This would not be necessary if the change was to the metadata folder.
    fits_output = subprocess.run(f'"{c.FITS}" -r -i "{aip.directory}/{aip.folder_name}/data/objects" -o "{aip.directory}/{aip.folder_name}/data/metadata"',
                                 shell=True, stderr=subprocess.PIPE)
    if fits_output.stderr:
        with open(f"{aip.directory}/{aip.folder_name}/data/metadata/{aip.id}_fits-tool-errors_fitserr.txt", "w") as fits_errors:
            fits_errors.write(fits_output.stderr.decode('utf-8'))
        aip.log["FITSTool"] = "FITs tools generated errors (saved to metadata folder)"
    else:
        aip.log["FITSTool"] = "No FITS tools errors"
    for item in os.listdir(f'{aip.folder_name}/data/metadata'):
        if item.endswith('.fits.xml'):
            new_name = item.replace('.fits', '_fits')
            os.rename(f'{aip.folder_name}/data/metadata/{item}', f'{aip.folder_name}/data/metadata/{new_name}')
    combo_tree = ET.ElementTree(ET.Element('combined-fits'))
    combo_root = combo_tree.getroot()
    for doc in os.listdir(f'{aip.folder_name}/data/metadata'):
        if doc.endswith('_fits.xml'):
            ET.register_namespace('', "http://hul.harvard.edu/ois/xml/ns/fits/fits_output")
            try:
                tree = ET.parse(f'{aip.folder_name}/data/metadata/{doc}')
                root = tree.getroot()
                combo_root.append(root)
            except ET.ParseError as error:
                aip.log["FITSError"] = f"Issue when creating combined-fits.xml: {error.msg}"
                aip.log["Complete"] = "Error during processing."
                log(aip.log)
                move_error('combining_fits', aip.id)
                continue
    aip.log["FITSError"] = "Successfully created combined-fits.xml"
    combo_tree.write(f'{aip.folder_name}/data/metadata/{aip.id}_combined-fits.xml', xml_declaration=True, encoding='UTF-8')

    combined_fits = f'{aip.folder_name}/data/metadata/{aip.id}_combined-fits.xml'
    cleanup_stylesheet = f'{c.STYLESHEETS}/fits-cleanup.xsl'
    cleaned_fits = f'{aip.folder_name}/data/metadata/{aip.id}_cleaned-fits.xml'
    cleaned_output = subprocess.run(
        f'java -cp "{c.SAXON}" net.sf.saxon.Transform -s:"{combined_fits}" -xsl:"{cleanup_stylesheet}" -o:"{cleaned_fits}"',
        stderr=subprocess.PIPE, shell=True)
    if cleaned_output.stderr:
        aip.log["PresXML"] = f"Issue when creating cleaned-fits.xml. Saxon error: {cleaned_output.stderr.decode('utf-8')}"
        aip.log["Complete"] = "Error during processing."
        log(aip.log)
        move_error('cleaned_fits_saxon_error', aip.id)
        continue
    stylesheet = f'{c.STYLESHEETS}/fits-to-preservation.xsl'
    preservation_xml = f'{aip.folder_name}/data/metadata/{aip.id}_preservation.xml'
    arguments = f'collection-id="{aip.collection_id}" aip-id="{aip.id}" aip-title="{aip.title}" ' \
                f'department="{aip.department}" version={aip.version} ns={c.NAMESPACE}'
    pres_output = subprocess.run(
        f'java -cp "{c.SAXON}" net.sf.saxon.Transform -s:"{cleaned_fits}" -xsl:"{stylesheet}" -o:"{preservation_xml}" {arguments}',
        stderr=subprocess.PIPE, shell=True)
    if pres_output.stderr:
        aip.log["PresXML"] = f"Issue when creating preservation.xml. Saxon error: {pres_output.stderr.decode('utf-8')}"
        aip.log["Complete"] = "Error during processing."
        log(aip.log)
        move_error('pres_xml_saxon_error', aip.id)
        continue
    validation = subprocess.run(f'xmllint --noout -schema "{c.STYLESHEETS}/preservation.xsd" "{preservation_xml}"',
                                stderr=subprocess.PIPE, shell=True)
    validation_result = validation.stderr.decode('utf-8')
    if 'failed to load' in validation_result:
        aip.log["PresXML"] = f"Preservation.xml was not created. xmllint error: {validation_result}"
        aip.log["Complete"] = "Error during processing."
        log(aip.log)
        move_error('preservationxml_not_found', aip.id)
        continue
    else:
        aip.log["PresXML"] = "Successfully created preservation.xml"
    if 'fails to validate' in validation_result:
        aip.log["PresValid"] = "Preservation.xml is not valid (see log in error folder)"
        aip.log["Complete"] = "Error during processing."
        log(aip.log)
        move_error('preservationxml_not_valid', aip.id)
        with open(f"../errors/preservationxml_not_valid/{aip.id}_presxml_validation.txt", "w") as validation_log:
            for line in validation_result.split("\r"):
                validation_log.write(line + "\n")
        continue
    else:
        aip.log["PresValid"] = f"Preservation.xml valid on {datetime.datetime.now()}"
    shutil.copy2(f'{aip.folder_name}/data/metadata/{aip.id}_preservation.xml', '../preservation-xml')
    os.replace(f'{aip.folder_name}/data/metadata/{aip.id}_combined-fits.xml', f'../fits-xml/{aip.id}_combined-fits.xml')
    os.remove(f'{aip.folder_name}/data/metadata/{aip.id}_cleaned-fits.xml')

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
    if aip.folder_name in os.listdir('..'):
        package(aip)

    # Adds the packaged AIP to the MD5 manifest in the aips-to-ingest folder.
    if aip.folder_name in os.listdir('..'):
        manifest(aip)

print("\nUpdate script is complete.")
print("This script does not check for completeness, so verify that manually.")
