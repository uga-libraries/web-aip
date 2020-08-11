# AIP Workflow for Web Content

# Purpose and overview
Creates a preservation backup of web content captured via the Archive-It web archiving service by the Hargrett or Russell libraries. One AIP is made per seed, which includes all WARCs saved during the specified time period and six metadata reports. After the script is complete, the aips are ready for staff review and ingest into the digital preservation system (ARCHive).

UGA downloads web content automatically on a quarterly basis, using chrontab on a Linux server (to schedule the download) and the web_aip_batch.py script. Alternately, the batch script can be used to download content from all seeds crawled since a specified date or the web_aip_single.py script can be used to download content from a specified seed.  

# Script approach
Each step of the workflow is its own Python function. The functions are in a separate document (web_functions.py) so that they can easily be used in multilpe workflows. The workflow also uses functions that are part of the general born digital workflow for creating AIPs (aip_functions.py).

The functions are called by either web_aip_batch.py or web_aip_single.py to implement the workflow. The workflow scripts call the web functions to download the WARCs and six metadata reports using the Archive-It APIs and call the functions for each of the AIP workflow steps. The batch version of the workflow script downloads content for all seeds from the specified departments (Hargrett and Russell) crawled within the specified time period (last_download date). The single version of the script downloads content for a specified seed, with an additional optional limit of the last_download date. 
 
If a known error is encountered while downloading the WARCs and metadata reports, the script will continue and the errors are detected by the check_aips() function which analyzes if all expected content is present. If a known errors is enountered while creating the AIPs, such as failing a validation test or a regular expression does not find a match, the AIP is moved to an error folder with the name of the error and the rest of the steps are skipped for that AIP. A log is also created as the script runs which saves deailts about the errors during both parts of the script. 

# Script usage
python /path/website_preservation.py

# Dependencies
* Mac or Linux operating system
* Python requests and dateutil libraries
* [UGA Libraries' General AIP Preservation Scripts](https://github.com/uga-libraries/general-aip)

# Installation
1. Install the dependencies (listed above).  To install the Python libraries using pip, the commands are:
    * pip install requests
    * pip install python-dateutil
    
    
2. Download the scripts folder and save them to your computer.
3. Update the file path variables in the web_variables.py script for your local machine.
4. Change permissions on the scripts so they are executable
5. Follow the installation instructions for the [general aip scripts.](https://github.com/uga-libraries/general-aip)
6. To run quarterly, add it to the chrontab file.

# Workflow Details

1. Imports warc data as a Python object from warc_data.py and seed data as a dictionary from seed_data.py. Both scripts use Archive-It API's to get the data. Quits the script if there was any API connection status error. (website_preservation.py)


2. Downloads the warc and metadata files from Archive-It and organizes them into aip directories, with one aip per seed.

    * Stores the warc and seed metadata as variables. The variables are used as arguments for other scripts. This way, each API is only called once instead of repeating the call to get the specific information each of the other scripts requires. (website_preservation.py)

    * Creates the aip directory: the aip folder has the naming convention aip-id_AIP Title and contains metadata and objects folders. (aip_directory.py)

    * Uses WASAPI to download the warc(s) for the seed from Archive-It and saves to the correct objects folder. Verifies fixity after downloading. (download_warcs.py)

    * Uses the Partner API to download six reports for each AIP based on seed id, Archive-It collection id, and crawl definition id. Saves the reports to the correct metadata folder as csv files. (download_metadata.py)

    * Deletes any reports with a file size of 0, which means there was no metadata in that report, and redacts login information from seed report. (download_metadata.py)

    * Checks all aip directories for empty metadata or objects folders, and moves to an error folder if found. (check_aip_directory.py)

3. Makes folders for script outputs: master-xml, fits-xml, and aips-to-ingest. (website_preservation.py)

4. Extracts technical metadata from each warc in the objects folder with FITS and save the FITS xml to the metadata folder. Copies the information from each fits xml file into one file named combined-fits.xml, also saved in the metadata folder. (fits.py)

5. Transforms the combined-fits xml into PREMIS metadata using Saxon and xslt stylesheets, which is saved as master.xml in the metadata folder. Verifies that the master.xml file meets UGA standards with xmllint and xsds. (master_xml.py)

6. Uses bagit.py to bag the aip folder in place, making md5 and sha256 manifests. Validates the bag. (package.py)

7. Uses the prepare_bag perl script to tar and zip a copy of the bag, which is saved in the aips-to-ingest folder. (package.py)

8. Once all aips are created, uses md5deep to calculate the md5 for each packaged aip and saves it to a manifest. (website_preservation.py)

# Initial Author
Adriane Hanson, Head of Digital Stewardship, January 2020
