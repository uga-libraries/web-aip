# AIP Workflow for Web Content

# Purpose and overview
Creates a preservation backup of web content captured via the Archive-It web archiving service by the Hargrett or Russell libraries. One AIP is made per seed, which includes all WARCs saved during the specified time period and six metadata reports (seed, seed scope, collection, collection scope, crawl job, and crawl definition). After the script is complete, the AIPs are ready for staff review and ingest into the digital preservation system (ARCHive).

UGA downloads web content automatically on a quarterly basis, using the web_aip_batch.py script. Alternately, the web_aip_single.py script can be used to download content from a specified seed.  

# Script approach
Each step of the workflow is its own Python function. The functions are in separate documents (web_functions.py and aip_functions.py) so that they can easily be used in multiple workflows. Passwords and local file paths used in the workflow are also stored in a separate document (configuration.py) so that this information can be kept out of the GitHub repository for security reasons.

The functions are called by either web_aip_batch.py or web_aip_single.py to implement the workflow. The workflow scripts call the web functions to download the WARCs and six metadata reports using the Archive-It APIs and call the functions for each of the AIP workflow steps. The batch version of the workflow script downloads content for all seeds from the Hargrett, MAGIL and Russell departments crawled between the specified dates. The single version of the script downloads content for a specified seed between the specified dates. 
 
If a known error is encountered while downloading the WARCs and metadata reports, the script will continue, and the errors are detected by the check_aips() function which analyzes if all expected content is present. If a known errors is encountered while creating the AIPs, such as failing a validation test, or a regular expression does not find a match, the AIP is moved to an error folder with the name of the error, and the rest of the steps are skipped for that AIP. A log is also created as the script runs which saves details about the errors during both parts of the script. 

# Script usage
python /path/web_aip_batch.py date_start date_end

python /path/web_aip_single.py seed_identifier aip_identifier archive-it_collection_identifier date_start date_end

date_start and date_end: the store date range for WARCs to be downloaded.
date_start is inclusive: the download will include WARCs stored on date_start.
date_end is exclusive: the download will not include WARCs stored on date_end.

This script has been tested on Windows 10 and Mac OS X.

# Dependencies
This list includes the dependencies for the General AIP portion of the workflow. md5deep, perl, and xmllint are pre-installed on most Mac and Linux operating systems. xmllint is included with Strawberry Perl.
* Archive-It login credentials
* Python Library: requests `pip install requests`
* [UGA Libraries' General AIP Script](https://github.com/uga-libraries/general-aip)
* bagit.py (https://github.com/LibraryOfCongress/bagit-python)
* FITS (https://projects.iq.harvard.edu/fits/downloads)
* md5deep (https://github.com/jessek/hashdeep)
* saxon9he (http://saxon.sourceforge.net/)
* Strawberry Perl (Windows only) (http://strawberryperl.com/)
* xmllint (http://xmlsoft.org/xmllint.html)
* 7-Zip (Windows only) (https://www.7-zip.org/download.html)

# Installation
1. Install the dependencies (listed above).
    
    
2. Download this repository and save to your computer.


3. Create a configuration.py file modeled after the configuration_template.py file with variables for Archive-It credentials and local file paths.


4. Download the [general aip repository](https://github.com/uga-libraries/general-aip) and follow the installation instructions.


5. Copy the aip_functions.py document to the scripts folder of the downloaded web-aip repository.


6. Change permissions on the scripts, so they are executable.


# Workflow Details
The workflow for each seed is essentially the same for the batch script and single AIP script. The batch script downloads content and makes a directory for each seed in the batch before making AIPs. The single AIP script uses a different function for downloading the seed data (since the AIP identifier is supplied via argument instead of calculated by the function) and checking the AIP for completeness (since it needs to filter by seed identifier).

1. Uses Archive-It API's to get data about the WARCs and seed(s) for this download. The WARC data is json converted to a Python object, and the seed data is stored in a dictionary (batch) or variables (single).


2. Downloads the WARCs and metadata files from Archive-It and organizes them into AIP directories, with one AIP per seed.

    * The AIP directory folder has the naming convention aip-id_AIP Title and contains metadata and objects folders.
   
    * Uses WASAPI to download the WARC(s) for the seed from Archive-It and saves to the objects folder. Verifies fixity after downloading.

    * Uses the Partner API to download six reports for each AIP based on the seed id, Archive-It collection id, and crawl definition id. Saves the reports to the metadata folder as csv files.

    * Deletes any reports with a file size of 0 (which means there was no metadata in that report) and redacts login information from seed report (if the field is present).

    * Checks all AIP directories for empty metadata or objects folders and moves to an error folder if found.


3. Makes folders for script outputs: aips-to-ingest, fits-xml, and preservation-xml.


4. Extracts technical metadata from each WARC in the objects folder with FITS and save the FITS xml to the metadata folder. Copies the information from each FITS xml file into one file named combined-fits.xml, also saved in the metadata folder.


5. Transforms the combined-fits xml into Dublin Core and PREMIS metadata using Saxon and xslt stylesheets, which is saved as preservation.xml in the metadata folder. Verifies that the master.xml file meets UGA standards with xmllint and xsds.


6. Uses bagit to bag the AIP folder in place, making md5 and sha256 manifests. Validates the bag.


7. Tars and zips a copy of the bag, which is saved in the aips-to-ingest folder.


8. Uses md5deep to calculate the md5 for each packaged AIP and saves it to a manifest.

# Additional Scripts
These are scripts that support the web preservation workflow. 
See the notes at the beginning of each script for further information and usage instructions.

## aip_from_download.py 
Converts a batch of folders organized as AIP directories (metadata and objects folders) with already downloaded WARCs and metadata files into an AIP. 
Use this when space limitations prevent download and AIP creation or errors cause you to need to manually download. 
WARNING: Created for a one time use so variables are part of the code (lines 136-139) instead of script arguments. 

Script usage: `python path/aip_from_download.py`

## linux_unzip.py
Checks WARC fixity, unzips the warc using the gunzip command, and bags the seed.
Must be run in a Linux environment.
Use there 7zip or other Windows zip programs have errors while unzipping the gzipped WARCs.

Script usage: `python linux_unzip.py aips_directory`

## Test Scripts
The four test scripts generate predictable script inputs to test the full functionality of web_aip_batch.py.
None of the scripts have arguments.

## update_web_aip.py
Remakes web AIPs when some but not all files had errors during the initial download.
Manually download any incorrect files from Archive-It before running this script.
This script saves time by not needing to re-download all the correct content.

Script usage: `python update_web_aip.py aips_directory`


# Initial Author
Adriane Hanson, Head of Digital Stewardship, January 2020
