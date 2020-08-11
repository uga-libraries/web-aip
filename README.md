# AIP Workflow for Web Content

# Purpose and overview
Creates a preservation backup of web content captured via the Archive-It web archiving service by the Hargrett or Russell libraries. One AIP is made per seed, which includes all WARCs saved during the specified time period and six metadata reports (seed, seed scope, collection, collection scope, crawl job, and crawl definition). After the script is complete, the AIPs are ready for staff review and ingest into the digital preservation system (ARCHive).

UGA downloads web content automatically on a quarterly basis, using chrontab on a Linux server (to schedule the download) and the web_aip_batch.py script. Alternately, the batch script can be used to download content from all seeds crawled since a specified date or the web_aip_single.py script can be used to download content from a specified seed.  

# Script approach
Each step of the workflow is its own Python function. The functions are in a separate document (web_functions.py) so that they can easily be used in multilpe workflows. The workflow also uses functions that are part of the general born digital workflow for creating AIPs (aip_functions.py).

The functions are called by either web_aip_batch.py or web_aip_single.py to implement the workflow. The workflow scripts call the web functions to download the WARCs and six metadata reports using the Archive-It APIs and call the functions for each of the AIP workflow steps. The batch version of the workflow script downloads content for all seeds from the specified departments (Hargrett and Russell) crawled within the specified time period (last_download date). The single version of the script downloads content for a specified seed, with an additional optional limit of the last_download date. 
 
If a known error is encountered while downloading the WARCs and metadata reports, the script will continue and the errors are detected by the check_aips() function which analyzes if all expected content is present. If a known errors is enountered while creating the AIPs, such as failing a validation test or a regular expression does not find a match, the AIP is moved to an error folder with the name of the error and the rest of the steps are skipped for that AIP. A log is also created as the script runs which saves deailts about the errors during both parts of the script. 

# Script usage
python /path/web_aip_batch.py [last_download_date]
python /path/web_aip_single.py seed_identifier aip_identifier archive-it_collection_identifier [last_download_date]

This script has been tested on Windows 10 and Mac OS X.

# Dependencies
This list includes the dependencies for the General AIP portion of the workflow. md5deep, perl, and xmllint are pre-installed on most Mac and Linux operating systems. xmllint is included with Strawberry Perl.
* Archive-It login credentials
* Python Libraries: requests and python-dateutil
* [UGA Libraries' General AIP Script](https://github.com/uga-libraries/general-aip)
* bagit.py (https://github.com/LibraryOfCongress/bagit-python)
* FITS (https://projects.iq.harvard.edu/fits/downloads)
* md5deep (https://github.com/jessek/hashdeep)
* saxon9he (http://saxon.sourceforge.net/)
* Strawberry Perl (Windows only) (http://strawberryperl.com/)
* xmllint (http://xmlsoft.org/xmllint.html)
* 7-Zip (Windows only) (https://www.7-zip.org/download.html)

# Installation
1. Install the dependencies (listed above).  To install the Python libraries using pip, the commands are:
    * pip install requests
    * pip install python-dateutil
    
    
2. Download this repository and save to your computer.
3. Update the constants variables for the script output and Archive-It credentials (line 18-22) in the web_variables.py script for your local machine.
4. Download the [general aip repository](https://github.com/uga-libraries/general-aip) and follow the installation instructions.
5. Copy the aip_functions.py document to the scripts folder of the downloaded web-aip repository.
6. Change permissions on the scripts so they are executable.
7. To run quarterly, add it to the chrontab file or use another scheduling method.

# Workflow Details
The workflow for each seed is the essentially the same for the batch script and single AIP script. The batch script downloads content and makes a directory for each seed in the batch before making AIPs. The single AIP script uses a different function for downloading the seed data (since the AIP identifier is supplied via argument instead of calculated by the function) and checking the AIP for completeness (since it needs to filter by seed identifier).

1. Uses Archive-It API's to get data about the WARCs and seed(s) for this download. The WARC data is json converted to a Python object and the seed data is stored in a dictionary (batch) or variables (single).


2. Downloads the WARCs and metadata files from Archive-It and organizes them into AIP directories, with one AIP per seed.

    * The AIP directory folder has the naming convention aip-id_AIP Title and contains metadata and objects folders.

    * Uses WASAPI to download the WARC(s) for the seed from Archive-It and saves to the objects folder. Verifies fixity after downloading.

    * Uses the Partner API to download six reports for each AIP based on seed id, Archive-It collection id, and crawl definition id. Saves the reports to the metadata folder as csv files.

    * Deletes any reports with a file size of 0 (which means there was no metadata in that report) and redacts login information from seed report (if the field is present).

    * Checks all AIP directories for empty metadata or objects folders and moves to an error folder if found.

3. Makes folders for script outputs: aips-to-ingest, fits-xml, and preservaiton-xml.

4. Extracts technical metadata from each WARC in the objects folder with FITS and save the FITS xml to the metadata folder. Copies the information from each fits xml file into one file named combined-fits.xml, also saved in the metadata folder.

5. Transforms the combined-fits xml into Dublin Core and PREMIS metadata using Saxon and xslt stylesheets, which is saved as master.xml in the metadata folder. Verifies that the master.xml file meets UGA standards with xmllint and xsds.

6. Uses bagit to bag the AIP folder in place, making md5 and sha256 manifests. Validates the bag.

7. Uses a perl script to tar and zip a copy of the bag, which is saved in the aips-to-ingest folder.

8. Once all AIPs are created, uses md5deep to calculate the md5 for each packaged AIP and saves it to a manifest.

# Initial Author
Adriane Hanson, Head of Digital Stewardship, January 2020
