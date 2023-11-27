# Download Archive-It Web Content for Preservation

# Overview
Downloads WARCs and six metadata reports for crawls saved during a specified time period 
from the Archive-It web archiving service to use for creating a preservation copy of web crawls.

The downloaded content is saved to folders organized by seed.
The metadata reports are collection, collection scope, crawl definition, crawl job, seed, and seed scope. 
After the script is complete, the folders are ready to use as input for the [UGA Libraries' General AIP Script](https://github.com/uga-libraries/general-aip), 
which prepares them for UGA's digital preservation system (ARCHive).

UGA downloads web content using this script on a quarterly basis.

# Getting Started

## Dependencies

* md5deep (https://github.com/jessek/hashdeep) - used to calculate fixity of the downloaded WARC
* numpy - used in unit tests to indicate blank cells
* pandas - used to work with API data and CSV (log) data
* requests - used to get data via Archive-It APIs

## Installation

Prior to running the script, create a file named configuration.py, modeled after the configuration_template.py,
and save it to your local copy of this repository.
This defines a place for script output to be saved and includes your Archive-It login credentials.

This script must be run in Linux, due to Windows commonly having unzip errors with gzip.

## Script Arguments

Run the script in the command line: `python ait_download.py date_start date_end`

   * date_start is inclusive: the download will include WARCs stored on date_start.
   * date_end is exclusive: the download will not include WARCs stored on date_end.
   * Format both dates YYYY-MM-DD
   
## Testing

There are unit tests for all the script functions used by ait_download.py except check_seeds(),
which will be changed soon.

# Workflow

Because the script can take days to run, due to the time required to download WARCs, it often gets interrupted. 
If this happens, running the script again will cause it to restart the seed that was in-progress when the error happened 
and download content for all seeds that had not started yet.
Any seed that already completed, even if it had errors, will not be downloaded again.

The script output is saved in the script output folder, defined in the configuration file.

1. Uses Archive-It API's, or the seeds_log.csv from an earlier iteration of the script, 
   to get data about the seeds for this download and make the metadata.csv used by the general-aip.py script. 
   

2. For each seed in the download:
   1. Makes a folder named with the seed id.
   2. Downloads the metadata reports.
   3. Deletes empty metadata reports (there is no metadata of that type in Archive-It).
   4. Redacts login information from the seed report.
   5. Downloads each WARC and verifies the fixity against the MD5 in Archive-It.
   6. Unzips each WARC.      
   7. Saves a summary of the errors, if any, to the log (seeds_log.csv).

   
3. Checks if everything expected was downloaded and makes a log (completeness_check.csv).


# Author
Adriane Hanson, Head of Digital Stewardship, University of Georgia
