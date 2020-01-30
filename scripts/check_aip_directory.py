# Purpose: Checks for any incomplete aips (empty metadata and/or objects folders) and moves them to an error folder.

import os
import re
from web_variables import move_error


# Iterates through the aips directory.
# Finds and acts on metadata and objects subfolders.

for root, folders, files in os.walk('.'):

    if root.endswith('metadata'):

        # This is true if the metadata folder is empty.

        if not os.listdir(root):

            # Calculates the name of the parent folder (the aip)
            # and moves the aip folder to an error folder.

            regex = re.match('^(.*)\/metadata', root)
            aip = regex.group(1)
                
            move_error('incomplete_directory', aip)
            

    if root.endswith('objects'):

        # This is true if the objects folder is empty.

        if not os.listdir(root):

            # Calculates the name of the parent folder (the aip).
            # and moves the aip folder to an error folder.

            regex = re.match('^(.*)\/objects', root)
            aip = regex.group(1)

            move_error('incomplete_directory', aip)
