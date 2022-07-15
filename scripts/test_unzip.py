import subprocess
import os

os.chdir(r"C:\Users\amhan\Desktop\web_pres\aipdir")
warc_path = r"C:\Users\amhan\Desktop\web_pres\aipdir\aip\objects\ARCHIVEIT-12470-TEST-JOB1085452-SEED2028986-20200129224159397-00001-h3.warc.gz"

# Extracts the WARC from the gzip file.
# Deletes the gzip file, unless 7zip had an error during unzipping.
unzip_output = subprocess.run(f'"C:/Program Files/7-Zip/7z.exe" x "{warc_path}" -o"aip\objects"',
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True)

print("\nOutput from 7zip:")
print(unzip_output.stderr.decode('utf-8'))

if unzip_output.stderr == b'':
    os.remove(warc_path)
    print(f"Successfully unzipped warc")
else:
    print(f"Error unzipping")
