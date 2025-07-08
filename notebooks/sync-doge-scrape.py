# Look for new data in https://github.com/m-nolan/doge-scraper/data and upload anything missing to Big Local News

import json
import os
import sys
from time import sleep
from urllib.parse import urlparse

from bln import Client
import requests
from tqdm import tqdm


starturl = "https://github.com/m-nolan/doge-scrape/data"    # Change this if you want to grab another folder. This URL should NOT actually work.

project_id = "UHJvamVjdDo2NzkxYTJmNi0wNTNmLTQzMTEtYjE5Yy03MTc3MzFmMGUwZDY="    # BLN platform ID

datadir = "data/"

os.makedirs(datadir, exist_ok=True)





pathchunks = urlparse(starturl).path.split("/")
pathstart = "/".join(pathchunks[0:3])
pathmiddle = "/tree/main/"
pathend = "/".join(pathchunks[3:])
# folderurl = urlparse(starturl).scheme + urlparse(starturl).netloc + pathstart + pathmiddle + pathend
folderurl = urlparse(starturl)._replace(path=pathstart + pathmiddle + pathend).geturl()
print(f"From {starturl} we are actually looking to scrape {folderurl}")

# No longer works

fileswanted = set()
r = requests.get(folderurl)
for holder in pq(r.content)("div.react-directory-filename-cell"):
    for link in pq(holder)("a"):
        fileswanted.add(pq(link)("a").attr("title"))
print(f"Seeking {' ... '.join(fileswanted)}")



fileswanted = set()
r = requests.get(folderurl)
html = r.text
script = html.split('data-target="react-app.embeddedData">')[1].split("</script>")[0]
rawdata = json.loads(script)
for entry in rawdata['payload']['tree']['items']:
    fileswanted.add(entry['name'])
print(f"Seeking {' ... '.join(fileswanted)}")





versionedfiles = {}

# https://api.github.com/repos/m-nolan/doge-scrape/commits?path=/data/
baseurl = urlparse(starturl)._replace(netloc="api.github.com", path="/repos" + "/".join(pathchunks[0:3]) + "/commits?path=/" + "/".join(pathchunks[3:]) + "/").geturl()
for filewanted in fileswanted:
    targeturl = baseurl + filewanted
    r = requests.get(targeturl)

    for entry in r.json():
        targetpath = "/".join(pathchunks[0:3]) + "/raw/" + entry['sha'] + "/" + "/".join(pathchunks[3:]) + "/" + filewanted
        targeturl = urlparse(starturl)._replace(path=targetpath).geturl()
        targetfilename = filewanted.split(".")[0] + "_"
        targetfilename += entry['commit']['committer']['date'].replace(":", "").replace("Z", "")
        if len(filewanted.split(".")) > 1:
            targetfilename += "."
            targetfilename += ".".join(filewanted.split(".")[1:])
        versionedfiles[targetfilename] = targeturl   





bln_api = os.environ["BLN_API_TOKEN"]
bln = Client(bln_api)
project = bln.get_project_by_id(project_id)

files_to_send = {}

# Get all the files in the project.
archived_files = {}
for f in project["files"]:
    archived_files[f["name"]] = f["updatedAt"]

for versionedfile in versionedfiles:
    if versionedfile not in archived_files:
        files_to_send[versionedfile] = versionedfiles[versionedfile]

if len(files_to_send) == 0:
    print("No files found to be sent.")
    sys.exit()
else:
    print(f"{len(files_to_send):,} files to be sent to BLN, and likely that many to download here first.")





for file_to_send in tqdm(files_to_send):
    # Get the file if it's not already here
    if os.path.exists(datadir + file_to_send):
        got_file = True
    else:
        got_file = False
        localurl = files_to_send[file_to_send]
        r = requests.get(localurl)
        if not r.ok:
            print(f"Failure downloading {localurl}")
        else:
            got_file = True
            with open(datadir + file_to_send, "wb") as outfile:
                outfile.write(r.content)
    if got_file:
        bln.upload_file(project_id, datadir + file_to_send)
    sleep(1)













