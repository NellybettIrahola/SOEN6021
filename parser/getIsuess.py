#!/usr/bin/python

import requests
import sys
import codecs
from pprint import pprint

# get the username and the git repo to retrieve the commits
github_user = "lxc"
github_repo = "lxd"

i= 1

#  call get api
print('https://api.github.com/repos/' + github_user + '/' + github_repo + '/' + 'issues?state=all')
r = requests.get('https://api.github.com/repos/' + github_user + '/' + github_repo + '/' + 'issues?state=all',
                 auth=('token', ''))

f = codecs.open("/home/nellybett/Desktop/issues/" + github_repo + "_" + str(i) + ".json", "w", "utf-8")
f.write(r.text)
f.close()



# # get next page
next_page = r.headers['link'].split(';')[0].replace('<', '').replace('>', '')
final_page=next_page[0:-1]
i = i + 1
print(next_page)
print(final_page)

while i< 237:

    # print next_page
    r = requests.get(final_page+str(i), auth=('token', ''))

    # write json file
    f = codecs.open("/home/nellybett/Desktop/issues/" + github_repo + "_" + str(i) + ".json", "w", "utf-8")
    f.write(r.text)
    f.close()

    i = i + 1
