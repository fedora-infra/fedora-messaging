#!/usr/bin/env python3

from subprocess import check_output

authors = {}
output = check_output(["git", "log", "--format=%ae\t%an"], universal_newlines=True)
for line in output.splitlines():
    email, fullname = line.split("\t")
    email = email.split("@")[0].replace(".", "")
    if email in authors:
        continue
    authors[email] = fullname

for nick, fullname in authors.items():
    with open("{}.author".format(nick), "w") as f:
        f.write(fullname)
        f.write("\n")
