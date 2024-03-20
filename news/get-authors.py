#!/usr/bin/env python3

"""
This script browses through git commit history (starting at latest tag), collects all authors of
commits and creates fragment for `towncrier`_ tool.

It's meant to be run during the release process, before generating the release notes.

Example::

    $ python get_authors.py

.. _towncrier: https://github.com/hawkowl/towncrier/

Authors:
    Aurelien Bompard
    Michal Konecny
"""

import os
from argparse import ArgumentParser
from collections import defaultdict
from subprocess import check_output


EXCLUDE = ["Weblate (bot)"]

last_tag = (
    check_output(["git", "tag", "--sort=creatordate"], text=True)
    .strip()
    .split("\n")[-1]
)

args_parser = ArgumentParser()
args_parser.add_argument(
    "until",
    nargs="?",
    default="HEAD",
    help="Consider all commits until this one (default: %(default)s).",
)
args_parser.add_argument(
    "since",
    nargs="?",
    default=last_tag,
    help="Consider all commits since this one (default: %(default)s).",
)
args = args_parser.parse_args()

authors = {}
commit_counts = defaultdict(int)

log_range = args.since + ".." + args.until
print(f"Scanning commits in range {log_range}")
output = check_output(["git", "log", log_range, "--format=%ae\t%an"], text=True)
for line in output.splitlines():
    email, fullname = line.split("\t")
    email = email.split("@")[0].replace(".", "")
    commit_counts[email] += 1
    if email in authors:
        continue
    authors[email] = fullname

for nick, fullname in authors.items():
    if fullname in EXCLUDE or fullname.endswith("[bot]"):
        continue
    filename = f"{nick}.author"
    if os.path.exists(filename):
        continue
    commit_count = commit_counts[nick]
    print(
        f"Adding author {fullname} ({nick}, {commit_count} commit{'' if commit_count < 2 else 's'})"
    )
    with open(filename, "w") as f:
        f.write(fullname)
        f.write("\n")
