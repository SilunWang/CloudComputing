#!/usr/bin/env python

'''
This is a mapper function for AWS EMR
Please do not remove the first line

Author: Silun Wang
Andrew id: silunw
Date: 09/17/2015
'''

import sys
import os
import re

excluded_title_prefix = [
    "Media:",
    "Special:",
    "Talk:",
    "User:",
    "User_talk:",
    "Project:",
    "Project_talk:",
    "File:",
    "File_talk:",
    "MediaWiki:",
    "MediaWiki_talk:",
    "Template:",
    "Template_talk:",
    "Help:",
    "Help_talk:",
    "Category:",
    "Category_talk:",
    "Portal:",
    "Wikipedia:",
    "Wikipedia_talk:"
]

excluded_extension = [".jpg", ".gif", ".png", ".JPG", ".GIF", ".PNG", ".txt", ".ico"]

excluded_boilerplate = [
    "404_error/",
    "Main_Page",
    "Hypertext_Transfer_Protocol",
    "Search"
]


def check_title_prefix(str):
    for prefix in excluded_title_prefix:
        if str.startswith(prefix):
            return False
    return True


def check_title_extension(str):
    for suffix in excluded_extension:
        if str.endswith(suffix):
            return False
    return True


def check_first_letter(str):
    if re.match(r'^[a-z]', str):
        return False
    else:
        return True


def check_title_boilerplate(str):
    for boiler in excluded_boilerplate:
        if boiler == str:
            return False
    return True


def get_filename(str):
    index = str.find('pagecounts')
    return str[index:]


def get_date(str):
    return str.split('-')[1]


def get_hour(str):
    return str.split('-')[2]

# read line from standard input
line = sys.stdin.readline()

while line:
    arr = line.split(' ')
    title = arr[1]
    # check if format is valid
    if arr[0] == 'en' and arr[1] != "" \
            and check_title_prefix(title) and check_title_extension(title) \
            and check_first_letter(title) and check_title_boilerplate(title):
        # get file name
        # notice that os.environ gets absolute path
        filename = get_filename(os.environ["mapreduce_map_input_file"])
        print arr[1] + "\t" + get_date(filename) \
            + "\t" + get_hour(filename) + "\t" + arr[2]
    # continue
    line = sys.stdin.readline()