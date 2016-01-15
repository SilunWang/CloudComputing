#!/usr/bin/env python

'''
This is a reducer function for AWS EMR
Please do not remove the first line

Author: Silun Wang
Andrew id: silunw
Date: 09/17/2015
'''

import sys
# title of current page
current_page = None
# pageviews for current page
current_date_count = [0 for i in range(31)]
page = None
date = None

# read line from standard input
line = sys.stdin.readline()


# print line in format
def print_line():
    curr_line = str(sum(current_date_count)) + "\t" + current_page
    for x in range(31):
        curr_line += "\t" + str(20150801 + x) + ':' + str(current_date_count[x])
    return curr_line


while line:
    page, date, hour, count = line.split('\t', 3)
    line = sys.stdin.readline()

    # string to int
    try:
        count = int(count)
    except ValueError:
        continue
    # same page
    if current_page == page:
        current_date_count[int(date) - 20150801] += count
    # new page
    else:
        if current_page and sum(current_date_count) > 100000:
            print print_line()
        # init
        current_date_count = [0 for i in range(31)]
        current_page = page
        current_date_count[int(date) - 20150801] += count

if current_page == page and sum(current_date_count) > 100000:
    print print_line()
