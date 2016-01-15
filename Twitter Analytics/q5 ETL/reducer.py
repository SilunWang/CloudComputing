#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os
import json

reload(sys)
sys.setdefaultencoding('utf-8')

lastuid = ''
tidlist = set()

for line in sys.stdin:
	uid, tid = line.strip().split('\t')
	
	if uid == lastuid:
		tidlist.add(tid)
	else: 
		if lastuid:
			count = len(tidlist)
			print '%s\t%s' % (lastuid,count)
		lastuid = uid
		tidlist = set()
		tidlist.add(tid)

count = len(tidlist)
print '%s\t%s' % (lastuid,count)

