#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os
import json

reload(sys)
sys.setdefaultencoding('utf-8')

lastuid = ''
content = list()

for line in sys.stdin:
	line = line.strip()
	uid, data = line.split('\t')
	data = json.loads(data)

	if uid == lastuid:
		content.append(data)
	else:  # new uid
		if lastuid:
			encoder = json.dumps(content)
			encoder = encoder.replace('\\','\\\\')
			print lastuid + '\t' + encoder
		lastuid = uid
		content = list()
		content.append(data)

encoder = json.dumps(content)
encoder = encoder.replace('\\','\\\\')
print lastuid + '\t' + encoder