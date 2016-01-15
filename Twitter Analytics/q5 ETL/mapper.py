#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os
import json

reload(sys)
sys.setdefaultencoding('utf-8')

for line in sys.stdin:
	line = line.strip()
	data = json.loads(line)

	user_id = data['user']['id_str']
	tweet_id = data['id_str']

	if user_id != None and tweet_id != None:
		print '%s\t%s' % (user_id, tweet_id)

