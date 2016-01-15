#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os
import dateutil.parser
import json

reload(sys)
sys.setdefaultencoding('utf-8')

timestamp = dateutil.parser.parse('Sun Apr 20 00:00:00 +0000 2014')

for line in sys.stdin:
	line = line.strip()
	data = json.loads(line)
	user_id = data['user']['id_str']
	tweet_id = data['id_str']
	tweet_time = data['created_at']
	timedata = dateutil.parser.parse(tweet_time)
	text = data['text']
	if user_id != None and tweet_id != None and timedata != None and text != None:
		if timedata >= timestamp:
			encoder = json.dumps({'user_id':user_id, 'tweet_id':tweet_id, 'tweet_time':tweet_time, 'text':text})
			print encoder