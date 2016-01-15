#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os
import dateutil.parser
import json

reload(sys)
sys.setdefaultencoding('utf-8')

for line in sys.stdin:
	line = line.strip()
	data = json.loads(line)

	user_id = data['user']['id_str']
	tweet_time = data['created_at']
	text = data['text']
	hashtags = data['entities']['hashtags']
	tweet_id = data['id_str']

	if user_id != None and tweet_time != None and text != None and hashtags != None and tweet_id != None:
		for t in hashtags:
			tag = t['text']
			t_time = dateutil.parser.parse(tweet_time)
			date, time = t_time.strftime('%Y-%m-%d %H:%M:%S').split(' ')
			key = json.dumps({'tag':tag, 'date':date})
			encoder = json.dumps({'time':time, 'user_id':user_id, 'text':text, 'tweet_id':tweet_id})
			print '%s\t%s' % (key, encoder)

