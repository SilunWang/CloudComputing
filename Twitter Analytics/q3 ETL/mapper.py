#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os
import dateutil.parser
import json

reload(sys)
sys.setdefaultencoding('utf-8')

tab = '_upinthecloudtab_'
linefeed = '_upinthecloudlinefeed_'
r = '_upinthecloudr_'

for line in sys.stdin:
	line = line.strip()
	data = json.loads(line)

	user_id = data['user']['id_str']
	tweet_id = data['id_str']
	tweet_time = data['created_at']
	text = data['text']
	followers_count = data['user']['followers_count']

	if user_id != None and tweet_id != None and tweet_time != None and text != None and followers_count != None:
		tweet_time = dateutil.parser.parse(tweet_time)
		tweet_time = tweet_time.strftime('%Y-%m-%d')
		text = text.replace('\t',tab)
		text = text.replace('\n',linefeed)
		text = text.replace('\r',r)
		#print '%s\t%s\t%s\t%s\t%s' % (tweet_id, user_id, tweet_time, followers_count, text) 
		encoder = json.dumps({'tweet_id':tweet_id, 'user_id':user_id, 'tweet_time':tweet_time, 'text':text, 'followers_count':followers_count})
		print encoder