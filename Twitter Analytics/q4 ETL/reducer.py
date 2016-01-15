#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os
import dateutil.parser
import json

reload(sys)
sys.setdefaultencoding('utf-8')

lasttag = ''
lastdate = ''
lasttime = ''
lasttext = ''
lastid = ''
users = set()
count = 0

for line in sys.stdin:
	line = line.strip()
	k, d = line.split('\t')
	key = json.loads(k)
	data = json.loads(d)
	
	tag = key['tag']
	date = key['date']

	user_id = data['user_id']
	text = data['text']
	tweet_id = data['tweet_id']
	time = data['time']
	
	if tag == lasttag and date == lastdate:
		users.add(int(user_id))
		count += 1
		if time < lasttime or (time == lasttime and text < remaintext):
			remaintext = text
			lastid = tweet_id
			lasttime = time
	else:  #tag != lasttag or date != lastdate:
		if lasttag:
			users = sorted(list(users))
			userstring = ','.join(str(u) for u in users)
			encoder = json.dumps({'tag':lasttag, 'date':lastdate, 'count':count, 'users':userstring, 'tweet_id':lastid, 'text':remaintext})
			print encoder
		users = set()
		users.add(int(user_id))
		count = 1
		remaintext = text
		lasttag = tag
		lastdate = date
		lasttime = time
		lastid = tweet_id


