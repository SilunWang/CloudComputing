#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os
import json

reload(sys)
sys.setdefaultencoding('utf-8')

tab = '_upinthecloudtab_'
linefeed = '_upinthecloudlinefeed_'
r = '_upinthecloudr_'

for line in sys.stdin:
	line = line.strip()
	data = json.loads(line)

	tweet_id = data['tweet_id']
	user_id = data['user_id']
	tweet_time = data['tweet_time']
	score = data['score']
	text = data['censored_text']
	text = text.replace(tab, '\t')
	text = text.replace(linefeed,'\n')
	text = text.replace(r,'\r')

	encoder = json.dumps({'tweet_id':tweet_id, 'tweet_time':tweet_time, 'score':score, 'censored_text':text})
	print '%s\t%s' % (user_id, encoder)