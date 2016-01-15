#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os

reload(sys)
sys.setdefaultencoding('utf-8')

lasttid = ''
for line in sys.stdin:
	tid, uid, time, score, text  = line.strip().split('\t')

	if tid != lasttid:
		#uid = (16-len(uid))*'0'+uid
		#key = uid + '+' + time
		date, time = time.split(' ')
		key = uid + ''.join(date.split('-')) +''.join(time.split(':'))
		print '%s\t%s\t%s\t%s' % (key, tid, text, score)

	lasttid = tid