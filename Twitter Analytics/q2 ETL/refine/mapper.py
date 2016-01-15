#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import os

reload(sys)
sys.setdefaultencoding('utf-8')

for line in sys.stdin:
	text,tid,score,uid,time  = line.strip().split('\t')
	print '%s\t%s\t%s\t%s\t%s' % (tid, uid, time, score, text)