#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import time
import urllib2
import re
import json

def generate_afinn_list():
	afinn_file = urllib2.urlopen("https://cmucc-datasets.s3.amazonaws.com/15619/f15/afinn.txt").read()
	afinn = dict()
	lines = afinn_file.split('\n')
	for line in lines:
		l = line.strip().split('\t')
		if len(l) == 2:
			word = l[0]
			score = int(l[1])
			afinn[word] = score
	return afinn


def generate_banned_list():
	banned_file = urllib2.urlopen("https://cmucc-datasets.s3.amazonaws.com/15619/f15/banned.txt").read()
	banned = list()
	lines = banned_file.split('\n')
	for line in lines:
		line = line.strip()
		#ban = ''
		#for c in line:
			#ban += ROT(c)
		banned.append(''.join(ROT(c) for c in line))
	return banned


def ROT(c):
	if (c >= 'A' and c <= 'M') or (c >= 'a' and c <= 'm'):
		c = chr(ord('n')-ord('a')+ord(c))
	elif (c >= 'N' and c <= 'Z') or (c >= 'n' and c <= 'z'):
		c = chr(ord('a')-ord('n')+ord(c))	
	return c


def calculate_score(tweet):
	words = pattern.findall(tweet)
	#score = 0
	#for word in words:
	#	if word in afinn_list:
	#		score += afinn_list[word]
	score = sum(afinn_list[word.lower()] for word in words if word.lower() in afinn_list)
	return score


def censor(tweet):
	words = pattern_ban.split(tweet)
	censored_tweet = ''
	for word in words:
		if word.lower() in banned_list:
			word = word[0] + '*'*(len(word)-2) + word[-1]
		censored_tweet += word
	return censored_tweet



def main():
	last = ''
	for line in sys.stdin:
		try:
			line = line.strip()
			if line == '' or line == last:
				continue
			last = line
			#tweet_id, user_id, tweet_time, followers_count, text = line.split('\t')
			line = line.strip()
			data = json.loads(line)

			user_id = data['user_id']
			tweet_id = data['tweet_id']
			tweet_time = data['tweet_time']
			text = data['text']
			followers_count = data['followers_count']

			sentiment_score = calculate_score(text)
			if sentiment_score == 0:
				continue
			censored_text = censor(text)
			try:
				followers_count = int(followers_count)
			except ValueError:
				continue
			score = sentiment_score*(1+followers_count)
			#print '%s\t%s\t%s\t%s\t%s' % (tweet_id, user_id, tweet_time, score, censored_text) 
			encoder = json.dumps({'tweet_id':tweet_id, 'user_id':user_id, 'tweet_time':tweet_time, 'score':score, 'censored_text':censored_text})
			print encoder
		except:
			sys.stderr.write("Reducer unexpected error: " + str(sys.exc_info()[0]) + "\n")
			sys.stderr.write(line+'\n')
			sys.stderr.write(tweet_id+' '+user_id+' '+tweet_time+' '+followers_count+' '+text+'\n')
        	pass


if __name__ == '__main__':
	reload(sys)
	sys.setdefaultencoding('utf-8')
	pattern = re.compile(r'[a-zA-Z0-9]+')
	pattern_ban = re.compile('([a-zA-Z0-9]+)')
	afinn_list = generate_afinn_list()
	banned_list = generate_banned_list()
	main()








