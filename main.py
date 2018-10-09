# -*- coding: utf-8 -*-
import gevent
from gevent import getcurrent
from gevent import monkey; monkey.patch_all()
from gevent.pool import Pool
from multiprocessing import cpu_count
from qiniu import Auth
from qiniu import BucketManager
import utils
import requests
import os
import time;

def listfiles(bucket, bucket_name, prefix=None, limit=None):
	if bucket is None:
		raise Exception("bucket is None!")
	marker = None
	eof = False
	items = []
	while eof is False:
		ret, eof, info = bucket.list(bucket_name, prefix=prefix, marker=marker, limit=limit)
		marker = ret.get('marker', None)
		items += ret['items']
	if eof is not True:
		raise Exception("list file Error!")
	return items

def downloadfile(index, private_url, file_name):
	time_start = time.time()
	result = False;
	try:
		r = requests.get(private_url)
		if r.status_code == 200:
			file = open(file_name, "wb")
			file.write(r.content)
			file.flush()
			file.close()
			result = True
	except Exception as e:
		pass
	time_end = time.time()
	elapsed = time_end - time_start
	utils.record_log('download: (%s) -> %s, result: %s, time: %ss' % (index, private_url, result, elapsed))
	return result, index, private_url

if __name__ == '__main__':	
	access_key = ''
	secret_key = ''
	bucket_domain = ''
	bucket_name = ''
	path = 'download/'
	if not os.path.exists(path):
		os.makedirs(path)	
	q = Auth(access_key, secret_key)
	bucket = BucketManager(q)
	items = listfiles(bucket, bucket_name)
	utils.record_log('total files: %s' % len(items))
	results = []
	jobs = []
	p = Pool(cpu_count() * 2)
	time_start = time.time()
	for index, item in enumerate(items):
		key = item['key']
		if '/' in key:
			key_path = path + key[:key.rfind('/')]
			if not os.path.exists(key_path):
				os.makedirs(key_path)
		base_url = 'http://%(domain)s/%(key)s' % {"domain": bucket_domain, "key": key}
		private_url = q.private_download_url(base_url, expires=3600)
		jobs.append(p.spawn(downloadfile, index, private_url, path + key))
	results = gevent.joinall(jobs)
	time_end = time.time()
	elapsed = time_end - time_start
	utils.record_log('finished: %ss' % elapsed)
	successful = 0
	unsuccessful = 0
	for result in results:
		if result.successful() and result.value[0] is True:
			successful+=1
		else:
			unsuccessful+=1
			lists.append('%s -> %s' % (result.value[1], result.value[2]))
	utils.record_log('total: %s, success: %s, unsuccessful: %s' % (len(jobs), successful, unsuccessful))
