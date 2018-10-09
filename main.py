# -*- coding: utf-8 -*-
from qiniu import Auth
from qiniu import BucketManager
import utils
import requests
import os

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
	utils.record_log('total files: ' + str(len(items)))
	for index, item in enumerate(items):
		key = item['key']
		# print(key)
		base_url = 'http://%(domain)s/%(key)s' % {"domain": bucket_domain, "key": key}
		# print(base_url)
		private_url = q.private_download_url(base_url, expires=3600)
		# print(private_url)
		r = requests.get(private_url)
		if r.status_code == 200:
			if '/' in key:
				key_path = path + key[:key.rfind('/')]
				if not os.path.exists(key_path):
					os.makedirs(key_path)
			file = open(path + key, "wb")
			file.write(r.content)
			file.flush()
			file.close()
			utils.record_log(str(index) + ": " + private_url + " download success")
		else:
			utils.record_log(private_url + " download failed")
