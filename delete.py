# -*- coding: utf-8 -*-

#!/bin/python

import os
import sys
import json
import threading
import time
import Queue

from optparse import OptionParser

import boto
import boto.s3.connection
from boto.s3.key import Key


class DeleteBucket:
    args = None
    DEBUG = 0
    NUM_THREAD = 100
    NUM_DELETE_PER_JOB = 1000

    def __init__(self, conn=None, target=None):
        if conn:
            self.conn = conn
        else:
            parser = OptionParser("""
 python delete.py -f [CONFIG] -t [TARGET] -b [BUCKET]
""")
            parser.add_option('-f', '--config', type="str", dest="config", default="config.json", help="config file path. Default is './config.json'")
            parser.add_option('-t', '--target', type="str", dest="target", default="s3", help="target to request. Default is 's3'")
            parser.add_option('-v', '--verbose', action="store_true", default=False, help="verbose on/off.")
            parser.add_option('-b', '--bucket', type="str", dest="bucket", help="arguments")
            parser.add_option('-s', '--ssl', action="store_true", default=False, help="using https connect.")

            options, args = parser.parse_args()

            self.conn = None
            self.conf = None
            self.bucket = options.bucket

            with open(options.config, 'rb') as file:
                conf = json.loads(file.read())

            _conf = conf["TARGET"][options.target]
            self.NUM_THREAD = int(conf["DEFAULT"]["NUM_THREAD"])
            self.NUM_DELETE_PER_JOB = int(conf["DEFAULT"]["NUM_DELETE_PER_JOB"])

            if options.target == 's3':
                self.conn = self.getConnectS3(aws_access_key_id=_conf['AWS_ACCESS_KEY'], aws_secret_access_key=_conf['AWS_ACCESS_SECRET_KEY'])
            elif not options.ssl:
                self.conn = self.getConnectECS_http(user=_conf['USER'], pw=_conf['PW'], host=_conf['HOST'], port=_conf['PORT'])
            else:
                self.conn = self.getConnectECS_https(user=_conf['USER'], pw=_conf['PW'], host=_conf['HOST'], port=_conf['PORT'])
            print '>> ' + str(self.conn)

            if options.verbose:
                 self.DEBUG = 2

    def getConnectS3(self, aws_access_key_id, aws_secret_access_key):
        return boto.connect_s3(aws_access_key_id, aws_secret_access_key)

    def getConnectECS_http(self, user, pw, host, port):
        print '>> http connecting...'
        conn = boto.connect_s3(#debug=2,
            aws_access_key_id=user,
            aws_secret_access_key=pw,
            port=int(port),
            host=host,
            is_secure=False,
            calling_format='boto.s3.connection.ProtocolIndependentOrdinaryCallingFormat'
        )
        return conn

    def getConnectECS_https(self, user, pw, host, port):
        print '>> https connecting...'
        conn = boto.connect_s3(#debug=2,
            aws_access_key_id=user,
            aws_secret_access_key=pw,
            # port=int(port),
            host=host
            # is_secure=True,
            # calling_format='boto.s3.connection.ProtocolIndependentOrdinaryCallingFormat'
        )
        return conn

    def main(self):
        bucket_name = self.bucket

        queue = Queue.Queue()

        _bucket = self.conn.get_bucket(bucket_name)
        resultset = _bucket.list()

        for seq in range(self.NUM_THREAD):
            t = DeleteJob(queue)
            t.start()

        for idx in resultset:
            queue.put({'bucket':_bucket, 'dlist': [idx.name]})

        queue.join()

        for seq in range(self.NUM_THREAD):
            queue.put({'stop': True})

        #print _bucket.delete()

class DeleteJob(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            queue = self.queue.get()

            if queue['stop']:
                self.queue.task_done()
                break

            try:
                result = queue['bucket'].delete_keys(queue['dlist'])
                print '%s %s' % (self, result)
            except:
                for key in queue['dlist']:
                    result = queue['bucket'].delete_key(key)
                    print '%s %s' % (self, result)
            self.queue.task_done()


if __name__ == '__main__':
    start = time.time()
    DeleteBucket().main()
    print '%d sec(s) elapsed..' % (time.time() - start)

