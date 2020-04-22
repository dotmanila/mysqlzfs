#!/bin/env python3

import boto3
import botocore.exceptions
import psutil
import os
import signal
import sys
import re
import time
from .. import util as zfs_util
from .. import aws
from ..constants import *
from .dump import MysqlDumper
from collections import OrderedDict
from multiprocessing import Pool, TimeoutError


class MysqlS3Client(object):
    def __init__(self, logger, opts):
        self.sigterm_caught = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.opts = opts
        self.logger = logger
        self.bucket = 'mysqlzfs-us-east-1'
        self.s3 = boto3.client('s3')
        self.ts_start = time.time()
        self.ts_end = None
        self.ignorelist = ['metadata', 'metadata.partial', 's3metadata', 's3metadata.partial']
        self.lockfile = '/tmp/mysqlzfs-s3-%s.lock' % (re.sub('/', '_', self.opts.dataset))
        self.is_running, self.pid = zfs_util.read_lock_file(self.lockfile)

        if self.is_running:
            raise Exception('Another S3 upload process is running with PID %d' % self.pid)

        zfs_util.write_lock_file(self.lockfile, self.opts.ppid)

        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as err:
            if int(err.response['Error']['Code']) == 404:
                self.logger.info('Bucket %s does not exist, creating' % self.bucket)
                self.s3.create_bucket(ACL='private', Bucket=self.bucket)
            else:
                self.logger.debug(err.response)
                self.logger.error('S3 Client Error %s' % str(err))
                sys.exit(1)
        except botocore.exceptions.NoCredentialsError as err:
            self.logger.error('S3 Client Error %s' % str(err))
            sys.exit(1)

    def _signal_handler(self, signal, frame):
        self.sigterm_caught = True

    def reap_children(self, timeout=3):
        """ Tries hard to terminate and ultimately kill all the children of this process. """
        def on_terminate(proc):
            print("process {} terminated with exit code {}".format(proc, proc.returncode))

        procs = psutil.Process(self.opts.ppid).children()
        # send SIGTERM
        for p in procs:
            try:
                p.terminate()
            except psutil.NoSuchProcess as err:
                pass
        gone, alive = psutil.wait_procs(procs, timeout=timeout, callback=on_terminate)
        if alive:
            # send SIGKILL
            for p in alive:
                print("process {} survived SIGTERM; trying SIGKILL".format(p))
                try:
                    p.kill()
                except psutil.NoSuchProcess as err:
                    pass
            gone, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
            if alive:
                # give up
                for p in alive:
                    print("process {} survived SIGKILL; giving up".format(p))

    def upload_file(self, job):
        bucket, fname, s3key = job
        ts_start = time.time()
        open('%s.s3part' % fname, 'a').close()
        uploaded = aws.s3_upload(bucket, fname, s3key)
        if not uploaded:
            return False, '%s %s' % (s3key, str(err))

        os.unlink('%s.s3part' % fname)
        return True, '%s took %fs' % (s3key, round(time.time()-ts_start))

    def upload_chunks(self, s3list):
        s3pool = Pool(self.opts.threads)
        results = s3pool.map_async(s3_upload, s3list)

        while not results.ready():
            if MYSQLZFS_SIGTERM_CAUGHT:
                self.reap_children()
                self.logger.info('Cleaned up children processes')
                psutil.Process(self.opts.ppid).kill()
                self.logger.info('Closing S3 upload pool')
                # Nothing gets executed beyond this point

                s3pool.close()
                s3pool.terminate()
                s3pool.join()
                self.logger.info('Terminated S3 upload pool')
                break

            self.logger.debug('Waiting for results')
            time.sleep(5)

        s3pool.close()
        s3pool.join()

        try:
            results = results.get(1)
            for result in results:
                success, message = result
                if not success:
                    self.logger.info(message)
                else:
                    self.logger.debug(message)
        except TimeoutError as err:
            pass

    def upload_dumps(self):
        snapshot = None
        source_dir = None

        if self.opts.snapshot is not None:
            source_dir = os.path.join(self.opts.dumpdir, self.opts.snapshot)
            if not MysqlDumper.is_dump_complete(source_dir):
                self.logger.error('%s is not valid dump directory' % source_dir)
                return False

            if self.is_upload_complete(source_dir):
                self.logger.info('%s upload is already complete, exiting' % source_dir)
                return True

            snapshot = self.opts.snapshot
        else:
            dumpdirs = os.listdir(self.opts.dumpdir)
            dumpdirs.sort()
            dumpdirs.reverse()
            for dumpdir in dumpdirs:
                snapshot = dumpdir
                source_dir = os.path.join(self.opts.dumpdir, dumpdir)

                if MysqlDumper.is_dump_complete(source_dir):
                    if not self.is_upload_complete(source_dir):
                        break

                    self.logger.info('%s upload is already complete, skipping' % source_dir)
                    source_dir = None
                else:
                    self.logger.warn('%s is an incomplete dump, skipping' % source_dir)
                    source_dir = None

        if source_dir is None:
            self.logger.info('No valid dump directory found to upload')
            return True

        dumpfiles = os.listdir(source_dir)
        dumpfiles.sort()
        s3key = '%s/%s/dump' % (socket.getfqdn(), self.opts.dataset)
        s3list = []
        partially_uploaded = False

        self.write_metadata(source_dir)
        for dumpfile in dumpfiles:
            if dumpfile in self.ignorelist:
                continue

            upload_file = os.path.join(source_dir, dumpfile)
            schema = dumpfile.split('.')

            # Less than 3 parts of filename sep with . is a file, and not a
            # table dump
            if len(schema) < 4:
                upload_key = '%s/%s/%s' % (s3key, snapshot, dumpfile)
            else:
                upload_key = '%s/%s/%s/%s' % (s3key, snapshot, schema[0], dumpfile)

            # If we hit a filename with *.s3part we discard whatever we have so far
            # it means an S3 upload previously failed up to this file and we
            # only need to resume from there
            if re.search('\.s3(last|part)$', dumpfile):
                if not partially_uploaded:
                    self.logger.info('Resuming a previously failed upload')
                    s3list = []
                    partially_uploaded = True
                    # We do this because the actual file precedes the current
                    # position and we do not want to skip it.
                    s3list.append((self.bucket, upload_file[:-7], upload_key[:-7], ))
                continue

            s3list.append((self.bucket, upload_file, upload_key, ))

        objects = len(s3list)
        self.logger.info('Uploading %d objects' % objects)
        # We upload in batches of 32 per pool to get nearer feedback
        chunks = 0
        s3list = [s3list[i:i + 32] for i in xrange(0, objects, 32)]
        for chunk in s3list:
            self.upload_chunks(chunk)
            chunks += 32

            if chunks >= objects:
                print(' ')
            else:
                print(chunks,)

        self.ts_end = time.time()
        self.write_metadata(source_dir)

    def scandir_binlog_hosts(self):
        hostdirs = []
        lsout = os.listdir(self.opts.binlogdir)

        if len(lsout) == 0:
            return hostdirs

        lsout.sort()

        for host in lsout:
            if not os.path.isdir(os.path.join(self.opts.binlogdir, host)):
                continue

            hostdirs.append(host)

        return hostdirs

    def scandir_binlog_days(self, host):
        binlogdays = []
        lsout = os.listdir(os.path.join(self.opts.binlogdir, host))

        if len(lsout) == 0:
            return hostdirs

        lsout.sort()

        for daydir in lsout:
            binlogdir = os.path.join(self.opts.binlogdir, host, daydir)

            if self.is_upload_complete(binlogdir):
                self.logger.debug('%s upload is already complete, skipping' % binlogdir)
                continue

            if not os.path.isdir(binlogdir):
                continue

            # daydir should be datetime format
            if not re.search('^20\d{6}$', daydir):
                self.logger.debug('%s does not match, skipping' % binlogdir)
                continue

            binlogdays.append(daydir)

        return binlogdays

    def scandir_binlogs(self, host, day):
        binlogdir = os.path.join(self.opts.binlogdir, host, day)
        self.logger.debug('Scanning %s' % binlogdir)
        lsout = os.listdir(binlogdir)
        lsout.sort()
        partially_uploaded = False
        binlogs = []
        s3key = '%s/%s/binlog' % (socket.getfqdn(), self.opts.dataset)

        for binlog in lsout:
            if binlog in self.ignorelist:
                continue

            upload_file = os.path.join(binlogdir, binlog)
            upload_key = '%s/%s/%s' % (s3key, day, binlog)

            if re.search('\.s3(last|part)$', binlog):
                if not partially_uploaded:
                    binlogs = []
                    partially_uploaded = True
                    binlogs.append((self.bucket, upload_file[:-7], upload_key[:-7], ))
                continue

            binlogs.append((self.bucket, upload_file, upload_key, ))

        return binlogs

    def upload_binlogs_host(self, host):
        self.logger.info('Scanning for binlog from %s' % host)
        binlogdays = self.scandir_binlog_days(host)
        s3last = None

        if len(binlogdays) == 0:
            self.logger.info('No binlog directories to upload from %s' % host)
            return True

        s3list = OrderedDict()
        for day in binlogdays:
            binlogdir = os.path.join(self.opts.binlogdir, host, day)
            binlogs = self.scandir_binlogs(host, day)

            self.logger.info('Uploading %d objects from %s/%s' % (len(binlogs), host, day))
            # print(binlogs)
            self.upload_chunks(binlogs)

            s3bucket, s3file, s3key = binlogs[0]
            if os.path.isfile('%s.s3last' % s3file):
                os.unlink('%s.s3last' % s3file)

            # Make sure to mark our last binlog to know where we resume next time
            s3bucket, s3file, s3key = binlogs[-1]
            open('%s.s3last' % s3file, 'a').close()

            if s3last is not None and os.path.isfile(s3last):
                os.unlink(s3last)

            s3last = '%s.s3last' % s3file

            self.ts_end = time.time()
            self.write_metadata(binlogdir)

        return True

    def upload_binlogs(self):
        hostdirs = self.scandir_binlog_hosts()
        if len(hostdirs) == 0:
            self.logger.error('No binlog directories to upload at this time')
            return False

        for host in hostdirs:
            self.upload_binlogs_host(host)

    def read_metadata(self, dumpdir):
        metadata = OrderedDict()
        partial = False
        check_file = os.path.join(dumpdir, 's3metadata')

        if os.path.isfile(check_file):
            meta_file = check_file
        else:
            check_file = os.path.join(dumpdir, 's3metadata.partial')
            if os.path.isfile(check_file):
                meta_file = check_file
                partial = True
            else:
                return None

        with open(meta_file, 'r') as meta_fd:
            for meta in meta_fd:
                k, v = meta.split(':')
                metadata[k] = v
            meta_fd.close()

        if partial:
            metadata['ts_end'] = None
            metadata['dt_end'] = None

        return metadata

    def write_metadata(self, dumpdir):
        meta_file = os.path.join(dumpdir, 's3metadata')
        meta_file_partial = os.path.join(dumpdir, 's3metadata.partial')

        if self.ts_end is not None:
            with open(meta_file, 'w') as metafd:
                metafd.write('dt_start:%s\n' % zfs_util.tsftime(self.ts_start, '%Y_%m_%d-%H_%M_%S'))
                metafd.write('ts_start:%s\n' % self.ts_start)
                metafd.write('dt_end:%s\n' % zfs_util.tsftime(self.ts_end, '%Y_%m_%d-%H_%M_%S'))
                metafd.write('ts_end:%s\n' % self.ts_end)
            metafd.close()

            if os.path.isfile(meta_file_partial):
                os.unlink(meta_file_partial)
        else:
            with open(meta_file_partial, 'w') as metafd:
                metafd.write('dt_start:%s\n' % zfs_util.tsftime(self.ts_start, '%Y_%m_%d-%H_%M_%S'))
                metafd.write('ts_start:%s\n' % self.ts_start)
            metafd.close()

            if os.path.isfile(meta_file):
                os.unlink(meta_file)

        return True

    def is_upload_complete(self, srcdir):
        """ Check if a particular cirectory we are upload has already
        been uploaded to S3
        """
        if os.path.isfile(os.path.join(srcdir, 's3metadata')):
            s3metadata = self.read_metadata(srcdir)
            if s3metadata is None or s3metadata['ts_end'] is None:
                return False
        else:
            return False

        return True
