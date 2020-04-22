#!/bin/env python3

import os
import re
import signal
from .. import util as zfs_util
from ..commands.mysqld import MysqlZfsService
from ..commands.mysqld_group import MysqlZfsServiceList
from ..constants import *
from collections import OrderedDict
from subprocess import Popen, PIPE

class MysqlDumper(object):
    def __init__(self, opts, logger, zfsmgr):
        self.sigterm_caught = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.opts = opts
        self.logger = logger
        self.mysqld = None
        self.zfsmgr = zfsmgr

        self.lockfile = '/tmp/mysqlzfs-dump-%s.lock' % (re.sub('/', '_', self.opts.dataset))
        self.is_running, self.pid = zfs_util.read_lock_file(self.lockfile)

        if self.is_running:
            raise Exception('Another dump process is running with PID %d' % self.pid)

        zfs_util.write_lock_file(self.lockfile, self.opts.ppid)

    def _signal_handler(self, signal, frame):
        self.sigterm_caught = True

    def start(self):
        mysqlds = MysqlZfsServiceList(self.logger, self.opts, self.zfsmgr)
        if self.opts.snapshot is None:
            self.opts.snapshot = self.zfsmgr.snaps[-1]

        sandbox = mysqlds.scan_sandbox(self.opts.snapshot)

        if sandbox is None:
            if not self.zfsmgr.clone_snapshot():
                raise Exception('Error cloning snapshot')
        elif not sandbox:
            raise Exception('Error checking cloned snapshot')

        mysqld = MysqlZfsService(self.logger, self.opts)
        if not mysqld.is_alive() and not mysqld.start():
            raise Exception('Could not start source instance')

        self.logger.info('My own PID %d' % self.opts.ppid)

        self.logger.info('Starting mydumper service on %s' % mysqld.rootdir)

        dump_dir = os.path.join(self.opts.dumpdir, self.opts.snapshot)
        meta_file = os.path.join(dump_dir, 'metadata')
        dump_log = os.path.join(dump_dir, 'mydumper.log')
        if not os.path.isdir(dump_dir):
            os.mkdir(dump_dir)
        dumper = ['mydumper', '-F', '100', '-c', '-e', '-G', '-E', '-R',
                  '--less-locking', '-o', dump_dir, '--threads', str(self.opts.threads),
                  '--socket', mysqld.cnf['mysqld']['socket'],
                  '--defaults-file', self.opts.dotmycnf,
                  '--logfile', dump_log]
        self.logger.debug('Running mydumper with %s' % ' '.join(dumper))

        p_dumper = Popen(dumper, stdout=PIPE, stderr=PIPE)
        r = p_dumper.poll()
        poll_count = 0

        dumperpid, piderr = zfs_util.pidno_from_pstree(self.opts.ppid, 'mydumper')
        self.logger.debug('mydumper pid %s' % str(dumperpid))
        # print(zfs_util.proc_status(dumperpid))

        while r is None:
            time.sleep(2)
            p_dumper.poll()
            poll_count = poll_count + 1

            if not os.path.isfile(dump_log):
                if poll_count > 2:
                    out, err = p_dumper.communicate()
                    r = p_dumper.returncode
                    if err.decode('ascii') != '':
                        self.logger.error(err.decode('ascii'))
                    break

                continue
            elif os.path.isfile(meta_file):
                with open(meta_file) as meta_file_fd:
                    for meta in meta_file_fd:
                        if 'Finished dump at' in meta:
                            r = p_dumper.wait()
                            meta_file_fd.close()
                            break

                meta_file_fd.close()

        mysqld.stop()
        self.zfsmgr.destroy_clone(self.opts.snapshot)

        if r != 0:
            self.logger.error('mydumper process returned with bad code: %d' % r)
            self.logger.error('Check error log at %s' % dump_log)
            return False

        self.logger.info('mydumper process completed')

        zfs_util.emit_text_metric(
            'mysqlzfs_last_dump{dataset="%s"}' % self.opts.dataset,
            int(time.time()), self.opts.metrics_text_dir)

        return True

    def status(self):
        lsout = os.listdir(self.opts.dumpdir)
        dumps = OrderedDict()

        for d in lsout:
            dumpdir = os.path.join(self.opts.dumpdir, d)
            if not os.path.isdir(dumpdir):
                continue

            if os.path.isfile(os.path.join(dumpdir, 'metadata')):
                dumps[d] = OrderedDict({'status': 'Complete'})
            elif os.path.isfile(os.path.join(dumpdir, 'metadata.partial')):
                dumps[d] = OrderedDict({'status': 'Incomplete'})
            else:
                dumps[d] = OrderedDict({'status': 'Not Started'})

            if os.path.isfile(os.path.join(dumpdir, 's3metadata')):
                dumps[d]['s3'] = 'Complete'
            elif os.path.isfile(os.path.join(dumpdir, 's3metadata.partial')):
                dumps[d]['s3'] = 'Incomplete'
            else:
                dumps[d]['s3'] = 'Not Started'

        if len(dumps) == 0:
            self.logger.info('No stored logical dumps found')

        for d in dumps:
            self.logger.info('Dump set %s' % d)
            self.logger.info('+-- Status: %s' % dumps[d]['status'])
            self.logger.info('+-- S3: %s' % dumps[d]['s3'])

    @staticmethod
    def is_dump_complete(dumpdir):
        """ Actually, if metadata file exists the dump is deemed complete
        while it is in progress the file is called metadata.partial
        """
        meta_file = os.path.join(dumpdir, 'metadata')
        if os.path.isfile(meta_file):
            with open(meta_file) as meta_file_fd:
                for meta in meta_file_fd:
                    if 'Finished dump at' in meta:
                        return True

        return False

