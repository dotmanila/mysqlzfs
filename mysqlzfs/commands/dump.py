#!/bin/env python3

import logging
import os
import re
import signal
import time
from .. import util as zfs_util
from ..commands.mysqld import MysqlZfsService
from ..commands.mysqld_group import MysqlZfsServiceList
from ..constants import *
from collections import OrderedDict
from subprocess import Popen, PIPE

logger = logging.getLogger(__name__)


def is_dump_complete(dump_dir):
    """ Actually, if metadata file exists the dump is deemed complete
    while it is in progress the file is called metadata.partial
    """
    meta_file = os.path.join(dump_dir, 'metadata')
    if not os.path.isfile(meta_file):
        return False

    with open(meta_file) as meta_file_fd:
        for meta in meta_file_fd:
            if 'Finished dump at' in meta:
                return True

    return False


def mydumper_version():
    mydumper = zfs_util.which('mydumper')
    if mydumper is None:
        return None

    try:
        process = Popen([mydumper, '--version'], stdout=PIPE, stderr=PIPE)
        out, err = process.communicate()
        version_string = out.decode('ascii').split(' ')[1].split(',')[0]
        high_or_equal = zfs_util.compare_versions(version_string, '0.9.5')

        return VERSION_EQUAL or VERSION_HIGH
    except ValueError as err:
        return None
    except TypeError as err:
        return None
    except IndexError as err:
        return None


class MysqlDumper(object):
    def __init__(self, opts, zfsmgr):
        self.sigterm_caught = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        if not mydumper_version():
            raise Exception('mydumper minimum version should be 0.9.5')

        self.opts = opts
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
        mysqlds = MysqlZfsServiceList(logger, self.opts, self.zfsmgr)
        if self.opts.snapshot is None:
            self.opts.snapshot = self.zfsmgr.snaps[-1]

        sandbox = mysqlds.scan_sandbox(self.opts.snapshot)

        if sandbox is None:
            if not self.zfsmgr.clone_snapshot():
                raise Exception('Error cloning snapshot')
        elif not sandbox:
            raise Exception('Error checking cloned snapshot')

        mysqld = MysqlZfsService(logger, self.opts)
        if not mysqld.is_alive() and not mysqld.start():
            raise Exception('Could not start source instance')

        logger.info('My own PID %d' % self.opts.ppid)
        logger.info('Starting mydumper service on %s' % mysqld.rootdir)

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
        logger.debug('Running mydumper with %s' % ' '.join(dumper))

        p_dumper = Popen(dumper, stdout=PIPE, stderr=PIPE)
        r = p_dumper.poll()
        poll_count = 0

        dumperpid, piderr = zfs_util.pidno_from_pstree(self.opts.ppid, 'mydumper')
        logger.debug('mydumper pid %s' % str(dumperpid))
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
                        logger.error(err.decode('ascii'))
                    break

                continue
            elif is_dump_complete(dump_dir):
                r = p_dumper.wait()
                break

        mysqld.stop()
        self.zfsmgr.destroy_clone(self.opts.snapshot)

        if r != 0:
            logger.error('mydumper process returned with bad code: %d' % r)
            logger.error('Check error log at %s' % dump_log)
            return False

        logger.info('mydumper process completed')

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
            logger.info('No stored logical dumps found')

        for d in dumps:
            logger.info('Dump set %s' % d)
            logger.info('+-- Status: %s' % dumps[d]['status'])
            logger.info('+-- S3: %s' % dumps[d]['s3'])

