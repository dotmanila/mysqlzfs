#!/bin/env python3

import logging
import os
import re
import signal
import shutil
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


def list_dumps(dumps_base_dir):
    base_dir_files = os.listdir(dumps_base_dir)
    base_dir_files.sort()
    dumps = OrderedDict()

    for d in base_dir_files:
        dump_dir = os.path.join(dumps_base_dir, d)
        if not os.path.isdir(dump_dir):
            continue

        if os.path.isfile(os.path.join(dump_dir, 'metadata')):
            dumps[d] = OrderedDict({'status': 'Complete'})
        elif os.path.isfile(os.path.join(dump_dir, 'metadata.partial')):
            dumps[d] = OrderedDict({'status': 'Incomplete'})
        else:
            dumps[d] = OrderedDict({'status': 'Not Started'})

        if os.path.isfile(os.path.join(dump_dir, 's3metadata')):
            dumps[d]['s3'] = 'Complete'
        elif os.path.isfile(os.path.join(dump_dir, 's3metadata.partial')):
            dumps[d]['s3'] = 'Incomplete'
        else:
            dumps[d]['s3'] = 'Not Started'

    return dumps


def mydumper_version():
    mydumper = zfs_util.which('mydumper')
    if mydumper is None:
        return False

    try:
        process = Popen([mydumper, '--version'], stdout=PIPE, stderr=PIPE)
        out, err = process.communicate()
        version_string = out.decode('ascii').split(' ')[1].split(',')[0]
        high_or_equal = zfs_util.compare_versions(version_string, '0.9.5')

        return True if high_or_equal in [VERSION_EQUAL, VERSION_HIGH] else False
    except ValueError as err:
        return False
    except TypeError as err:
        return False
    except IndexError as err:
        return False


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
        logger.info('Signal caught, cleaning up')

    def prune(self):
        dumps = list_dumps(self.opts.dumpdir)

        if len(dumps) == 0:
            logger.info('No stored logical dumps found')

        dumps_keys = list(dumps.keys())
        prune_list = dumps_keys[0:len(dumps_keys)-self.opts.retention_sets]

        if len(prune_list) == 0:
            logger.info('No old logical dumps to prune')

        for d in prune_list:
            dump_dir = os.path.join(self.opts.dumpdir, d)
            logger.info('Pruning dump %s' % dump_dir)
            shutil.rmtree(dump_dir)

        return True

    def execute_mydumper(self, dump_dir, defaults_file, mysqld_socket):
        mydumper_log = os.path.join(dump_dir, 'mydumper.log')

        dumper = ['mydumper', '-F', '100', '-c', '-e', '-G', '-E', '-R',
                  '--less-locking', '-o', dump_dir, '--threads', str(self.opts.threads),
                  '--socket', mysqld_socket,
                  '--defaults-file', defaults_file,
                  '--logfile', mydumper_log]
        logger.debug('Running mydumper with %s' % ' '.join(dumper))

        p_dumper = Popen(dumper, stdout=PIPE, stderr=PIPE)
        r = p_dumper.poll()
        poll_count = 0

        mydumper_pid, pid_error = zfs_util.pidno_from_pstree(self.opts.ppid, 'mydumper')
        logger.debug('mydumper pid %s' % str(mydumper_pid))
        # print(zfs_util.proc_status(mydumper_pid))

        while r is None:
            time.sleep(2)
            p_dumper.poll()
            poll_count = poll_count + 1

            if not os.path.isfile(mydumper_log):
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

        logger.info('mydumper process completed')

        if r != 0:
            logger.error('mydumper process returned with bad code: %d' % r)
            logger.error('Check error log at %s' % mydumper_log)
            return False

        return True

    def start(self):
        mysqlds = MysqlZfsServiceList(self.opts)
        if self.opts.snapshot is None:
            self.opts.snapshot = self.zfsmgr.snaps[-1]

        sandbox = mysqlds.scan_sandbox(self.opts.snapshot)

        if sandbox is None:
            if not self.zfsmgr.clone_snapshot():
                raise Exception('Error cloning snapshot')
        elif not sandbox:
            raise Exception('Error checking cloned snapshot')

        dump_dir = os.path.join(self.opts.dumpdir, self.opts.snapshot)
        if not os.path.isdir(dump_dir):
            os.mkdir(dump_dir)
        elif is_dump_complete(dump_dir):
            logger.error('The directory %s already has completed dump data' % dump_dir)
            return False

        mysqld = MysqlZfsService(self.opts)
        if not mysqld.is_alive() and not mysqld.start():
            raise Exception('Could not start source instance')

        logger.info('My own PID %d' % self.opts.ppid)
        logger.info('Starting mydumper service on %s' % mysqld.rootdir)

        self.execute_mydumper(dump_dir, defaults_file=self.opts.dotmycnf,
                              mysqld_socket=mysqld.cnf['mysqld']['socket'])
        mysqld.stop()
        self.zfsmgr.destroy_clone(self.opts.snapshot)

        zfs_util.emit_text_metric(
            'mysqlzfs_last_dump{dataset="%s"}' % self.opts.dataset,
            int(time.time()), self.opts.metrics_text_dir)

        self.prune()

        return True

    def status(self):
        dumps = list_dumps(self.opts.dumpdir)

        if len(dumps) == 0:
            logger.info('No stored logical dumps found')

        for d in dumps:
            logger.info('Dump set %s' % d)
            logger.info('+-- Status: %s' % dumps[d]['status'])
            logger.info('+-- S3: %s' % dumps[d]['s3'])

