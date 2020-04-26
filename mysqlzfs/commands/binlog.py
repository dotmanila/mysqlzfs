#!/bin/env python3

import logging
import os
import re
import shutil
import signal
import struct
import time
import traceback
from .. import util as zfs_util
from ..constants import *
from ..mysql import MySQLClient
from ..packet import ___write_events, __event_map
from datetime import datetime
from mysql.connector.errors import Error as MySQLError
from subprocess import Popen, PIPE

logger = logging.getLogger(__name__)


def mysqlbinlog_version():
    mysqlbinlog = zfs_util.which('mysqlbinlog')
    if mysqlbinlog is None:
        return None

    try:
        process = Popen([mysqlbinlog, '--version'], stdout=PIPE, stderr=PIPE)
        out, err = process.communicate()
        version_string = out.decode('ascii').split(' ')[1].split(',')[0]
        high_or_equal = zfs_util.compare_versions(version_string, '3.4')

        return VERSION_EQUAL or VERSION_HIGH
    except ValueError as err:
        return None
    except TypeError as err:
        return None
    except IndexError as err:
        return None


class MysqlBinlogStreamer(object):
    """ In a perfect world, we connect directly to the source and decode the
    packets ourselves, i.e. overloading mysql-replication's BinlogStreamReader
    https://github.com/noplay/python-mysql-replication/blob/master/pymysqlreplication/binlogstream.py
    this would allow us to monitor as binlog rotate on the source and track metadata
    such as GTID, checksums, etc. Some concerns could be slowdown and too complex
    for now...

    We simply keep N amount of binlogs based on their creation dates
    If a binlog source changes, we take the max age of binlog or based on 1month retention
    Binlogs will be stored based on day:
    +-- hostname
      +-- 20191125
         +-- 20191125030511.log-bin.00034.gz
    The timestamp on the binlog filename is the rotation timestamp of that binlog
    """

    def __init__(self, opts):
        self.sigterm_caught = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.mysql_client = None
        self.opts = opts
        self.is_running = False
        self.lockfile = os.path.join(self.opts.binlogdir, 'mysqlbinlog.lock')
        self.pid = None
        self.srv_hostname = None
        self.srv_binlog_first = None
        self.srv_binlog_last = None
        self.ses_binlog_next = None
        self.ses_binlog_last = None
        self.srv_binlog_prefix = None
        self.srv_connect_ctl()
        self.read_server_metadata()
        self.ses_binlog_lst_prefix = 'lst_._%s_._%s' % (self.srv_hostname, self.srv_binlog_prefix)
        self.ses_binlog_lst_file = os.path.join(self.opts.binlogdir, self.ses_binlog_lst_prefix)
        self.ses_binlog_lst_bin = None
        self.ses_binlog_tmp_prefix = 'tmp_._%s_._%s' % (self.srv_hostname, self.srv_binlog_prefix)
        self.binlogdir = os.path.join(self.opts.binlogdir, self.srv_hostname)
        if not os.path.isdir(self.binlogdir):
            os.mkdir(self.binlogdir)

        self.binzip = zfs_util.which('pigz')
        if self.binzip is None:
            logger.info('pigz is not available, consider installing '
                             'for faster compressions')
            self.binzip = zfs_util.which('gzip')
            if self.binzip is None:
                logger.info('gzip is not available, consider installing '
                                 'pigz for faster compressions')

    def _signal_handler(self, signal, frame):
        self.sigterm_caught = True
        logger.info('Signal caught, cleaning up')

    def srv_connect_ctl(self):
        try:
            logger.debug('Establishing control connection to MySQL with %s' % self.opts.dotmycnf)
            defaults_file = zfs_util.read_config_file(self.opts.dotmycnf)
            if defaults_file is None:
                raise Exception('mysqlbinlog section is not available from .my.cnf')
            elif not defaults_file.has_option('mysqlbinlog', 'host'):
                raise Exception('mysqlbinlog section requires at least host value')

            params = {'option_files': self.opts.dotmycnf,
                      'option_groups': 'mysqlbinlog'}

            self.mysql_client = MySQLClient(params=params)
        except MySQLError as err:
            raise Exception('Could not establish connection to MySQL server')

    def start(self):
        mysqlbinlog = zfs_util.which('mysqlbinlog')
        if mysqlbinlog is None:
            raise Exception('mysqlbinlog command not found')

        if not mysqlbinlog_version():
            raise Exception('Required mysqlbinlog version should be 3.4 or higher')

        self.is_running, self.pid = zfs_util.read_lock_file(self.lockfile)
        if self.is_running:
            logger.error('mysqlbinlog process still running with PID %s' % self.pid)
            return False
        else:
            self.pid = None

        logger.info('Starting session pre-cleanup')
        self.session_cleanup()

        self.ses_binlog_next = self.find_next_binlog()

        # Adding compress seems to be buggy on mysqlbinlog
        mysqlbinlog_command = [mysqlbinlog,
                               "--defaults-file=%s" % self.opts.dotmycnf,
                               "--read-from-remote-master=BINLOG-DUMP-GTIDS",
                               "--stop-never-slave-server-id=%s" % str(int(time.time())),
                               "--raw", "--stop-never", "--result-file=%s_._%s_._" % (
                                   'tmp', self.srv_hostname),
                               self.ses_binlog_next]

        logger.info(' '.join(mysqlbinlog_command))

        try:
            os.chdir(self.opts.binlogdir)
            end_ts = time.time() + 3585.0
            cleanup_int = 0

            dev_null = None
            if self.opts.debug:
                p = Popen(mysqlbinlog_command)
            else:
                dev_null = open(os.devnull, 'w')
                p = Popen(mysqlbinlog_command, stdout=dev_null, stderr=dev_null)

            r = p.poll()

            self.pid = self.wait_for_pid()
            logger.debug('mysqlbinlog PID %s' % str(self.pid))

            if self.pid is None:
                logger.error('Timed out waiting for mysqlbinlog pid')
                return False

            zfs_util.write_lock_file(self.lockfile, self.pid)

            while time.time() < end_ts:
                # Cleanup every minute
                if cleanup_int % 60 == 0:
                    self.session_cleanup(True)

                r = p.poll()
                if r is not None:
                    break

                if self.sigterm_caught:
                    logger.info('Cleaning up mysqlbinlog process')
                    break

                time.sleep(10)
                cleanup_int += 10

            if r is None:
                p.kill()
            elif r != 0:
                logger.error("mysqlbinlog exited error code %s" % str(r))
                zfs_util.emit_text_metric(
                    'mysqlzfs_last_binlogd_error{dataset="%s"}' % self.opts.dataset,
                    int(time.time()), self.opts.metrics_text_dir)

            if dev_null is not None:
                dev_null.close()

            os.chdir(self.opts.pcwd)
        except Exception as err:
            logger.error("mysqlbinlog died with error %s" % str(err))
            zfs_util.emit_text_metric(
                'mysqlzfs_last_binlogd_error{dataset="%s"}' % self.opts.dataset,
                int(time.time()), self.opts.metrics_text_dir)
            raise

        logger.info('Starting session post-cleanup')
        self.session_cleanup()

        zfs_util.emit_text_metric(
            'mysqlzfs_last_binlogd_ok{dataset="%s"}' % self.opts.dataset,
            int(time.time()), self.opts.metrics_text_dir)

        return True

    def wait_for_pid(self):
        sleeps = 10
        while sleeps > 0:
            pidno, err = zfs_util.pidno_from_pstree(self.opts.ppid, 'mysqlbinlog')
            if pidno is not None:
                return pidno

            time.sleep(1)
            sleeps = sleeps - 1

        return None

    def session_cleanup(self, keep_last = False):
        """ The mysqlbinlog process saves binlogs collected for the last hour
        into tmp_<hostname>_<binlogname> into the binlogdir directory. At the end or beginning
        of each session, we clean them up to help identify where we start.
        """
        logger.debug('Starting session cleanup')
        self.session_cleanup_tmp(keep_last)
        self.session_cleanup_lst()
        logger.debug('Session cleanup complete')

    def session_cleanup_tmp(self, keep_last = False):
        binlogs = self.find_session_binlogs()
        if len(binlogs) == 0:
            return True

        last_binlog = binlogs[-1]
        last_binlog_lst = last_binlog.replace('tmp_._', 'lst_._')

        for fn in binlogs:
            fnpath = os.path.join(self.opts.binlogdir, fn)
            fnparts = fn.split('_._')
            fn_created_ts = self.binlog_ts_created(fnpath)
            fndir = os.path.join(self.binlogdir, fn_created_ts[:8])
            fndest = os.path.join(fndir, '%s_._%s' % (str(fn_created_ts), fnparts[2]))

            if os.path.isfile(fndest):
                logger.error('Destination binlog exists, saving as duplicate')
                fndest = os.path.join(fndir, '%s_._%s_._%s' % (str(fn_created_ts), fnparts[2], time.time()))

            fndest_zip = '%s.gz' % fndest
            # We always assume the last binary log is not complete and start the
            # stream from there. But we also do not delete it automatically in case
            # starting the mysqlbinlog daemon fails and we need to determine
            # where to start again. We record it in a meta last file
            if last_binlog == fn:
                if keep_last:
                    break

                with open(self.ses_binlog_lst_file, 'w') as lstfd:
                    lstfd.write(last_binlog)
                lstfd.close()
                last_binlog_lst = os.path.join(self.opts.binlogdir, last_binlog_lst)
                if os.path.isfile(last_binlog_lst):
                    os.unlink(last_binlog_lst)
                os.rename(fnpath, last_binlog_lst)
            else:
                if not os.path.isdir(fndir):
                    os.mkdir(fndir)

                logger.debug('Found %s, moving to %s' % (fn, fndest))
                if not self.zip(fnpath, fndest_zip):
                    if os.path.isfile(fndest_zip):
                        os.unlink(fndest_zip)

                    zfs_util.emit_text_metric(
                        'mysqlzfs_last_binlogd_error{dataset="%s"}' % self.opts.dataset,
                        int(time.time()), self.opts.metrics_text_dir)

                    raise Exception('Compression failed for %s' % fnpath)

        return True

    def session_cleanup_lst(self):
        binlogs = self.find_session_binlogs(self.ses_binlog_lst_prefix)
        if len(binlogs) == 0:
            return True

        next_binlog = self.find_next_binlog()

        for fn in binlogs:
            fnpath = os.path.join(self.opts.binlogdir, fn)
            fnparts = fn.split('_._')
            fn_created_ts = self.binlog_ts_created(fnpath)
            fndir = os.path.join(self.binlogdir, fn_created_ts[:8])
            fndest = os.path.join(fndir, '%s_._%s' % (str(fn_created_ts), fnparts[2]))
            fndest_zip = '%s.gz' % fndest
            fndest_part_zip = '%s.part.gz' % fndest

            if os.path.isfile(fndest_zip):
                logger.debug('Removing stale %s' % fnpath)
                os.unlink(fnpath)
                continue
            else:
                skip = '%s.%s' % (self.ses_binlog_lst_prefix, next_binlog[-6:])

                if skip == fn:
                    logger.debug('%s is last tmp binlog, skipping partial save' % skip)
                    continue

                logger.info('Full binlog %s is missing' % fndest)
                logger.info('Filling with partial %s' % fnpath)

                if not os.path.isdir(fndir):
                    os.mkdir(fndir)

                if os.path.isfile(fndest_part_zip):
                    logger.warn('%s exists, skipping' % fndest_part_zip)
                    logger.warn('Please check this binlog manually')
                    continue

                if not self.zip(fnpath, fndest_part_zip):
                    if os.path.isfile(fndest_zip):
                        os.unlink(fndest_zip)

                    zfs_util.emit_text_metric(
                        'mysqlzfs_last_binlogd_error{dataset="%s"}' % self.opts.dataset,
                        int(time.time()), self.opts.metrics_text_dir)

                    raise Exception('Compression failed for %s' % fnpath)

        return True

    def find_next_binlog(self):
        """ Identify the next binlog to download
        - Check if lst_._ file exists, make sure it still exists on source
        - Check last file from hostname/binlogs, increment by one, make sure it still exists on source
        - Default to first binlog on source
        """
        next_binlog = None
        if os.path.isfile(self.ses_binlog_lst_file):
            with open(self.ses_binlog_lst_file, 'r') as lstfd:
                for fn in lstfd:
                    fnparts = fn.split('_._')
                    break
            lstfd.close()
            if len(fnparts) == 3:
                next_binlog = fnparts[2]
                logger.debug('Binlog from lst file %s' % next_binlog)

        if next_binlog is not None:
            if not self.binlog_exists_on_server(next_binlog):
                logger.info('Recorded last binlog in session, '
                                 'has been purged')
                logger.warn('Potential gap in downloaded binlogs, please review')
                next_binlog = None
            else:
                logger.info('Recorded last binlog in session, '
                                 'streaming will start from %s' % next_binlog)
                return next_binlog

        lsout = os.listdir(self.binlogdir)
        lsout.sort()
        lsout.reverse()
        for lsdir in lsout:
            binlogdir = os.path.join(self.binlogdir, lsdir)
            if os.path.isdir(binlogdir):
                binlogs = self.list_binlogs_from_dir(binlogdir)
                # We only need the last binlog from the newest dir
                if len(binlogs) == 0:
                    continue

                next_binlog = self.normalize_binlog_name(binlogs[-1])
                logger.info('Last binlog based on stored set %s' % next_binlog)

                if not self.binlog_exists_on_server(next_binlog):
                    next_binlog = None
                    break
                else:
                    next_binlog = '%s.%06d' % (next_binlog[:-7], (int(next_binlog[-6:])+1))
                    logger.info('Next binlog is %s' % next_binlog)
                    return next_binlog

        logger.info('No recorded last binlog in session, '
                         'downloading all from source')
        return self.srv_binlog_first

    def normalize_binlog_name(self, binname):
        """ Take the actual binary log name from a file name
        i.e. tmp_._hostname_._mysql-bin.000004.gz -> mysql-bin.000004
        """
        if binname == '':
            return ''

        binlog = binname.split('_._')[-1]
        if '.gz' == binlog[-3:]:
            return binlog[:-3]

        return binlog

    def list_binlogs_from_dir(self, binlogdir):
        lsout = os.listdir(binlogdir)
        lsout.sort()
        binlogs = []
        for fn in lsout:
            # We can check for self.is_binlog_format(os.path.join(binlogdir, fn))
            # too but we are compressing binlogs, have to think of how to do
            # that in the future
            if len(fn.split('_._')) == 2:
                binlogs.append(fn)

        return binlogs

    def binlog_exists_on_server(self, binlog):
        sql = 'SHOW BINLOG EVENTS IN "%s" LIMIT 1' % binlog
        row = None
        try:
            row = self.mysql_client.fetchone(sql)
        except MySQLError as err:
            if err.errno == 2006:
                self.srv_connect_ctl()
                row = self.mysql_client.fetchone(sql)
            else:
                if self.opts.debug:
                    traceback.print_exc()
                logger.error(str(err))
                logger.debug(sql)
                logger.error('Binlog does not exist on server [%s]' % binlog)
                return False

        logger.debug('Binlog exists on server [%s]' % str(row))
        return True

    def find_session_binlogs(self, prefix=None):
        if prefix is None:
            prefix = self.ses_binlog_tmp_prefix

        binlogs = []
        lsout = os.listdir(self.opts.binlogdir)
        if len(lsout) == 0:
            return binlogs

        lsout.sort()
        for fn in lsout:
            fnpath = os.path.join(self.opts.binlogdir, fn)
            if os.path.isdir(fnpath):
                continue

            if re.search('^%s' % prefix, fn) is not None \
                    and self.is_binlog_format(fnpath):
                binlogs.append(fn)

        return binlogs

    def read_server_metadata(self):
        row = self.mysql_client.fetchone('SELECT @@hostname AS hn')
        self.srv_hostname = row['hn']
        row = self.mysql_client.fetchone('SHOW BINARY LOGS')
        self.srv_binlog_first = row['Log_name']
        row = self.mysql_client.fetchone('SHOW MASTER STATUS')
        self.srv_binlog_last = row['File']
        self.srv_binlog_prefix = self.srv_binlog_last[:-7]

    def is_binlog_format(self, fn):
        if open(fn, 'rb').read(4) == '\xfebin':
            return True

        return False

    def byte2int(self, b):
        """ from pymysql.util import byte2int
        """
        if isinstance(b, int):
            return b
        else:
            return struct.unpack("!B", b)[0]

    def binlog_ts_created(self, fn):
        with open(fn, 'rb') as fd:
            fd.seek(4)

            # We look for the first write event in the binlog. mysqlbinlog writes
            # its own timestamp on the first events which is the current time
            # when the binlogs are downloaded.
            while True:
                unpack = struct.unpack('<IcIIIH', fd.read(19))
                fd.seek(unpack[4])
                if not self.byte2int(unpack[1]) in self.___write_events:
                    continue

                break

        d = datetime.fromtimestamp(float(unpack[0]))
        return d.strftime('%Y%m%d%H%M%S')

    def zip(self, binlog, dest):
        if self.binzip is not None:
            cmd_gzip = [self.binzip, binlog]

            p = Popen(cmd_gzip, stdout=PIPE, stderr=PIPE)
            r = p.poll()

            while r is None:
                time.sleep(2)
                r = p.poll()

            if r != 0:
                logger.error('Failed to compress with gzip, '
                                  'command was %s' % str(cmd_gzip))
            else:
                os.rename('%s.gz' % binlog, dest)
                return True

        # We tried the gzip python bindings but it is very slow, we use popen instead
        try:
            with open(binlog, 'rb') as f_in, gzip.open(dest, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out, 33554432*4)

            os.unlink(binlog)

            return True
        except Exception as err:
            logger.error(str(err))
            return False

    def unzip(self, binlog):
        pass
