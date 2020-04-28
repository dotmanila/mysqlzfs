#!/bin/env python3

import gzip
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


def mysqlbinlog_version(required_version='3.4'):
    mysqlbinlog = zfs_util.which('mysqlbinlog')
    if mysqlbinlog is None:
        return False

    try:
        process = Popen([mysqlbinlog, '--version'], stdout=PIPE, stderr=PIPE)
        out, err = process.communicate()
        version_string = out.decode('ascii').split(' ')[2].split(',')[0]
        high_or_equal = zfs_util.compare_versions(version_string, required_version)

        return True if high_or_equal in [VERSION_EQUAL, VERSION_HIGH] else False
    except ValueError as err:
        return False
    except TypeError as err:
        return False
    except IndexError as err:
        return False


def is_binlog_format(fn):
    file_stats = os.stat(fn)
    # If file size is less than 120 bytes, we cannot even decode it
    if file_stats.st_size < 120:
        return False

    with open(fn, 'rb') as binlog_file_fd:
        magic = binlog_file_fd.read(4)
        if magic == b'\xfebin':
            return True

    return False


def list_binlogs_from_dir(binlog_dir):
    binlog_dir_list = os.listdir(binlog_dir)
    binlog_dir_list.sort()
    binary_logs = []
    for fn in binlog_dir_list:
        # We can check for self.is_binlog_format(os.path.join(binlogdir, fn))
        # too but we are compressing binary_logs, have to think of how to do
        # that in the future
        if len(fn.split('_._')) == 2:
            binary_logs.append(fn)

    return binary_logs


def byte2int(b):
    """ from pymysql.util import byte2int
    """
    if isinstance(b, int):
        return b
    else:
        return struct.unpack("!B", b)[0]


def normalize_binlog_name(binname):
    """ Take the actual binary log name from a file name
    i.e. tmp_._hostname_._mysql-bin.000004.gz -> mysql-bin.000004
    """
    if binname == '':
        return ''

    binlog = binname.split('_._')[-1]
    if '.gz' == binlog[-3:]:
        return binlog[:-3]

    return binlog


def binlog_ts_created(fn):
    with open(fn, 'rb') as fd:
        fd.seek(4)

        # We look for the first write event in the binlog. mysqlbinlog writes
        # its own timestamp on the first events which is the current time
        # when the binlogs are downloaded.
        try:
            while True:
                unpack = struct.unpack('<IcIIIH', fd.read(19))
                fd.seek(unpack[4])
                if not byte2int(unpack[1]) in ___write_events:
                    continue

                break
        except struct.error as err:
            raise BinaryLogFormatError('Binary log %s has no valid write events' % fn)

    d = datetime.fromtimestamp(float(unpack[0]))
    return d.strftime('%Y%m%d%H%M%S')


def find_session_binlogs(prefix, binlog_dir):
    binlogs = []
    binlog_path = None
    binlog_dir_list = os.listdir(binlog_dir)

    if len(binlog_dir_list) == 0:
        return binlogs

    binlog_dir_list.sort()
    for binlog in binlog_dir_list:
        binlog_path = os.path.join(binlog_dir, binlog)
        if os.path.isdir(binlog_path):
            continue

        if re.search('^%s' % prefix, binlog) is not None \
                and is_binlog_format(binlog_path):
            binlogs.append(binlog)

    return binlogs


def get_zip_binary(choices=None):
    if choices is None:
        choices = ['pigz','gzip']

    zip_binary = None

    for binary in choices:
        zip_binary = zfs_util.which(binary)
        if zip_binary is None:
            continue

    return zip_binary


def get_mysqlbinlog_binary():
    mysqlbinlog = zfs_util.which('mysqlbinlog')
    if mysqlbinlog is None:
        raise Exception('mysqlbinlog command not found')

    if not mysqlbinlog_version():
        raise Exception('Required mysqlbinlog version should be 3.4 or higher')

    return mysqlbinlog


def wait_for_pid(proc_name, expires=10):
    sleeps = expires
    my_pid = os.getpid()
    while sleeps > 0:
        pidno, err = zfs_util.pidno_from_pstree(my_pid, proc_name)
        if pidno is not None:
            return pidno

        time.sleep(1)
        sleeps = sleeps - 1

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

    sigterm_caught = False
    is_running = False
    mysql_client = None
    zip_binary = None
    pid = None
    srv_hostname = None
    srv_binlog_first = None
    srv_binlog_last = None
    ses_binlog_next = None
    ses_binlog_last = None
    srv_binlog_prefix = None
    lockfile = None
    ses_binlog_lst_prefix = None
    ses_binlog_lst_file = None
    ses_binlog_lst_bin = None
    ses_binlog_tmp_prefix = None
    binlog_dir = None

    def __init__(self, base_binlog_dir, mysql_defaults_file, prometheus_text_dir=None,
                 prometheus_metrics_prefix='mysqlzfs_binlogd'):
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.base_binlog_dir = base_binlog_dir
        self.debug = True if logger.getEffectiveLevel() == logging.DEBUG else False
        self.mysql_defaults_file = mysql_defaults_file
        self.prometheus_text_dir = prometheus_text_dir
        self.prometheus_metrics_prefix = prometheus_metrics_prefix

    def _signal_handler(self, signal, frame):
        self.sigterm_caught = True
        logger.info('Signal caught, cleaning up')

    def init_variables(self):
        self.zip_binary = get_zip_binary()
        if self.zip_binary is None:
            raise Exception('Cannot find a suitable compression binary')
        else:
            logger.info('Using %s to compress binary logs' % self.zip_binary)

        self.lockfile = os.path.join(self.base_binlog_dir, 'mysqlbinlog.lock')
        self.srv_connect_ctl()
        self.read_server_metadata()
        self.ses_binlog_lst_prefix = 'lst_._%s_._%s' % (self.srv_hostname, self.srv_binlog_prefix)
        self.ses_binlog_lst_file = os.path.join(self.base_binlog_dir, self.ses_binlog_lst_prefix)
        self.ses_binlog_lst_bin = None
        self.ses_binlog_tmp_prefix = 'tmp_._%s_._%s' % (self.srv_hostname, self.srv_binlog_prefix)
        self.binlog_dir = os.path.join(self.base_binlog_dir, self.srv_hostname)

        if not os.path.isdir(self.binlog_dir):
            os.mkdir(self.binlog_dir)

    def write_prometheus_metric(self, key, value):
        zfs_util.emit_text_metric('%s_%s{hostname="%s"}' % (self.prometheus_metrics_prefix,
                                                            key, self.srv_hostname),
                                  value, self.prometheus_text_dir)

    def srv_connect_ctl(self):
        try:
            logger.debug('Establishing control connection to MySQL with %s' % self.mysql_defaults_file)
            defaults_file = zfs_util.read_config_file(self.mysql_defaults_file)
            if defaults_file is None:
                raise Exception('mysqlbinlog section is not available from .my.cnf')
            elif not defaults_file.has_option('mysqlbinlog', 'host'):
                raise Exception('mysqlbinlog section requires at least host value')

            params = {'option_files': self.mysql_defaults_file,
                      'option_groups': 'mysqlbinlog'}

            self.mysql_client = MySQLClient(params=params)
        except MySQLError as err:
            raise Exception('Could not establish connection to MySQL server')

    def start(self):
        self.init_variables()
        mysqlbinlog = get_mysqlbinlog_binary()
        self.is_running, self.pid = zfs_util.read_lock_file(self.lockfile)
        if self.is_running:
            logger.error('mysqlbinlog process still running with PID %s' % self.pid)
            return False
        else:
            self.pid = None

        logger.info('Running session initial cleanup')
        self.session_cleanup()

        self.ses_binlog_next = self.find_next_binlog()

        # Adding compress seems to be buggy on mysqlbinlog
        mysqlbinlog_command = [mysqlbinlog,
                               "--defaults-file=%s" % self.mysql_defaults_file,
                               "--read-from-remote-master=BINLOG-DUMP-GTIDS",
                               "--stop-never-slave-server-id=%s" % str(int(time.time())),
                               "--raw", "--stop-never", "--result-file=%s_._%s_._" % (
                                   'tmp', self.srv_hostname),
                               self.ses_binlog_next]

        logger.info(' '.join(mysqlbinlog_command))

        try:
            os.chdir(self.base_binlog_dir)
            end_ts = time.time() + 3585.0
            cleanup_int = 0

            dev_null = None
            if self.debug:
                p = Popen(mysqlbinlog_command)
            else:
                dev_null = open(os.devnull, 'w')
                p = Popen(mysqlbinlog_command, stdout=dev_null, stderr=dev_null)

            r = p.poll()

            self.pid = wait_for_pid('mysqlbinlog')
            if self.pid is None:
                logger.error('Timed out waiting for mysqlbinlog pid')
                return False

            logger.debug('mysqlbinlog PID %s' % str(self.pid))

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
                self.write_prometheus_metric('last_error', int(time.time()))

            if dev_null is not None:
                dev_null.close()
        except Exception as err:
            logger.error("mysqlbinlog died with error %s" % str(err))
            self.write_prometheus_metric('last_error', int(time.time()))
            raise

        logger.info('Starting session post-cleanup')
        self.session_cleanup()
        self.write_prometheus_metric('last_ok', int(time.time()))

        return True

    def session_cleanup(self, keep_last=False):
        """ The mysqlbinlog process saves binlogs collected for the last hour
        into tmp_<hostname>_<binlogname> into the binlogdir directory. At the end or beginning
        of each session, we clean them up to help identify where we start.
        """
        logger.debug('Starting session cleanup')
        self.session_cleanup_tmp(keep_last)
        self.session_cleanup_lst()
        logger.debug('Session cleanup complete')

    def session_cleanup_tmp(self, keep_last=False):
        binlogs = find_session_binlogs(self.ses_binlog_tmp_prefix, self.base_binlog_dir)
        if len(binlogs) == 0:
            return True

        last_binlog = binlogs[-1]
        last_binlog_lst = last_binlog.replace('tmp_._', 'lst_._')

        for fn in binlogs:
            fnpath = os.path.join(self.base_binlog_dir, fn)
            logger.debug(fnpath)
            fnparts = fn.split('_._')

            try:
                fn_created_ts = binlog_ts_created(fnpath)
            except BinaryLogFormatError as err:
                os.unlink(fnpath)
                logger.error(str(err))
                logger.error('%s is not a valid binary log' % fnpath)
                continue

            fndir = os.path.join(self.binlog_dir, fn_created_ts[:8])
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
                last_binlog_lst = os.path.join(self.base_binlog_dir, last_binlog_lst)
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

                    self.write_prometheus_metric('last_error', int(time.time()))
                    raise Exception('Compression failed for %s' % fnpath)

        return True

    def session_cleanup_lst(self):
        binlogs = find_session_binlogs(self.ses_binlog_lst_prefix, self.base_binlog_dir)
        if len(binlogs) == 0:
            return True

        next_binlog = self.find_next_binlog()

        for fn in binlogs:
            fnpath = os.path.join(self.base_binlog_dir, fn)
            fnparts = fn.split('_._')
            fn_created_ts = binlog_ts_created(fnpath)
            fndir = os.path.join(self.binlog_dir, fn_created_ts[:8])
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

                    self.write_prometheus_metric('last_error', int(time.time()))
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
                logger.info('Recorded last binlog in session, has been purged')
                logger.warn('Potential gap in downloaded binlogs, please review')
                next_binlog = None
            else:
                logger.info('Recorded last binlog in session, '
                            'streaming will start from %s' % next_binlog)
                return next_binlog

        binlog_dir_list = os.listdir(self.binlog_dir)
        binlog_dir_list.sort()
        binlog_dir_list.reverse()
        for binlog_dir in binlog_dir_list:
            binlog_dir_path = os.path.join(self.binlog_dir, binlog_dir)
            if not os.path.isdir(binlog_dir_path):
                continue

            binlogs = list_binlogs_from_dir(binlog_dir_path)
            # We only need the last binlog from the newest dir
            if len(binlogs) == 0:
                continue

            next_binlog = normalize_binlog_name(binlogs[-1])
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
                if self.debug:
                    traceback.print_exc()
                logger.error(str(err))
                logger.debug(sql)
                logger.error('Binlog does not exist on server [%s]' % binlog)
                return False

        logger.debug('Binlog exists on server [%s]' % str(row))
        return True

    def read_server_metadata(self):
        row = self.mysql_client.fetchone('SELECT @@hostname AS hn')
        self.srv_hostname = row['hn']
        row = self.mysql_client.fetchone('SHOW BINARY LOGS')
        self.srv_binlog_first = row['Log_name']
        row = self.mysql_client.fetchone('SHOW MASTER STATUS')
        self.srv_binlog_last = row['File']
        self.srv_binlog_prefix = self.srv_binlog_last[:-7]

    def zip(self, binlog, dest):
        if self.zip_binary is not None:
            cmd_gzip = [self.zip_binary, binlog]

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


class BinaryLogFormatError(MySQLError):
    def __init__(self, message):
        super(BinaryLogFormatError, self).__init__(errno=30001, msg=message)

