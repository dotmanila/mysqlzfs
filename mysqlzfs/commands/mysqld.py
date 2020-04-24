#!/bin/env python3

import MySQLdb
import os
import signal
import time
from .. import util as zfs_util
from collections import OrderedDict
from subprocess import Popen, PIPE


class MysqlZfsService(object):
    """
    Manage a MySQL instance/service running against a dataset, wether imported
    or cloned from snapshot.
    """

    def __init__(self, logger, opts, snapshot=None):
        self.sigterm_caught = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.logger = logger
        self.opts = opts
        if snapshot is not None:
            self.snapshot = snapshot
        else:
            self.snapshot = self.opts.snapshot

        self.rootdir = os.path.join('/%s' % self.opts.dataset, 's%s' % self.snapshot)
        self.dataset = os.path.join('%s' % self.opts.dataset, 's%s' % self.snapshot)
        self.datadir = os.path.join(self.rootdir, 'data')
        self.cnf_file = os.path.join(self.rootdir, 'my.cnf')
        self.cnf_auto = os.path.join(self.datadir, 'auto.cnf')

        self.cnf = OrderedDict({
            'client': OrderedDict({
                'socket':os.path.join(self.datadir, 'mysqld%s.sock' % self.snapshot)
            }),
            'mysqld': OrderedDict({
                'pid-file': os.path.join(self.datadir, 'mysqld%s.pid' % self.snapshot),
                'socket': os.path.join(self.datadir, 'mysqld%s.sock' % self.snapshot),
                'sql_mode': 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION',
                'datadir': self.datadir,
                'innodb_log_group_home_dir': os.path.join(self.rootdir, 'redo'),
                'innodb_doublewrite': 0,
                'innodb_checksum_algorithm': 'none',
                'innodb_log_file_size': '1G',
                'innodb_buffer_pool_size': '1G',
                'innodb_flush_log_at_trx_commit': 2,
                'port': 30066,
                'user': 'mysql',
                'skip-slave-start': None,
                'skip-networking': None,
                'skip-name-resolve': None,
                'log-error': os.path.join(self.datadir, 'error%s.log' % self.snapshot)
            })
        })

    def _signal_handler(self, signal, frame):
        self.sigterm_caught = True

    def write_cnf(self):
        """ Writes out a bootstrap my.cnf for this imported dataset.
        Also makes sure that transient files like pid and socket files are named
        based on snapshot name in case the snapshot comes with these files from
        the original source.
        """

        # make sure to move aside any my.cnf that comes with the ZFS snapshot
        cnf_file_restored = '%s.zfs' % self.cnf_file
        if os.path.isfile(cnf_file_restored) and os.path.isfile(self.cnf_file) and \
                os.stat(self.cnf_file).st_size > 0:
            return True
        elif not os.path.isfile(cnf_file_restored) and os.path.isfile(self.cnf_file):
            self.logger.debug('Looks like my.cnf restored from ZFS snapshot, backing up')
            os.rename(self.cnf_file, cnf_file_restored)

        cnffd = open(self.cnf_file, 'w+')
        for section in self.cnf:
            cnffd.write('[%s]\n' % section)
            for key in self.cnf[section]:
                if self.cnf[section][key] is None:
                    cnffd.write('%s\n' % key)
                else:
                    cnffd.write('%s = %s\n' % (key, str(self.cnf[section][key])))

        cnffd.close()
        return True

    def stop(self):
        if not self.is_alive():
            self.logger.info('MySQL does not seem to be running, not doing anything')
            return True

        self.logger.info('Stopping MySQL service on %s' % self.rootdir)

        mysqld = ['/usr/bin/mysqladmin', '--defaults-file=%s' % self.opts.dotmycnf,
                  '--socket', self.cnf['mysqld']['socket'], 'shutdown']
        self.logger.debug('Shutting down mysqld with %s' % ' '.join(mysqld))

        p_mysqld = Popen(mysqld, stdout=PIPE, stderr=PIPE)
        r = p_mysqld.poll()
        poll_count = 0
        pid_exists = os.path.isfile(self.cnf['mysqld']['pid-file'])
        can_connect = False
        poll_size = 0
        log_size = 0

        while r is None:
            time.sleep(2)
            p_mysqld.poll()

            if pid_exists:
                poll_count = poll_count + 1
                pid_exists = os.path.isfile(self.cnf['mysqld']['pid-file'])

                # if pid still not exist after 10 seconds, break and wait()
                if poll_count > 5:
                    log_size = os.stat(self.cnf['mysqld']['log-error']).st_size
                    # If the log size has not changed after 10 seconds, most likely
                    # already hung
                    if log_size != poll_size:
                        self.logger.info('.. we are not stuck, mysqld is shutting down')
                        poll_count = 0
                        poll_size = log_size
                        continue
                    else:
                        self.logger.info('Giving up waiting on PID, we\'ll keep checking connection')
                        pid_exists = False
                        continue

                if not pid_exists:
                    self.logger.info('MySQL PID removed, almost there ...')
                    poll_count = 0

                continue

            can_connect = self.is_alive()
            poll_count = poll_count + 1

            if poll_count > 30 or not can_connect:
                self.logger.debug('Exceeded poll count %d' % poll_count)
                if not can_connect:
                    r = p_mysqld.wait()
                    if r is not None and r != 0:
                        break

                can_connect = self.is_alive()
                if can_connect:
                    self.logger.info('MySQL still running, giving up, please shutdown manually')
                    r = 9999
                    break

        if can_connect:
            self.logger.info('MySQL is up, connect with command:')
            self.logger.info('mysql --defaults-file=%s --socket %s' % (
                            self.opts.dotmycnf, self.cnf['mysqld']['socket']))

        if r != 0:
            self.logger.error('Stopping mysqld returned with bad code: %d' % r)
            self.logger.error('Check error log at %s' % self.cnf['mysqld']['log-error'])
            return False

        return True

    def start(self):
        """
        Attempts to start MySQL server given the root dataset name specified
        by --snapshot in the command line.
        - Writes the bootstrap my.cnf on the root directory
        - Starts MySQL and monitor if pid file gets created
        - Checks if connectivity can be established before returning control to operator
        """

        self.write_cnf()
        if self.is_alive():
            self.logger.info('MySQL is already running on this dataset, connect with:')
            self.logger.info('mysql --defaults-file=%s --socket %s' % (
                             self.opts.dotmycnf, self.cnf['mysqld'][socket]))

        self.logger.info('Starting MySQL service on %s' % self.rootdir)

        if os.path.isfile(self.cnf['mysqld']['log-error']):
            os.unlink(self.cnf['mysqld']['log-error'])

        if os.path.isfile(self.cnf_auto):
            os.unlink(self.cnf_auto)

        mysqld = ['/usr/sbin/mysqld', '--defaults-file=%s' % self.cnf_file,
                  '--user=%s' % self.cnf['mysqld']['user'], '--daemonize']
        self.logger.debug('Starting mysqld with %s' % ' '.join(mysqld))
        self.logger.info('You can tail the error log at %s' % self.cnf['mysqld']['log-error'])

        p_mysqld = Popen(mysqld, stdout=PIPE, stderr=PIPE)
        r = p_mysqld.poll()
        poll_count = 0
        can_connect = False
        pid_no = None
        proc_status = None

        time.sleep(2)
        # Workaround poll() not catching return code for small snapshots
        # https://lists.gt.net/python/bugs/633489
        pid_no = self.wait_for_pid(self.cnf['mysqld']['pid-file'],
                                   self.cnf['mysqld']['log-error'])

        if pid_no is None:
            pid_no, piderr = zfs_util.pidno_from_pstree(self.opts.ppid, 'mysqld')

            if pid_no is not None:
                self.logger.info('Using PID from from pstree %d' % int(pid_no))

        if pid_no is None:
            pid_no = self.pid_from_log(self.cnf['mysqld']['log-error'])
            self.logger.info('Using PID from from error log %d' % int(pid_no))

        if not self.wait_for_io(pid_no):
            self.logger.error('MySQL process has had no IO activity')

        p_mysqld.poll()
        while r is None:
            time.sleep(2)
            p_mysqld.poll()

            if self.is_alive():
                r = p_mysqld.wait()
                break

            proc_status = zfs_util.proc_status(pid_no)
            if proc_status is not None and 'Z' not in proc_status['State']:
                continue

            poll_count = poll_count + 1

            if poll_count > 30:
                self.logger.info('Still cannot connect, giving up ...')
                r = 9999
                break

        if self.is_alive():
            self.logger.info('MySQL is up, connect with command:')
            self.logger.info('mysql --defaults-file=%s --socket %s' % (
                            self.opts.dotmycnf, self.cnf['mysqld']['socket']))

        if r != 0:
            self.logger.error('Starting mysqld returned with bad code: %d' % r)
            self.logger.error('Check error log at %s' % self.cnf['mysqld']['log-error'])
            return False

        return True

    def restart(self):
        return self.stop() and self.start()

    def is_alive(self):
        """ Simple check if we can connect ot the MySQL server manage by
        this object.
        """
        conn = None
        cur = None
        params = dict()

        params['read_default_file'] = self.opts.dotmycnf
        params['read_default_group'] = 'client'
        params['unix_socket'] = self.cnf['mysqld']['socket']
        params['connect_timeout'] = 1

        try:
            conn = MySQLdb.connect('localhost', **params)
            # MySQLdb for some reason has autocommit off by default
            conn.autocommit(True)
            try:
                cur = conn.cursor(MySQLdb.cursors.DictCursor)
                cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST")
            except MySQLdb.Error as e:
                self.logger.error('MySQL is running but could not execute query')
                return None

            return True
        except MySQLdb.Error as e:
            return False
        finally:
            if conn is not None:
                cur.close()
                conn.close()

    def get_pid_no(self, pid_file):
        pid_no = 0
        with open(pid_file) as pid_fd:
            pid_no = int(pid_fd.read())
        pid_fd.close()

        return pid_no

    def get_proc_io(self, pid_number):
        iostat = OrderedDict()
        with open('/proc/%d/io' % pid_number) as io_fd:
            for stat in io_fd:
                kv = stat.split(':')
                iostat[kv[0].strip()] = int(kv[1])

        return iostat

    def wait_for_io(self, pidno, polls = 5):
        """ Check for proc io stats, if there is no change in read bytes
        after poll * 2 number of seconds, and still cannot connect, we
        return False
        """

        io_prev = None
        io_curr = None
        poll = 0

        while True:
            io_curr = self.get_proc_io(pidno)
            if io_prev is None:
                io_prev = io_curr
                continue

            if self.is_alive():
                return True

            self.logger.debug('Old read bytes: %d, new read bytes: %d' % (
                              io_prev['read_bytes'], io_curr['read_bytes']))

            if io_prev['read_bytes'] != io_curr['read_bytes']:
                time.sleep(2)
                poll = 0
                io_prev = io_curr
                continue

            poll = poll + 1
            self.logger.debug('wait_for_io polls %d' % poll)
            if poll > polls:
                return False

    def wait_for_pid(self, pidfile, errorlog, polls=2):
        """ Wait for pid until there is no error log activity for more than
        polls * 2 seconds, return pid_no if exists, None if not.
        """

        pid_exists = os.path.isfile(pidfile)
        poll = 0
        size_prev = 0
        size_curr = 0

        while True:
            if pid_exists:
                return self.get_pid_no(pidfile)

            size_curr = os.stat(errorlog).st_size
            if size_prev is None:
                size_prev = size_curr
                continue

            self.logger.debug('Old log size: %d, new log size: %d' % (size_prev, size_curr))
            if size_prev != size_curr:
                time.sleep(2)
                poll = 0
                pid_exists = os.path.isfile(pidfile)
                size_prev = size_curr
                continue

            poll = poll + 1
            self.logger.debug('wait_for_pid polls %d' % poll)
            size_prev = size_curr
            if poll > polls:
                self.logger.info('Timed out waiting for PID file to be created')
                return None

            time.sleep(2)

    def pid_from_log(self, errorlog):
        """ Very crude way of identifying the mysqld PID when
        pidfile takes time, and subprocess.Popen does not return
        the correct one

        Relies on the error log always new!
        """
        pid_no = None

        with open(errorlog, 'r') as logfd:
            for log in logfd:
                if 'starting as process' in log:
                    pid_no = int(log.split(' ')[-2])
                    break

        logfd.close()
        return pid_no
