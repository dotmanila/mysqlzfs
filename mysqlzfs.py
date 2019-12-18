#!/usr/bin/env python

import boto3
import gzip
import logging
import MySQLdb
import os
import psutil
import re
import shutil
import socket
import sys
import shlex
import signal
import struct
import time
import traceback
from ConfigParser import ConfigParser, NoOptionError
from botocore.exceptions import ClientError as BotoClientError
from collections import OrderedDict
from datetime import datetime, timedelta
from multiprocessing import Pool, TimeoutError, cpu_count
from optparse import OptionParser
from subprocess import Popen, PIPE, STDOUT, CalledProcessError

# TODO items:
# Binlog streaming support
# Auto replication service after import
# How can we make sure exported snapshots are usable?
# Handling disk space issues.
# Check to make sure mydumper version is at leas 0.9.5
# Pruning from S3 bucket
# When binlog server source changes, binlog number changes too
#   need a way to handle this more gracefully
# ZFS clone and destroy should operate on specific fs and not do recursive 
#   to avoid snapshoting cloned/imported datasets
# binlog purge and compression
# purge old dumps after s3 upload
# alert + cleanup stale mysqlds
# starting mysqlbinlog process should not depend on result of session cleanup
# When you have zero sized exported snapshot or cleanup on failed send_to_bin
# Sync scripts failures with proper exit codes
# cleanup old lst_._* binlog files
# binlog gap scanning and report, total count, size and date range

MYSQLZFS_VERSION = 0.3
MYSQLZFS_CMD_SNAP = 'snapshot'
MYSQLZFS_CMD_EXPORT = 'export'
MYSQLZFS_CMD_IMPORT = 'import'
MYSQLZFS_CMD_DUMP = 'dump'
MYSQLZFS_CMD_MYSQLD = 'mysqld'
MYSQLZFS_CMD_CLONE = 'clone'
MYSQLZFS_CMD_S3 = 's3'
MYSQLZFS_CMD_BINLOGD = 'binlogd'

MYSQLZFS_EXPTYPE_FULL = 'full'
MYSQLZFS_EXPTYPE_INCR = 'incremental'
MYSQLZFS_EXPTYPE_NONE = None

MYSQLZFS_SIGTERM_CAUGHT = False
MYSQLZFS_S3_CLIENT = None


def __sigterm_handler(signal, frame):
    global MYSQLZFS_SIGTERM_CAUGHT
    print('Signal caught, terminating')
    MYSQLZFS_SIGTERM_CAUGHT = True

def s3_client_initialize():
    global MYSQLZFS_S3_CLIENT
    MYSQLZFS_S3_CLIENT = boto3.client('s3')

def s3_upload(job):
    bucket, file, s3key = job
    try:
        MYSQLZFS_S3_CLIENT.head_object(Bucket=bucket, Key=s3key)

        if not os.path.isfile('%s.s3part' % file):
            return (True, '%s (skipped, already exist)' % s3key)
    except BotoClientError, err:
        pass

    try:
        ts_start = time.time()
        open('%s.s3part' % file, 'a').close()
        MYSQLZFS_S3_CLIENT.upload_file(file, bucket, s3key,
                                       ExtraArgs={'StorageClass': 'STANDARD_IA'})
        os.unlink('%s.s3part' % file)
        return (True, '%s took %fs' % (s3key, round(time.time()-ts_start)))
    except BotoClientError, err:
        return (False, '%s %s' % (s3key, str(err)))


class MysqlZfs(object):
    @staticmethod
    def buildopts():
        opt_usage = "Usage: %prog [options] COMMAND"
        opt_desc = "Managed ZFS snapshots for MySQL backups"
        opt_epilog = ""
        parser = MysqlZfsOptParser(opt_usage, version="%prog " + str(MYSQLZFS_VERSION),
            description=opt_desc, epilog=opt_epilog)
        parser.add_option('-z', '--dataset', dest='dataset', type='string',
            help='ZFS root dataset to manage, default mysql/root')
        parser.add_option('-o', '--backup-dir', dest='backupdir', type='string',
            help='Root backups directory for binlogs, exports and dumps')
        parser.add_option('-B', '--s3-bucket', dest='s3bucket', type='string',
            help='S3 bucket for this node, NOT USED AT THIS TIME')
        parser.add_option('-K', '--status', dest='status', action="store_true", 
            help='For most commands, display existing snapshots/dumps/exports',
            default=False)
        parser.add_option('-s', '--snapshot', dest='snapshot', type='string',
            help='Operate on this named snapshot i.e. for export/import')
        parser.add_option('-d', '--debug', dest='debug', action="store_true", 
            help='Enable debugging outputs',
            default=False)
        parser.add_option('-F', '--full', dest='full', action="store_true", 
            help='Force a full export of most recent snapshot',
            default=False)
        parser.add_option('-I', '--incr', dest='incr', action="store_true", 
            help='Force a incr export of most recent snapshot',
            default=False)
        parser.add_option('-c', '--defaults-file', dest='dotmycnf', type='string', 
            help='Path to .my.cnf containing connection credentials to MySQL',
            default='/root/.my.cnf')
        parser.add_option('-r', '--skip-repl-check', dest='skipreplcheck', action="store_true", 
            help='Wether to skip replication check when taking the snapshot',
            default=False)
        parser.add_option('-S', '--stage', dest='stage', action="store_true", 
            help='When specified with import command, run MySQL service on imported dataset',
            default=False)
        parser.add_option('-k', '--stop', dest='stop', action="store_true", 
            help='Stop the MySQL instance running on staged --snapshot',
            default=False)
        parser.add_option('-g', '--start', dest='start', action="store_true", 
            help='Start the MySQL instance running on staged --snapshot',
            default=False)
        parser.add_option('-x', '--run', dest='run', action="store_true", 
            help='Run the default actions for a command i.e. for s3 is upload',
            default=False)
        parser.add_option('-w', '--cleanup', dest='cleanup', action="store_true", 
            help='Run the cleanup actions for a command i.e. for s3 is prune local and remote',
            default=False)
        parser.add_option('-t', '--threads', dest='threads', type='int',
            help='How many threads for parallel jobs i.e. S3 uploads, mydumper')

        (opts, args) = parser.parse_args()

        if os.getuid() != 0:
            parser.error('This tool should only be run as root ... for now')

        if opts.dataset is None:
            parser.error('ZFS dataset to manage is required i.e. mysql/root.')
        else:
            zfsprop, zfserr = Zfs.get(opts.dataset, ['compression'])
            if zfsprop is None or zfserr != '':
                parser.error('ZFS dataset is unreadable (%s)' % zfserr)

        if opts.backupdir is None:
            parser.error('Backups directory is required')
        elif not os.path.isdir(opts.backupdir):
            parser.error('Backups directory does not exist')

        opts.dataset = opts.dataset.strip('/')

        try:
            dset = opts.dataset.replace('/', '_')
            cdir = os.path.join(opts.backupdir, dset)
            if not os.path.isdir(cdir):
                os.mkdir(cdir)

            for bdir in ['dump', 'binlog', 'zfs']:
                cdir = os.path.join(opts.backupdir, dset, bdir)
                if not os.path.isdir(cdir):
                    os.mkdir(cdir)
        except Exception, err:
            parser.error('Error creating %s' % cdir)

        opts.bindir = os.path.join(opts.backupdir, dset, 'zfs')
        opts.dumpdir = os.path.join(opts.backupdir, dset, 'dump')
        opts.binlogdir = os.path.join(opts.backupdir, dset, 'binlog')

        cmds = [MYSQLZFS_CMD_SNAP, MYSQLZFS_CMD_EXPORT, MYSQLZFS_CMD_IMPORT, 
                MYSQLZFS_CMD_DUMP, MYSQLZFS_CMD_MYSQLD, MYSQLZFS_CMD_CLONE,
                MYSQLZFS_CMD_S3, MYSQLZFS_CMD_BINLOGD]
        if len(args) == 1 and args[0] not in cmds:
            parser.error("Command not recognized, got '%s'. See more with --help" % args[0])
        elif len(args) <= 0:
            parser.error("Command not specified. See more with --help")
        elif len(args) > 1:
            parser.error("Multiple commands specified. See more with --help")
        else:
            opts.cmd = args[0]

        if opts.snapshot is None and opts.cmd == MYSQLZFS_CMD_IMPORT:
            parser.error('Snapshot name is required when importing, see --status')

        if opts.cmd == MYSQLZFS_CMD_DUMP and Zfs.which('mydumper') is None:
            parser.error('mydumper command/utility not found')
        # We hardcode the s3 bucket name for now
        #elif opts.cmd == MYSQLZFS_CMD_S3:
        #    if opts.s3bucket is None:
        #        parser.error('S3 bucket is required with this command')

        opts.root = '%s@s' % opts.dataset
        opts.fslist = ['%s/data@s' % opts.dataset, '%s/redo@s' % opts.dataset]
        opts.ppid = os.getpid()
        opts.pcwd = os.path.dirname(os.path.realpath(__file__))

        if opts.threads is None:
            cpus = cpu_count()
            if cpus <= 2:
                opts.threads = 1
            elif cpus <= 8:
                opts.threads = int(cpus/2)
            else:
                opts.threads = 8

        return opts

    @staticmethod
    def create_logger(opts):
        logger = None
        logfile = os.path.join('/var/log', 
                               'mysqlzfs-%s-%s.log' % (
                                    opts.cmd.lower(), 
                                    opts.dataset.strip().replace('/', '-')
                                ))
        logformat = '%(asctime)s <%(process)d> %(levelname)s mysqlzfs:: %(message)s'

        if not os.path.isdir(os.path.dirname(logfile)):
            os.mkdir(os.path.dirname(logfile))

        loglevel = None
        if opts.debug:
            loglevel = logging.DEBUG
        else:
            loglevel = logging.INFO

        logging.basicConfig(filename=logfile, level=loglevel, format=logformat)

        logger = logging.getLogger('mysqlzfs')
        
        if sys.stdout.isatty():
            log_stream = logging.StreamHandler(sys.stdout)
            log_stream.setLevel(logger.getEffectiveLevel())
            log_stream.setFormatter(logging.Formatter(fmt=logformat))
            logger.addHandler(log_stream)

        return logger

    @staticmethod
    def pidno_from_pstree(ppid, procname):
        """ Traverses an output of pstree -p INT -A
        and returns the pidno of the first matching process name

        python(9934)-+-mydumper(10667)-+-{mydumper}(10669)
                     |                 |-{mydumper}(10670)
                     |                 |-{mydumper}(10672)
                     |                 |-{mydumper}(10674)
                     |                 |-{mydumper}(10676)
                     |                 |-{mydumper}(10678)
                     |                 |-{mydumper}(10680)
                     |                 `-{mydumper}(10682)
                     `-pstree(10668)
        """
        pidno = None
        cmd_pstree = ['pstree', '-p', '-A', str(ppid)]

        p = Popen(cmd_pstree, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        p = None
        if err is not '':
            return None, err

        root_list = out.split('\n')
        
        for s in root_list:
            if s == '':
                continue

            p = s.split('-')
            if len(p) < 1:
                continue

            for n in p:
                if procname not in n:
                    continue

                name, pid = n.strip(')').split('(')
                if name.strip('}{') == procname:
                    return int(pid), None

        return None, ''

    @staticmethod
    def proc_status(pidno):
        """ retrieves the data from /proc/PID/status and puts it in an
        OrderedDict
        """
        status_file = '/proc/%d/status' % int(pidno)
        if not os.path.isfile(status_file):
            return None

        status_list = OrderedDict()
        with open(status_file) as statusfd:
            for status_line in statusfd:
                k, v = status_line.split(':')
                status_list[k.strip()] = v.strip()
        statusfd.close()

        return status_list

    @staticmethod
    def tsftime(unixtime, format = '%m/%d/%Y %H:%M:%S'):
        d = datetime.fromtimestamp(unixtime)
        return d.strftime(format)

    @staticmethod
    def read_config_file(cfgfile):
        if not os.path.isfile(cfgfile):
            return None

        cfg = ConfigParser(allow_no_value=True)
        cfg.read(cfgfile)
        return cfg

    @staticmethod
    def sizeof_fmt(num, suffix='B'):
        for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    @staticmethod
    def write_lock_file(lockfile, pidno):
        with open(lockfile, 'w') as lockfd:
            lockfd.write(str(pidno))

        lockfd.close()
        return True

    @staticmethod
    def read_lock_file(lockfile):
        """ Check if lock file exists and returns the pidno of the binlog 
        process. If binlog process is dead, delete lock file and return None
        """
        pid = None
        
        if not os.path.isfile(lockfile):
            return False, pid

        with open(lockfile, 'r') as lockfd:
            for pidline in lockfd:
                try:
                    pid = int(pidline)
                    if pid <= 0:
                        continue
                    break
                except ValueError, err:
                    return False, pid
                finally:
                    lockfd.close()
                    break

        if pid is None or pid <= 0:
            return False, pid

        return MysqlZfs.is_process_running(pid), pid

    @staticmethod
    def is_process_running(pidno):
        """ Check for /proc/PID/status
        - If it exists, if not process is dead, return True
        - If exists and process state is Z, return False
        - 
        TODO: Check also that that process is not in zombie?
        """
        if pidno is None:
            return False

        if MysqlZfs.proc_status(pidno) is not None:
            return True
        else:
            return False

    @staticmethod
    def mysql_connect(dotmycnf, section='client'):

        try:
            cnf = MysqlZfs.read_config_file(dotmycnf)
            if cnf is None:
                raise Exception('Could not read provided %s' % dotmycnf)
            elif not cnf.has_option(section, 'host'):
                section = 'client'

            if not cnf.has_option(section, 'host'):
                raise Exception('.my.cnf %s group requires host option' % section)

            params = { 'read_default_file': dotmycnf,
                       'read_default_group': section }
                       
            conn = MySQLdb.connect(cnf.get(section, 'host'), **params)
            # MySQLdb for some reason has autoccommit off by default
            conn.autocommit(True)
            return conn
        except MySQLdb.Error, e:
            raise Exception('Could not establish connection to MySQL server')


class MysqlZfsSnapshotManager(object):
    def __init__(self, logger, opts):
        self.opts = opts
        self.logger = logger
        self.snaps = None
        self.bins = None
        self.bins_dict = OrderedDict()
        self.error_message = ''

        if self.opts.dataset is not None and self.zfs_dataset_info(self.opts.dataset) is None:
            raise Exception('Specified dataset does not exist in any pool')

        self.list_binaries()
        self.list_snapshots()

    def list_snapshots(self, refresh=False):
        if self.snaps is not None and refresh is False:
            return len(self.snaps), ''

        cmd_list = ['/sbin/zfs', 'list', '-H', '-t', 'snap', '-d', '1', self.opts.dataset]
        p = Popen(cmd_list, stdout=PIPE, stderr=PIPE)
        self.logger.debug(' '.join(cmd_list))
        out, err = p.communicate()
        if err is not '':
            self.logger.error(err)
            return 0, err

        root_list = out.split('\n')
        self.snaps = []
        for s in root_list:
            if self.opts.root not in s:
                continue
            snap = s.split('\t')
            self.snaps.append(re.sub(self.opts.root, '', snap[0]))


        if len(self.snaps) == 0:
            self.logger.info('No existing snapshots found')
        else:
            self.logger.info('Found %d snapshots' % len(self.snaps))

        return len(self.snaps), err

    def zfs_dataset_info(self, full_snapname):
        p = Popen(['/sbin/zfs', 'list', '-H', '-p', full_snapname], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err is not '':
            self.logger.error(err)
            return None

        return out.split('\t')
        
    def snapshot_to_bin(self, snapname=None):
        full_names = []
        full_bins = []
        has_errors = False
        snapdt = datetime.today()
        snapweek = snapdt - timedelta(days=snapdt.isoweekday())
        snapday = snapdt.strftime('%Y%m%d%H%M00')

        # If snapshot name is not provided, we get today's snapshot from
        # midnight (or closest)
        if snapname is None:
            # If --incr/--full is specified, explicitly export the last snapshot
            snapshots_today = [x for x in self.snaps if re.search('^%s' % snapday[0:8], x)]
            self.logger.debug('Found %d snapshot(s) taken today' % len(snapshots_today))

            if len(snapshots_today) == 0:
                self.logger.error('No existing snapshots from today!')
                return False

            if self.opts.incr or self.opts.full:
                snapname = snapshots_today[-1]
            else:
                snapname = snapshots_today[0]


        for fs in self.opts.fslist:
            full_name = '%s%s' % (fs, snapname)
            snap_info = self.zfs_dataset_info(full_name)
            if snap_info is None:
                self.logger.error('%s is not found from root dataset')
                return False

            full_names.append(full_name)

        last_type, last_snapname, last_set = self.find_last_export()
        execute_full_snapshot = False
        if snapdt.isoweekday() == 7 or self.bins is None or self.opts.full:
            execute_full_snapshot = True

        if last_type is not MYSQLZFS_EXPTYPE_NONE and int(last_snapname) >= int(snapname):
            self.logger.info('Today\'s snapshot (%s) has already been exported' % snapname)
            self.logger.info('Try exporting most recent snapshot with --incr/--full')
            return True

        if not execute_full_snapshot:
            snapprop, snaperr = Zfs.get('%s@s%s' % (self.opts.dataset, last_snapname), ['origin'])
            if snapprop is None:
                self.logger.error('Looks like the source for incremental snapshot is missing')
                self.logger.error(snaperr)
                return False

        for fs in self.opts.fslist:
            full_name = '%s%s' % (fs, snapname)
            if execute_full_snapshot:
                self.logger.info('Start full snapshot to binary copy for %s' % full_name)
                full_binname = os.path.join(self.opts.bindir, snapday)

                if not os.path.isdir(full_binname):
                    os.mkdir(full_binname)

                binname = '_'.join(re.sub('@', '/', full_name).split('/')[::-1])
                full_binname = os.path.join(full_binname, '%s_full.zfs' % binname)

                if not self.zfs_send_to_bin(full_name, full_binname):
                    has_errors = True
                    break
            else:
                self.logger.info('Start incremental snapshot to binary copy for %s' % full_name)

                full_binname = os.path.join(self.opts.bindir, last_set)

                binname = '_'.join(re.sub('@', '/', full_name).split('/')[::-1])
                full_binname = os.path.join(full_binname, '%s_incr.zfs' % binname)

                if not self.zfs_send_to_bin(full_name, full_binname, '@s%s' % last_snapname):
                    has_errors = True
                    break

            full_bins.append(full_binname)

        if has_errors:
            for fs in full_bins:
                self.logger.info('Cleaning up %s' % fs)
                if os.path.isfile(fs):
                    os.unlink(fs)

            return False

        self.list_binaries()
        if len(self.bins) > 3:
            self.logger.debug('Export sets list %s' % str(self.bins))
            # delete only the oldes set when the newest one is full
            # this means if we want to keep two weeks/sets, we make it three here
            self.logger.debug('Pruning export sets %s' % str(self.bins.keys()[:-3]))
            purge_list = self.bins.keys()[:-3]
            for folder in purge_list:
                purge_folder = os.path.join(self.opts.bindir, folder)
                self.logger.info('Pruning export set %s' % purge_folder)
                shutil.rmtree(purge_folder)

        return True

    def zfs_send_to_bin(self, full_snapname, full_binname, incr_snapname=None):
        p_send = None

        if os.path.isfile(full_binname):
            self.logger.error('%s exists, aborting' % full_binname)
            return False

        cmd_send = '/sbin/zfs send -P -c -v'
        if incr_snapname is not None:
            cmd_send = '%s -i %s' % (cmd_send, incr_snapname)
        cmd_send = '%s %s' % (cmd_send, full_snapname)
        binfd = os.open(full_binname, os.O_WRONLY|os.O_CREAT|os.O_TRUNC)
        binlog = os.open(re.sub('.zfs$', '.log', full_binname), os.O_WRONLY|os.O_CREAT|os.O_TRUNC)

        self.logger.info('Running command [%s > %s]' % (cmd_send, full_binname))

        p_send = Popen(shlex.split(cmd_send), stdout=binfd, stderr=binlog)

        r = p_send.poll()
        poll_count = 0
        poll_size = 0
        while r is None:
            time.sleep(2)
            p_send.poll()

            # Workaround poll() not catching return code for small snapshots
            # https://lists.gt.net/python/bugs/633489
            current_size = os.stat(full_binname).st_size
            if poll_size == current_size:
                poll_count = poll_count + 1
            else:
                poll_count = 0

            poll_size = current_size
            if poll_count > 3:
                r = p_send.wait()

        os.close(binfd)
        os.close(binlog)

        if r != 0:
            self.logger.error('ZFS send command failed with code: %d' % r)
            return False

        return True

    def list_binaries(self):
        self.bins = OrderedDict()
        root_list = os.listdir(self.opts.bindir)
        root_list.sort()
        if len(root_list) == 0:
            self.bins = None
            self.logger.info('No existing set of binary backups found')
            return False

        fparts = None
        fsnapname = None
        fdataset = None
        ftype = None
        fsize = 0

        for d in root_list:
            exp_dir = os.path.join(self.opts.bindir, d)
            if not os.path.isdir(exp_dir): continue

            exp_list = os.listdir(exp_dir)

            if len(exp_list) > 0:
                """ bins_dict = { 
                    20191201000001: { 
                        20191201000501: { 
                            name: 20191201000501_mysql_root_data_full.zfs, 
                            type: full, 
                            size: bytes,
                            fs: mysql 
                        }, 
                        ...
                    } 
                }
                """

                # We should remove self.bins in the future
                self.bins[d] = OrderedDict()
                self.bins_dict[d] = OrderedDict()
                self.bins[d]['incr'] = []
                exp_list.sort()

                # Keep track of the previous export name so we do
                # not append the same export name on the list i.e. redo + data
                prev_exp = None
                for f in exp_list:
                    if not re.search('.zfs$', f):
                        continue

                    fparts = f[:-4].split('_')
                    fsnapname = re.sub('^s','', fparts[0])
                    fdataset = '/'.join(fparts[1:-1][::-1])
                    ftype = fparts[-1]
                    fsize = os.stat(os.path.join(exp_dir, f)).st_size

                    if fsnapname not in self.bins_dict[d]:
                        self.bins_dict[d][fsnapname] = OrderedDict()
                        self.bins_dict[d][fsnapname]['files'] = []
                        self.bins_dict[d][fsnapname]['size'] = 0
                    
                    self.bins_dict[d][fsnapname]['files'].append({
                        'name': f, 'type': ftype,
                        'fs': fdataset, 'size': fsize
                    })
                    self.bins_dict[d][fsnapname]['size'] += fsize

                    if fsnapname == prev_exp: 
                        continue

                    if ftype == 'full':
                        self.bins[d]['full'] = fsnapname
                    else:
                        self.bins[d]['incr'].append(fsnapname)

                    prev_exp = fsnapname

        return True

    def show_binaries(self):
        if self.bins is None:
            return None

        size_h = None
        for s in self.bins:
            self.logger.info('Binary export set: %s' % s)
            size_h = MysqlZfs.sizeof_fmt(self.bins_dict[s][self.bins[s]['full']]['size'])
            self.logger.info('+- full: %s %s' % (self.bins[s]['full'], size_h))
            for i in self.bins[s]['incr']:
                size_h = MysqlZfs.sizeof_fmt(self.bins_dict[s][i]['size'])
                self.logger.info('+--- incr: %s %s' % (i, size_h))

    def find_last_export(self):
        if self.bins is None:
            return MYSQLZFS_EXPTYPE_NONE, MYSQLZFS_EXPTYPE_NONE, MYSQLZFS_EXPTYPE_NONE

        last_set = self.bins.keys()[-1]
        if len(self.bins[last_set]['incr']) > 0:
            return MYSQLZFS_EXPTYPE_INCR, self.bins[last_set]['incr'][-1], last_set
        else:
            return MYSQLZFS_EXPTYPE_FULL, self.bins[last_set]['full'], last_set

    def find_bin_set(self, binname):
        series = []

        if self.bins is None:
            return None, None, None, None

        for s in self.bins:
            series.append(self.bins[s]['full'])
            if binname == self.bins[s]['full']:
                return MYSQLZFS_EXPTYPE_FULL, self.bins[s]['full'], s, series

            if len(self.bins[s]['incr']) > 0:
                for b in self.bins[s]['incr']:
                    series.append(b)
                    if binname == b:
                        return MYSQLZFS_EXPTYPE_INCR, b, s, series

            series = []

        return None, None, None, None

    def zfs_receive_from_bin(self, bin_series, bin_set, root_dataset):
        is_incr = False
        bin_dict = OrderedDict()

        # Build the list of binary backups and verify all of them exists
        # instead of encoutering a missing bin file in the middle of 20TB
        # restore
        for b in bin_series:
            bin_dir = os.path.join(self.opts.bindir, bin_set)
            fs_name = self.opts.dataset.split('/')[-1]
            bin_dict[b] = dict()

            for fs in self.opts.fslist:
                snapname = '%s%s' % (fs, b)
                bin_filename = '_'.join(re.sub('@', '/', snapname).split('/')[::-1])
                dataset_name = bin_filename.split('_')[1]
                full_dataset_name = '%s/%s' %(root_dataset, dataset_name)
                if is_incr:
                    bin_fullpath = os.path.join(bin_dir, '%s_incr.zfs' % bin_filename)
                else:
                    bin_fullpath = os.path.join(bin_dir, '%s_full.zfs' % bin_filename)

                if not os.path.isfile(bin_fullpath):
                    self.logger.error('Expected binary backup is missing %s' % bin_fullpath)
                    return False

                bin_dict[b][dataset_name] = { 'file': bin_fullpath, 'dataset': full_dataset_name }

            is_incr = True

        for b in bin_dict:
            for ds in bin_dict[b]:
                bin_fullpath = bin_dict[b][ds]['file']
                bin_dataset = bin_dict[b][ds]['dataset']
                self.logger.info('Importing %s' % bin_fullpath)

                p_receive = None

                cmd_receive = ['/sbin/zfs', 'receive', bin_dataset]
                binfd = os.open(bin_fullpath, os.O_RDONLY)
                p_receive = Popen(cmd_receive, stdin=binfd, stderr=PIPE)
                self.logger.debug(cmd_receive)

                r = p_receive.poll()
                poll_count = 0
                poll_size = 0
                while r is None:
                    time.sleep(2)
                    p_receive.poll()

                    # Workaround poll() not catching return code for small snapshots
                    # https://lists.gt.net/python/bugs/633489
                    current_size = int(self.zfs_dataset_info(bin_dataset)[1])
                    self.logger.debug('%s %d' % (bin_dataset, current_size))
                    if poll_size == current_size:
                        poll_count = poll_count + 1
                    else:
                        poll_count = 0

                    poll_size = current_size
                    if poll_count > 3:
                        r = p_receive.wait()

                os.close(binfd)

                if r != 0:
                    self.logger.error('ZFS receive command failed with code: %d' % r)
                    return False

        return True

    def zfs_destroy_dataset(self, dataset, recursive=True):
        args = ['/sbin/zfs', 'destroy']
        if recursive:
            args.append('-r')
        args.append(dataset)

        p = Popen(args, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err is not '':
            self.logger.error(err)
            return False

        return True

    def zfs_create_dataset(self, dataset, properties={}):
        p = Popen(['/sbin/zfs', 'create', dataset], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err is not '':
            self.logger.error(err)
            return False

        if len(properties) > 0:
            for k in properties:
                p = Popen(['/sbin/zfs', 'set', '%s=%s' % (k, properties[k]), dataset], 
                          stdout=PIPE, stderr=PIPE)
                out, err = p.communicate()
                if err is not '':
                    self.logger.error(err)
                    self.zfs_destroy_dataset(dataset)
                    return False

        return True

    def import_bin(self, binname):
        bin_type, bin_name, bin_set, bin_series = self.find_bin_set(binname)
        if bin_set is None:
            self.logger.error('Binary backup does not belong to any set')
            return False

        # Create root staging dataset, this will be on top of the main dataset
        # for now.
        root_target = '%s/s%s' % (self.opts.dataset, binname)

        dsprop, zfserr = Zfs.get(root_target, ['origin'])
        if dsprop is not None:
            self.logger.error('Target dataset %s already exists, cannot import' % root_target)
            return False

        if not self.zfs_create_dataset(root_target):
            self.logger.error('Unable to create staging ZFS dataset %s' % root_target)
            return False

        if not self.zfs_receive_from_bin(bin_series, bin_set, root_target):
            self.logger.error('An error occurred importing the binary backup set')
            self.zfs_destroy_dataset(root_target, True)
            return False

        return True

    def zfs_snapshot(self):
        conn = None

        try:
            conn = MysqlZfs.mysql_connect(self.opts.dotmycnf, 'zfsmysql')
            cur = conn.cursor(MySQLdb.cursors.DictCursor)

            if not self.opts.skipreplcheck:
                cur.execute('SHOW SLAVE STATUS')

                while True:
                    row = cur.fetchone()
                    if row is None: 
                        self.logger.error('MySQL server is not running as replica')
                        return False

                    break

                if row['Slave_IO_Running'] != 'Yes' or row['Slave_SQL_Running'] != 'Yes':
                    self.logger.error('Replication thread(s) are not running')
                    return False

            if self.opts.skipreplcheck:
                self.logger.debug('Locking tables for backup')
                cur.execute('LOCK TABLES FOR BACKUP')
            else:
                self.logger.debug('Stopping SQL thread')
                cur.execute('STOP SLAVE SQL_THREAD')
                self.logger.debug('Flushing tables')
                cur.execute('FLUSH TABLES')

            snapname = datetime.today().strftime('%Y%m%d%H%M%S')
            self.logger.debug('Taking snapshot')
            args = ['/sbin/zfs', 'snap', '-r', '%s@s%s' % (self.opts.dataset, snapname)]

            p = Popen(args, stdout=PIPE, stderr=PIPE)
            out, err = p.communicate()
            if err is not '':
                self.logger.error(err)
                return False

            self.logger.info('Snapshot %s@s%s complete' % (self.opts.dataset, snapname))
            self.logger.info('Pruning %d old snapshots' % len(self.snaps[:-431]))
            for s in self.snaps[:-431]:
                self.logger.debug(' - %s@s%s' % (self.opts.dataset, s))
                self.zfs_destroy_dataset('%s@s%s' % (self.opts.dataset, s))

        except MySQLdb.Error, e:
            self.logger.error('A MySQL error has occurred, aborting new snapshot')
            self.logger.error(str(e))
            return False
        finally:
            if conn is not None:
                if cur is None:
                    cur = conn.cursor(MySQLdb.cursors.DictCursor)

                if self.opts.skipreplcheck:
                    self.logger.debug('Unlocking tables')
                    cur.execute('UNLOCK TABLES')
                else:
                    self.logger.debug('Resuming replication')
                    cur.execute('START SLAVE')

                cur.close()
                conn.close()

    def zfs_snapshot_summary(self):
        if len(self.snaps) <= 0:
            return False

        self.logger.info('Oldest snapshot %s' % self.snaps[0])
        self.logger.info('Latest snapshot %s' % self.snaps[-1])

        return True

    def clone_snapshot(self):
        root_snapname = '%s@s%s' % (self.opts.dataset, self.opts.snapshot)
        root_target = '%s/s%s' % (self.opts.dataset, self.opts.snapshot)

        dsprop, zfserr = Zfs.get(root_target, ['origin'])
        if dsprop is not None:
            self.logger.error('Target dataset %s already exists, cannot clone' % root_target)
            return False

        self.logger.info('Cloning %s to %s' % (root_snapname, root_target))
        snapinfo = self.zfs_dataset_info(root_snapname)
        if snapinfo is None:
            self.logger.error('Missing root snapshot %s' % root_snapname)
            return False

        for fs in self.opts.fslist:
            snapname = '%s%s' % (fs, self.opts.snapshot)
            snapinfo = self.zfs_dataset_info(snapname)
            if snapinfo is None:
                self.logger.error('Missing child on this dataset %s' % snapname)
                return False

        if not self.zfs_clone_snapshot(root_snapname, root_target):
            self.zfs_destroy_dataset(root_snapname, True)
            return False

        for fs in self.opts.fslist:
            target = '%s/%s' % (root_target, fs.split('@')[0].split('/')[-1])
            snapname = '%s%s' % (fs, self.opts.snapshot)

            if not self.zfs_clone_snapshot(snapname, target):
                self.zfs_destroy_dataset(root_snapname, True)
                return False

        self.logger.info('Clone successfully completed')
        return True

    def zfs_clone_snapshot(self, snapshot, target):
        p = Popen(['/sbin/zfs', 'clone', snapshot, target], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err is not '':
            self.logger.error(err)
            return False

        return True

    def destroy_clone(self, snapshot):
        dataset = '%s/s%s' % (self.opts.dataset, snapshot)
        return self.zfs_destroy_dataset(dataset, True)


class MysqlDumper(object):
    def __init__(self, opts, logger, zfsmgr):
        self.opts = opts
        self.logger = logger
        self.mysqld = None
        self.zfsmgr = zfsmgr

        self.lockfile = '/tmp/mysqlzfs-dump-%s.lock' % (re.sub('/', '_', self.opts.dataset))
        self.is_running, self.pid = MysqlZfs.read_lock_file(self.lockfile)

        if self.is_running:
            raise Exception('Another dump process is running with PID %d' % self.pid)

        MysqlZfs.write_lock_file(self.lockfile, self.opts.ppid)

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

        dumperpid, piderr = MysqlZfs.pidno_from_pstree(self.opts.ppid, 'mydumper')
        self.logger.debug('mydumper pid %s' % str(dumperpid))
        #print(MysqlZfs.proc_status(dumperpid))

        while r is None:
            time.sleep(2)
            p_dumper.poll()
            poll_count = poll_count + 1

            if not os.path.isfile(dump_log):
                if poll_count > 2:
                    out, err = p_dumper.communicate()
                    r = p_dumper.returncode
                    if err != '':
                        self.logger.error(err)
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


class MysqlZfsService(object):
    """
    Manage a MySQL instance/service running against a dataset, wether imported
    or cloned from snapshot.
    """

    def __init__(self, logger, opts, snapshot=None):
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
                'pid-file':os.path.join(self.datadir, 'mysqld%s.pid' % self.snapshot),
                'socket':os.path.join(self.datadir, 'mysqld%s.sock' % self.snapshot),
                'sql_mode':'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION',
                'datadir':self.datadir,
                'innodb_log_group_home_dir':os.path.join(self.rootdir, 'redo'),
                'innodb_doublewrite':0,
                'innodb_checksum_algorithm':'none',
                'innodb_log_file_size':'1G',
                'innodb_buffer_pool_size':'1G',
                'innodb_flush_log_at_trx_commit':2,
                'port':30066,
                'user':'mysql',
                'skip-slave-start':None,
                'skip-networking':None,
                'skip-name-resolve':None,
                'log-error':os.path.join(self.datadir, 'error%s.log' % self.snapshot)
            })
        })

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
            pid_no, piderr = MysqlZfs.pidno_from_pstree(self.opts.ppid, 'mysqld')

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

            proc_status = MysqlZfs.proc_status(pid_no)
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
        params = dict()

        params['read_default_file'] = self.opts.dotmycnf
        params['read_default_group'] = 'client'
        params['unix_socket'] = self.cnf['mysqld']['socket']
        params['connect_timeout'] = 1

        try:
            conn = MySQLdb.connect('localhost', **params)
            # MySQLdb for some reason has autoccommit off by default
            conn.autocommit(True)
            try:
                cur = conn.cursor(MySQLdb.cursors.DictCursor)
                cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST")
            except MySQLdb.Error, e:
                self.logger.error('MySQL is running but could not execute query')
                return None

            return True
        except MySQLdb.Error, e:
            return False
        finally:
            if conn is not None:
                cur.close()
                conn.close()

    def get_pid_no(self, pidfile):
        pid_no = 0
        with open(self.cnf['mysqld']['pid-file']) as pidfd:
            pid_no = int(pidfd.read())
        pidfd.close()

        return pid_no

    def get_proc_io(self, pidno):
        iostat = OrderedDict()
        with open('/proc/%d/io' % pidno) as iofd:
            for stat in iofd:
                kv = stat.split(':')
                iostat[kv[0].strip()] = int(kv[1])
        iofd.close()

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

    """ Copied from pymysqlreplication.constants.BINLOG.py, we do not need to import 
    it here
    """
    UNKNOWN_EVENT = 0x00
    START_EVENT_V3 = 0x01
    QUERY_EVENT = 0x02
    STOP_EVENT = 0x03
    ROTATE_EVENT = 0x04
    INTVAR_EVENT = 0x05
    LOAD_EVENT = 0x06
    SLAVE_EVENT = 0x07
    CREATE_FILE_EVENT = 0x08
    APPEND_BLOCK_EVENT = 0x09
    EXEC_LOAD_EVENT = 0x0a
    DELETE_FILE_EVENT = 0x0b
    NEW_LOAD_EVENT = 0x0c
    RAND_EVENT = 0x0d
    USER_VAR_EVENT = 0x0e
    FORMAT_DESCRIPTION_EVENT = 0x0f
    XID_EVENT = 0x10
    BEGIN_LOAD_QUERY_EVENT = 0x11
    EXECUTE_LOAD_QUERY_EVENT = 0x12
    TABLE_MAP_EVENT = 0x13
    PRE_GA_WRITE_ROWS_EVENT = 0x14
    PRE_GA_UPDATE_ROWS_EVENT = 0x15
    PRE_GA_DELETE_ROWS_EVENT = 0x16
    WRITE_ROWS_EVENT_V1 = 0x17
    UPDATE_ROWS_EVENT_V1 = 0x18
    DELETE_ROWS_EVENT_V1 = 0x19
    INCIDENT_EVENT = 0x1a
    HEARTBEAT_LOG_EVENT = 0x1b
    IGNORABLE_LOG_EVENT = 0x1c
    ROWS_QUERY_LOG_EVENT = 0x1d
    WRITE_ROWS_EVENT_V2 = 0x1e
    UPDATE_ROWS_EVENT_V2 = 0x1f
    DELETE_ROWS_EVENT_V2 = 0x20
    GTID_LOG_EVENT = 0x21
    ANONYMOUS_GTID_LOG_EVENT = 0x22
    PREVIOUS_GTIDS_LOG_EVENT = 0x23

    # INTVAR types
    INTVAR_INVALID_INT_EVENT = 0x00
    INTVAR_LAST_INSERT_ID_EVENT = 0x01
    INTVAR_INSERT_ID_EVENT = 0x02

    """ Copied from pymysqlreplication.packet.py, we do not need to import 
    it here
    """
    __event_map = {
        # event
        QUERY_EVENT: 'QueryEvent',
        ROTATE_EVENT: 'RotateEvent',
        FORMAT_DESCRIPTION_EVENT: 'FormatDescriptionEvent',
        XID_EVENT: 'XidEvent',
        INTVAR_EVENT: 'IntvarEvent',
        GTID_LOG_EVENT: 'GtidEvent',
        STOP_EVENT: 'StopEvent',
        BEGIN_LOAD_QUERY_EVENT: 'BeginLoadQueryEvent',
        EXECUTE_LOAD_QUERY_EVENT: 'ExecuteLoadQueryEvent',
        HEARTBEAT_LOG_EVENT: 'HeartbeatLogEvent',
        # row_event
        UPDATE_ROWS_EVENT_V1: 'UpdateRowsEvent',
        WRITE_ROWS_EVENT_V1: 'WriteRowsEvent',
        DELETE_ROWS_EVENT_V1: 'DeleteRowsEvent',
        UPDATE_ROWS_EVENT_V2: 'UpdateRowsEvent',
        WRITE_ROWS_EVENT_V2: 'WriteRowsEvent',
        DELETE_ROWS_EVENT_V2: 'DeleteRowsEvent',
        TABLE_MAP_EVENT: 'TableMapEvent',
        #5.6 GTID enabled replication events
        ANONYMOUS_GTID_LOG_EVENT: 'NotImplementedEvent',
        PREVIOUS_GTIDS_LOG_EVENT: 'NotImplementedEvent'
    }

    ___write_events = [
        UPDATE_ROWS_EVENT_V1,
        WRITE_ROWS_EVENT_V1,
        DELETE_ROWS_EVENT_V1,
        UPDATE_ROWS_EVENT_V2,
        WRITE_ROWS_EVENT_V2,
        DELETE_ROWS_EVENT_V2
    ]


    def __init__(self, logger, opts):
        self.logger = logger
        self.opts = opts
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
        #self.cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        self.binlogdir = os.path.join(self.opts.binlogdir, self.srv_hostname)
        if not os.path.isdir(self.binlogdir):
            os.mkdir(self.binlogdir)

        self.binzip = Zfs.which('pigz')
        if self.binzip is None:
            self.logger.info('pigz is not available, consider installing '
                             'for faster compressions')
            self.binzip = Zfs.which('gzip')
            if self.binzip is None:
                self.logger.info('gzip is not available, consider installing '
                                 'pigz for faster compressions')

    def srv_connect_ctl(self):
        try:
            self.cnf = MysqlZfs.read_config_file(self.opts.dotmycnf)
            if self.cnf is None:
                raise Exception('mysqlbinlog section is not available from .my.cnf')
            elif not self.cnf.has_option('mysqlbinlog', 'host'):
                raise Exception('mysqlbinlog section requires at least host value')

            params = { 'read_default_file': self.opts.dotmycnf,
                       'read_default_group': 'mysqlbinlog' }

            self.conn = MySQLdb.connect(self.cnf.get('mysqlbinlog', 'host'), **params)
            # MySQLdb for some reason has autoccommit off by default
            self.conn.autocommit(True)
        except MySQLdb.Error, e:
            raise Exception('Could not establish connection to MySQL server')

    def srv_cursor_fetchone(self, sql):
        """ This should not be called directly, see binlog_exists_ON_server
        """
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        return cur.fetchone()

    def start(self):
        mysqlbinlog = Zfs.which('mysqlbinlog')
        if mysqlbinlog is None:
            raise Exception('mysqlbinlog command not found')

        self.is_running, self.pid = MysqlZfs.read_lock_file(self.lockfile)
        if self.is_running:
            self.logger.error('mysqlbinlog process still running with PID %s' % self.pid)
            return False
        else:
            self.pid = None

        self.logger.info('Starting session pre-cleanup')
        self.session_cleanup()

        self.ses_binlog_next = self.find_next_binlog()

        # Adding compress seems to be buggy on mysqlbinlog
        binlogd_cmd = [mysqlbinlog, 
                    "--defaults-file=%s" % self.opts.dotmycnf, 
                    "--read-from-remote-master=BINLOG-DUMP-GTIDS",  
                    "--stop-never-slave-server-id=%s" % str(int(time.time())), 
                    "--raw", "--stop-never", "--result-file=%s_._%s_._" % (
                        'tmp', self.srv_hostname),
                    self.ses_binlog_next]

        self.logger.info(' '.join(binlogd_cmd))

        try:
            os.chdir(self.opts.binlogdir)
            end_ts = time.time() + 3585.0
            cleanup_int = 0

            FNULL = None
            if self.opts.debug:
                p = Popen(binlogd_cmd)
            else:
                FNULL = open(os.devnull, 'w')
                p = Popen(binlogd_cmd, stdout=FNULL, stderr=FNULL)

            r = p.poll()

            self.pid = self.wait_for_pid()
            self.logger.debug('mysqlbinlog PID %s' % str(self.pid))

            if self.pid is None:
                self.logger.error('Timed out waiting for mysqlbinlog pid')
                return False

            MysqlZfs.write_lock_file(self.lockfile, self.pid)

            while time.time() < end_ts:
                # Cleanup every minute
                if cleanup_int%60 == 0:
                    self.session_cleanup(True)

                r = p.poll()
                if r is not None:
                    break

                if MYSQLZFS_SIGTERM_CAUGHT:
                    self.logger.info('Cleaning up mysqlbinlog process')
                    break

                time.sleep(10)
                cleanup_int += 10

            if r is None:
                p.kill()
            elif r != 0:
                self.logger.error("mysqlbinlog exited error code %s" % str(r))

            if FNULL is not None:
                FNULL.close()

            os.chdir(opts.pcwd)
        except Exception, e:
            self.logger.error("mysqlbinlog died with error %s" % str(e))
            raise

        self.logger.info('Starting session post-cleanup')
        self.session_cleanup()

        return True

    def wait_for_pid(self):
        sleeps = 10
        while sleeps > 0:
            pidno, err = MysqlZfs.pidno_from_pstree(self.opts.ppid, 'mysqlbinlog')
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
        self.logger.debug('Starting session cleanup')
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
                self.logger.error('Destination binlog exists, saving as duplicate')
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

                self.logger.debug('Found %s, moving to %s' % (fn, fndest))
                if not self.zip(fnpath, fndest_zip):
                    self.logger.error('Compression failed for %s' % fnpath)
                    if os.path.isfile(fndest_zip):
                        os.unlink(fndest_zip)
                    return False

        self.logger.debug('Session cleanup complete')

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
                self.logger.debug('Binlog from lst file %s' % next_binlog)

        if next_binlog is not None:
            if not self.binlog_exists_on_server(next_binlog):
                self.logger.info('Recorded last binlog in session, '
                                 'has been purged')
                self.logger.warn('Potential gap in downloaded binlogs, please review')
                next_binlog = None
            else:
                self.logger.info('Recorded last binlog in session, '
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
                self.logger.info('Last binlog based on stored set %s' % next_binlog)

                if not self.binlog_exists_on_server(next_binlog):
                    next_binlog = None
                    break
                else:
                    next_binlog = '%s.%06d' % (next_binlog[:-7], (int(next_binlog[-6:])+1))
                    self.logger.info('Next binlog is %s' % next_binlog)
                    return next_binlog

        self.logger.info('No recorded last binlog in session, '
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
            # too but we are cmopressing binlogs, have to think of how to do
            # that in the future
            if len(fn.split('_._')) == 2:
                binlogs.append(fn)

        return binlogs

    def binlog_exists_on_server(self, binlog):
        sql = 'SHOW BINLOG EVENTS IN "%s" LIMIT 1' % binlog
        row = None
        try:
            row = self.srv_cursor_fetchone(sql)
        except MySQLdb.Error, err:
            if err.args[0] == 2006:
                self.srv_connect_ctl()
                row = self.srv_cursor_fetchone(sql)
            else:
                if self.opts.debug:
                    traceback.print_exc()
                self.logger.error(str(err))
                self.logger.debug(sql)
                self.logger.error('Binlog does not exist on server [%s]' % binlog)
                return False

        self.logger.debug('Binlog exists on server [%s]' % str(row))
        return True        

    def find_session_binlogs(self):
        binlogs = []
        lsout = os.listdir(self.opts.binlogdir)
        if len(lsout) == 0:
            return binlogs

        lsout.sort()
        for fn in lsout:
            fnpath = os.path.join(self.opts.binlogdir, fn)
            if os.path.isdir(fnpath):
                continue

            if re.search('^%s' % self.ses_binlog_tmp_prefix, fn) is not None \
                    and self.is_binlog_format(fnpath):
                binlogs.append(fn)

        return binlogs     

    def read_server_metadata(self):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute('SELECT @@hostname AS hn')
        row = cur.fetchone()
        self.srv_hostname = row['hn']
        cur.execute('SHOW BINARY LOGS')
        row = cur.fetchone()
        self.srv_binlog_first = row['Log_name']
        cur.execute('SHOW MASTER STATUS')
        row = cur.fetchone()
        self.srv_binlog_last = row['File']
        self.srv_binlog_prefix = self.srv_binlog_last[:-7]
        cur.close()

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
                self.logger.error('Failed to compress with gzip, '
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
        except Exception, err:
            self.logger.error(str(err))
            return False

    def unzip(self, binlog):
        pass


class MysqlZfsServiceList(object):
    """
    Manage a group of MysqlZfsService
    """

    def __init__(self, logger, opts, zfsmgr):
        self.logger = logger
        self.opts = opts
        self.rootdir = '/%s' % self.opts.dataset
        self.zfsmgr = zfsmgr

    def cleanup(self):
        sandboxes = self.scan_sandboxes()
        if sandboxes is None:
            self.logger.info('No sandboxes running on any stage datasets')
            return None

        for s in sandboxes:
            if self.opts.snapshot and self.opts.snapshot != s:
                continue

            mysqld = MysqlZfsService(self.logger, self.opts, s)
            self.logger.info('+- %s' % sandboxes[s]['rootdir'])
            if mysqld.is_alive():
                self.logger.info('+--- MySQL is running, shutting down')
                mysqld.stop()

            self.logger.info('+--- Cleaning up ZFS dataset %s' % mysqld.dataset)
            if self.zfsmgr.zfs_destroy_dataset(mysqld.dataset, recursive=True):
                self.logger.info('+--- Done')

    def show_sandboxes(self):
        sandboxes = self.scan_sandboxes()
        if sandboxes is None:
            self.logger.info('No sandboxes running on any stage datasets')
            return None

        for s in sandboxes:
            mysqld = MysqlZfsService(self.logger, self.opts, s)
            self.logger.info('+- %s' % sandboxes[s]['rootdir'])
            self.logger.info('+--- mysql --defaults-file=%s --socket=%s' % (
                             self.opts.dotmycnf, sandboxes[s]['socket']))
            if mysqld.is_alive():
                self.logger.info('+--- Running: Yes')
            else:
                self.logger.info('+--- Running: No')

            if sandboxes[s]['deployed']:
                self.logger.info('+--- MySQL deployed: Yes')
            else:
                self.logger.info('+--- MySQL deployed: No')

            self.logger.info('+--- Origin: %s' % sandboxes[s]['origin'])

    def scan_sandboxes(self):
        l = os.listdir(self.rootdir)
        if len(l) == 0:
            return None

        sandboxes = OrderedDict()

        for d in l:
            rootdir = os.path.join(self.rootdir, d)
            # We are only interested on directories that matches snapshot names
            if not os.path.isdir(rootdir) or not re.search('^s[0-9]{14}$', d):
                continue

            props, err = Zfs.get(rootdir.strip('/'), ['origin'])
            if err is not '':
                self.logger.error('Unable to retrieve dataset property for %s' % rootdir.strip('/'))
                self.logger.error(err)
                continue

            snapname = re.sub('^s', '', d)
            sandboxes[snapname] = OrderedDict({
                'rootdir': os.path.join(self.rootdir, d),
                'socket': os.path.join(self.rootdir, d, 'data', 'mysqld%s.sock' % snapname),
                'mycnf': os.path.join(self.rootdir, d, 'my.cnf'),
                'origin': props['origin'],
                'dataset': rootdir.strip('/')
                })

            if os.path.isfile(sandboxes[snapname]['mycnf']):
                sandboxes[snapname]['deployed'] = True
            else:
                sandboxes[snapname]['deployed'] = False

        if len(sandboxes) == 0:
            return None

        return sandboxes

    def scan_sandbox(self, sandbox):
        """ Check if a sandbox exists and if mysql is running, returns an
        OrderedDict of metadata about a sandbox. See scan_sandboxes.
        """

        rootdir = os.path.join(self.rootdir, 's%s' % sandbox)
        # We are only interested on directories that matches snapshot names
        if not os.path.isdir(rootdir):
            return None

        props, err = Zfs.get(rootdir.strip('/'), ['origin'])
        if err is not '':
            self.logger.error('Unable to retrieve dataset property for %s' % rootdir.strip('/'))
            self.logger.error('Returned "%s"' % err)
            return False

        sandbox = OrderedDict({
            'rootdir': os.path.join(self.rootdir),
            'socket': os.path.join(self.rootdir, 'data', 'mysqld%s.sock' % sandbox),
            'mycnf': os.path.join(self.rootdir, 'my.cnf'),
            'origin': props['origin']
            })

        if os.path.isfile(sandbox['mycnf']):
            sandbox['deployed'] = True
        else:
            sandbox['deployed'] = False

        return sandbox


class MysqlS3Client(object):
    def __init__(self, logger, opts):
        self.opts = opts
        self.logger = logger
        self.bucket = 'mysqlzfs-us-east-1'
        self.s3 = boto3.client('s3')
        self.ts_start = time.time()
        self.ts_end = None
        self.ignorelist = ['metadata', 'metadata.partial', 's3metadata', 's3metadata.partial']
        self.lockfile = '/tmp/mysqlzfs-s3-%s.lock' % (re.sub('/', '_', self.opts.dataset))
        self.is_running, self.pid = MysqlZfs.read_lock_file(self.lockfile)

        if self.is_running:
            raise Exception('Another S3 upload process is running with PID %d' % self.pid)

        MysqlZfs.write_lock_file(self.lockfile, self.opts.ppid)

        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except BotoClientError, err:
            if int(err.response['Error']['Code']) == 404:
                self.logger.info('Bucket %s does not exist, creating' % self.bucket)
                self.s3.create_bucket(ACL='private', Bucket=self.bucket)
            else:
                self.logger.debug(err.response)
                self.logger.error(str(err))

    def reap_children(self, timeout=3):
        "Tries hard to terminate and ultimately kill all the children of this process."
        def on_terminate(proc):
            print("process {} terminated with exit code {}".format(proc, proc.returncode))

        procs = psutil.Process(self.opts.ppid).children()
        # send SIGTERM
        for p in procs:
            try:
                p.terminate()
            except psutil.NoSuchProcess:
                pass
        gone, alive = psutil.wait_procs(procs, timeout=timeout, callback=on_terminate)
        if alive:
            # send SIGKILL
            for p in alive:
                print("process {} survived SIGTERM; trying SIGKILL".format(p))
                try:
                    p.kill()
                except psutil.NoSuchProcess:
                    pass
            gone, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
            if alive:
                # give up
                for p in alive:
                    print("process {} survived SIGKILL; giving up".format(p))

    def upload_chunks(self, s3list):
        # So far, this has been the only method that worked trying to parallelize
        # tried the __class_) trick, pathos, creating a sep class for S3, etc
        s3pool = Pool(self.opts.threads, s3_client_initialize)
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
        except TimeoutError, err:
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
                print ' '
            else:
                print chunks,

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
            #print(binlogs)
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
                metafd.write('dt_start:%s\n' % MysqlZfs.tsftime(self.ts_start, '%Y_%m_%d-%H_%M_%S'))
                metafd.write('ts_start:%s\n' % self.ts_start)
                metafd.write('dt_end:%s\n' % MysqlZfs.tsftime(self.ts_end, '%Y_%m_%d-%H_%M_%S'))
                metafd.write('ts_end:%s\n' % self.ts_end)
            metafd.close()

            if os.path.isfile(meta_file_partial):
                os.unlink(meta_file_partial)
        else:
            with open(meta_file_partial, 'w') as metafd:
                metafd.write('dt_start:%s\n' % MysqlZfs.tsftime(self.ts_start, '%Y_%m_%d-%H_%M_%S'))
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


class Zfs(object):
    @staticmethod
    def get(dataset, properties=[]):
        cmd_get = ['/sbin/zfs', 'get', '-H', '-p']
        if len(properties) == 0:
            cmd_get.append('all')
        else:
            cmd_get.append(','.join(properties))
        cmd_get.append(dataset)

        p = Popen(cmd_get, stdout=PIPE, stderr=PIPE)
        #print(str(cmd_get))
        out, err = p.communicate()
        if err is not '':
            #print('x"%s"-' % err)
            return None, err

        root_list = out.split('\n')
        
        prop_list = OrderedDict()
        for s in root_list:
            if s == '':
                continue

            p = s.split('\t')
            prop_list[p[1]] = p[2]

        return prop_list, ''

    @staticmethod
    def which(file):
        for path in os.environ["PATH"].split(os.pathsep):
            filepath = os.path.join(path, file)
            if os.path.exists(filepath):
                    return filepath

        return None


# http://stackoverflow.com/questions/1857346/\
# python-optparse-how-to-include-additional-info-in-usage-output
class MysqlZfsOptParser(OptionParser):
    def format_epilog(self, formatter):
        return self.epilog


if __name__ == "__main__":
    try:
        signal.signal(signal.SIGTERM, __sigterm_handler)
        signal.signal(signal.SIGINT, __sigterm_handler)

        logger = None
        opts = None

        opts = MysqlZfs.buildopts()
        logger = MysqlZfs.create_logger(opts)
        zfsmgr = MysqlZfsSnapshotManager(logger, opts)

        if opts.cmd == MYSQLZFS_CMD_EXPORT:
            if opts.run:
                zfsmgr.snapshot_to_bin()
            else:
                zfsmgr.show_binaries()
        elif opts.cmd == MYSQLZFS_CMD_CLONE:
            if opts.cleanup:
                mysqlds = MysqlZfsServiceList(logger, opts, zfsmgr)
                mysqlds.cleanup()
            elif opts.run:
                success = zfsmgr.clone_snapshot()
                if success and opts.stage:
                    mysqld = MysqlZfsService(logger, opts)
                    mysqld.start()
            else:
                mysqlds = MysqlZfsServiceList(logger, opts, zfsmgr)
                mysqlds.show_sandboxes()
        elif opts.cmd == MYSQLZFS_CMD_IMPORT:
            success = zfsmgr.import_bin(opts.snapshot)
            if success and opts.stage:
                mysqld = MysqlZfsService(logger, opts)
                mysqld.start()
        elif opts.cmd == MYSQLZFS_CMD_DUMP:
            dumper = MysqlDumper(opts, logger, zfsmgr)
            if opts.run:
                dumper.start()
            else:
                dumper.status()
        elif opts.cmd == MYSQLZFS_CMD_MYSQLD:
            if opts.stop:
                mysqld = MysqlZfsService(logger, opts)
                mysqld.stop()
            elif opts.start:
                mysqld = MysqlZfsService(logger, opts)
                mysqld.start()
            elif opts.cleanup:
                mysqlds = MysqlZfsServiceList(logger, opts, zfsmgr)
                mysqlds.cleanup()
            else:
                mysqlds = MysqlZfsServiceList(logger, opts, zfsmgr)
                mysqlds.show_sandboxes()
        elif opts.cmd == MYSQLZFS_CMD_SNAP:
            if opts.run:
                zfsmgr.zfs_snapshot()
            else:
                zfsmgr.zfs_snapshot_summary()
        elif opts.cmd == MYSQLZFS_CMD_S3:
            s3 = MysqlS3Client(logger, opts)
            if opts.run:
                s3.upload_dumps()
                s3.upload_binlogs()
            else:
                logger.info('S3 command does not have status subcommand yet')
        elif opts.cmd == MYSQLZFS_CMD_BINLOGD:
            if opts.run:
                binlogd = MysqlBinlogStreamer(logger, opts)
                binlogd.start()
            else:
                logger.info('binlogd command does not have status subcommand yet')
        else:
            logger.error('Unknown command "%s"' % opts.cmd)

        logger.info('Done')

    except Exception, e:
        if logger is not None:
            if opts is None or (opts is not None and opts.debug):
                tb = traceback.format_exc().splitlines()
                for l in tb:
                    logger.error(l)
            else:
                logger.error(str(e))
        else:
            traceback.print_exc()
        
        sys.exit(1)
