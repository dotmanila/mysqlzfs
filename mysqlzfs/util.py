#!/bin/env python3

import logging
import MySQLdb
import os
import sys
from . import *
from . import zfs
from . import util as zfs_util
from collections import OrderedDict
from configparser import ConfigParser, NoOptionError
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from optparse import OptionParser


def which(file_name):
    for path in os.environ["PATH"].split(os.pathsep):
        filepath = os.path.join(path, file_name)
        if os.path.exists(filepath):
            return filepath

    return None


def buildopts():
    opt_usage = "Usage: %prog [options] COMMAND"
    opt_desc = "Managed ZFS snapshots for MySQL backups"
    opt_epilog = ""
    parser = OptionParser(opt_usage, version="%prog " + str(MYSQLZFS_VERSION),
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
                      help='Enable debugging outputs', default=False)
    parser.add_option('-F', '--full', dest='full', action="store_true",
                      help='Force a full export of most recent snapshot', default=False)
    parser.add_option('-I', '--incr', dest='incr', action="store_true",
                      help='Force a incr export of most recent snapshot', default=False)
    parser.add_option('-c', '--defaults-file', dest='dotmycnf', type='string',
                      help='Path to .my.cnf containing connection credentials to MySQL',
                      default='/root/.my.cnf')
    parser.add_option('-r', '--skip-repl-check', dest='skip_repl_check', action="store_true",
                      help='Whether to skip replication check when taking the snapshot',
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
    parser.add_option('-m', '--metrics-text-dir', dest='metrics_text_dir', type='string',
                      help='Emit textfile metrics to this directory for node_exporter',
                      default=None)

    (opts, args) = parser.parse_args()

    if os.getuid() != 0:
        parser.error('This tool should only be run as root ... for now')

    if opts.dataset is None:
        parser.error('ZFS dataset to manage is required i.e. mysql/root.')
    else:
        zfsprop, zfserr = zfs.get(opts.dataset, ['compression'])
        if zfsprop is None or zfserr != '':
            parser.error('ZFS dataset is unreadable (%s)' % zfserr)

    if opts.backupdir is None:
        parser.error('Backups directory is required')
    elif not os.path.isdir(opts.backupdir):
        parser.error('Backups directory does not exist')

    if opts.metrics_text_dir is not None and not os.path.isdir(opts.metrics_text_dir):
        parser.error('Specified metrics textfile directory does not exist')

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
    except Exception as err:
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

    if opts.cmd == MYSQLZFS_CMD_DUMP and zfs_util.which('mydumper') is None:
        parser.error('mydumper command/utility not found')
    # We hardcode the s3 bucket name for now
    # elif opts.cmd == MYSQLZFS_CMD_S3:
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
    if err.decode('ascii') is not '':
        return None, err.decode('ascii')

    root_list = out.decode('ascii').split('\n')

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


def tsftime(unixtime, format = '%m/%d/%Y %H:%M:%S'):
    d = datetime.fromtimestamp(unixtime)
    return d.strftime(format)


def read_config_file(cfgfile):
    if not os.path.isfile(cfgfile):
        return None

    cfg = ConfigParser(allow_no_value=True)
    cfg.read(cfgfile)
    return cfg


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def write_lock_file(lockfile, pidno):
    with open(lockfile, 'w') as lockfd:
        lockfd.write(str(pidno))

    lockfd.close()
    return True


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
            except ValueError as err:
                return False, pid
            finally:
                lockfd.close()
                break

    if pid is None or pid <= 0:
        return False, pid

    return is_process_running(pid), pid


def is_process_running(pidno):
    """ Check for /proc/PID/status
    - If it exists, if not process is dead, return True
    - If exists and process state is Z, return False
    -
    TODO: Check also that that process is not in zombie?
    """
    if pidno is None:
        return False

    if proc_status(pidno) is not None:
        return True
    else:
        return False


def mysql_connect(dotmycnf, section='client'):
    try:
        cnf = read_config_file(dotmycnf)
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
    except MySQLdb.Error as e:
        raise Exception('Could not establish connection to MySQL server')


def emit_text_metric(name, value, textdir):
    if textdir is None:
        return True

    file = '%s/mysqlzfs.prom' % textdir
    wrote = False
    write = []

    if not os.path.isfile(file):
        write.append('%s %s' % (name, value))
        wrote = True
    else:
        with open(file, 'r') as fd:
            metrics = fd.readlines()
            for metric in metrics:
                if '%s ' % name in metric:
                    write.append('%s %s\n' % (name, value))
                    wrote = True
                else:
                    write.append('%s\n' % metric.rstrip())

        if not wrote:
            write.append('%s %s\n' % (name, value))

    with open(file, 'w') as fd:
        fd.writelines(write)

    return True
