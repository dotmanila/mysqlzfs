#!/usr/bin/env python

import boto3
import logging
import MySQLdb
import os
import psutil
import re
import requests
import shutil
import sys
import signal
import time
import traceback
from ConfigParser import ConfigParser, NoOptionError
from botocore.exceptions import ClientError as BotoClientError
from collections import OrderedDict
from datetime import datetime, timedelta
from optparse import OptionParser

# INSTALL
# We need pip as there is a bug on older requests module version
#   pip install awscli requests -U
# Make sure [mysqlebs] section exists on /root/.my.cnf (or specify --dotmycnf)
# Make sure aws configure is ran (~/.aws/[config|credentials] exists)

MYSQLEBS_VERSION = 0.3
MYSQLEBS_CMD_SNAP = 'snapshot'
MYSQLEBS_CMD_VOLS = 'identify-volumes'

MYSQLEBS_SIGTERM_CAUGHT = False


def __sigterm_handler(signal, frame):
    global MYSQLEBS_SIGTERM_CAUGHT
    print('Signal caught, terminating')
    MYSQLEBS_SIGTERM_CAUGHT = True


class MysqlZfs(object):
    @staticmethod
    def buildopts():
        opt_usage = "Usage: %prog [options] COMMAND"
        opt_desc = "Managed EBS snapshots as MySQL backups"
        opt_epilog = ""
        parser = MysqlEbsOptParser(opt_usage, version="%prog " + str(MYSQLEBS_VERSION),
            description=opt_desc, epilog=opt_epilog)
        parser.add_option('-v', '--volume-ids', dest='volume_ids', type='string',
            help='Comma separated list of volume-ids to snapshot', default=None)
        parser.add_option('-f', '--all-volumes', dest='all_volumes', action="store_true",
            help=('Wether to snapshot all EBS volumes '
                  'instead of specifying volume-ids'), default=False)
        parser.add_option('-F', '--all-volumes-noboot', dest='all_volumes_noboot', 
            help=('Wether to snapshot all EBS volumes (except boot volume)'
                  'instead of specifying volume-ids'), default=False, action="store_true")
        parser.add_option('-n', '--retention-days', dest='retention_days', type='int',
            help='How many days worth of snapshot to keep (age of snapshots)', default=7)
        parser.add_option('-d', '--debug', dest='debug', action="store_true", 
            help='Enable debugging outputs', default=False)
        parser.add_option('-c', '--defaults-file', dest='dotmycnf', type='string', 
            help='Path to .my.cnf containing connection credentials to MySQL',
            default='/root/.my.cnf')
        parser.add_option('-r', '--skip-repl-check', dest='skipreplcheck', action="store_true", 
            help='Wether to skip replication check when taking the snapshot',
            default=False)
        parser.add_option('-x', '--run', dest='run', action="store_true", 
            help='Run the default actions for a command i.e. for s3 is upload',
            default=False)

        (opts, args) = parser.parse_args()
        
        cmds = [MYSQLEBS_CMD_SNAP, MYSQLEBS_CMD_VOLS]
        if len(args) == 1 and args[0] not in cmds:
            parser.error("Command not recognized, got '%s'. See more with --help" % args[0])
        elif len(args) <= 0:
            parser.error("Command not specified. See more with --help")
        elif len(args) > 1:
            parser.error("Multiple commands specified. See more with --help")
        else:
            opts.cmd = args[0]

        if opts.all_volumes or opts.all_volumes_noboot:
            if opts.volume_ids is not None:
                parser.error('Volume-ids and all-volumes are mutually exclusive')
            elif opts.all_volumes and opts.all_volumes_noboot:
                parser.error('All-volumes and all-volumes-noboot are mutually exclusive')
        elif opts.volume_ids is None and opts.cmd == MYSQLEBS_CMD_SNAP and opts.run:
            parser.error(('List of volume-ids is required (--volume-ids). '
                          'Use the command "identify-volumes" to try and list local volume-ids'))
        
        opts.ppid = os.getpid()
        opts.pcwd = os.path.dirname(os.path.realpath(__file__))

        return opts

    @staticmethod
    def create_logger(opts):
        logger = None
        logfile = os.path.join('/var/log/mysqlebs.log')
        logformat = '%(asctime)s <%(process)d> %(levelname)s mysqlzfs:: %(message)s'

        if not os.path.isdir(os.path.dirname(logfile)):
            os.mkdir(os.path.dirname(logfile))

        loglevel = None
        if opts.debug:
            loglevel = logging.DEBUG
        else:
            loglevel = logging.INFO

        logging.basicConfig(filename=logfile, level=loglevel, format=logformat)

        logger = logging.getLogger('mysqlebs')
        
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

    @staticmethod
    def which(file):
        for path in os.environ["PATH"].split(os.pathsep):
            filepath = os.path.join(path, file)
            if os.path.exists(filepath):
                    return filepath

        return None


class MysqlEbsSnapshotManager(object):
    def __init__(self, logger, opts):
        self.opts = opts
        self.logger = logger

        self.lockfile = '/tmp/mysqlebs-snapshot.lock'
        self.is_running, self.pid = MysqlZfs.read_lock_file(self.lockfile)

        if self.is_running:
            raise Exception('Another snapshot process is running with PID %d' % self.ppid)

        MysqlZfs.write_lock_file(self.lockfile, self.opts.ppid)

        self.ec2 = boto3.client('ec2')
        self.instance_id = self.ec2_instance_id()
        self.logger.info('This node\'s instance-id is %s' % self.instance_id)

        self.volume_ids = None
        if self.opts.volume_ids is not None:
            self.opts.volume_ids.strip().split(',')

    def ec2_instance_id(self):
        metadata_url = 'http://169.254.169.254/latest/meta-data/instance-id'
        try:
            resp = requests.get(metadata_url, timeout=2)
            return resp.text.strip()
        except requests.exceptions.RequestException, err:
            self.logger.error(str(err))
            raise Exception('Unable to determine instance-id')

    def ec2_list_ebs_volumes(self, instance_id):
        """ Try to list the attached EBS volumes on this instance.
        """
        vols = self.ec2.describe_volumes(Filters=[{
            'Name': 'attachment.instance-id',
            'Values': [instance_id]
            }])

        return vols['Volumes']

    def ec2_volumes_exists(self):
        filters = [
            {'Name': 'volume-id', 'Values': self.volume_ids},
            {'Name': 'attachment.instance-id', 'Values': [self.instance_id]}
        ]
        self.logger.debug(self.volume_ids)
        self.logger.debug(filters)
        vols = self.ec2.describe_volumes(Filters=filters)
        self.logger.debug(vols)
        if len(vols['Volumes']) < len(self.volume_ids):
            return False

        return True

    def ec2_create_snapshot(self, ts=None):
        """ aws ec2 create-snapshot
        """
        if ts is None:
            ts = datetime.today().strftime('%Y%m%d%H%M%S')

        desc = '%s mysqlebs Snapshot' % ts

        tags = [{
            'ResourceType': 'snapshot', 
            'Tags': [
                {'Key': 'mysqlebs-desc', 'Value': desc},
                {'Key': 'mysqlebs-ts', 'Value': ts}
            ]
        }]

        instance_spec = {'InstanceId': self.instance_id}
        if self.opts.all_volumes_noboot:
            instance_spec['ExcludeBootVolume'] = True
        elif self.opts.all_volumes:
            instance_spec['ExcludeBootVolume'] = False

        if self.opts.volume_ids is None:
            resp = self.ec2.create_snapshots(Description=desc, 
                                             InstanceSpecification=instance_spec,
                                             TagSpecifications=tags,
                                             DryRun=False,
                                             CopyTagsFromSource='volume')
            self.logger.debug(resp)
            return True

        for volume_id in self.volume_ids:
            resp = self.ec2.create_snapshot(Description=desc, 
                                            VolumeId=volume_id,
                                            TagSpecifications=tags,
                                            DryRun=False)
            self.logger.debug(resp)

        return True



    def ec2_list_ebs_snapshots(self):
        """ List available EBS snapshots. 
        """

    def ec2_prune_old_snapshots(self):
        """ Delete snapshots older than N days.
        """

    def create_snapshot(self):
        conn = None
        if self.volume_ids is not None and not self.ec2_volumes_exists():
            raise Exception('Specified volume-id(s) does not belong to this instance')

        try:
            conn = MysqlZfs.mysql_connect(self.opts.dotmycnf, 'mysqlebs')
            cur = conn.cursor(MySQLdb.cursors.DictCursor)

            if not self.opts.skipreplcheck:
                cur.execute('SHOW SLAVE STATUS')

                while True:
                    row = cur.fetchone()
                    if row is None: 
                        raise Exception('MySQL server is not running as replica')

                    break

                if row['Slave_IO_Running'] != 'Yes' or row['Slave_SQL_Running'] != 'Yes':
                    raise Exception('Replication thread(s) are not running')

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
            self.ec2_create_snapshot(snapname)

            self.logger.info('Snapshot %s complete' % snapname)
            self.logger.info('Pruning old snapshots')
            self.ec2_prune_old_snapshots()

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

    def snapshots_summary(self):
        """ Listing end to end snapshot summary from AWS via
        SDK is expensive. There is no way to query newest and oldest snapshots
        per volume-id without querying everything, especially if we are snapshotting
        multiple volumes. 

        For now, we leave it to the user to identify snapshots they want to use 
        i.e. for restore. We only tag them appropriately for proper identification.
        """
        return True

        snaps = self.ec2_list_ebs_snapshots()
        if len(self.snaps) <= 0:
            return False

        self.logger.info('Oldest snapshot %s' % self.snaps[0])
        self.logger.info('Latest snapshot %s' % self.snaps[-1])

        return True
        pass

    def list_volumes(self):
        vols = self.ec2_list_ebs_volumes(instance_id)
        self.logger.debug(vols)

        for vol in vols:
            self.logger.info('VolumeId: %s, Type: %s, Device: %s' % (
                vol['VolumeId'], vol['VolumeType'], vol['Attachments'][0]['Device']))


# http://stackoverflow.com/questions/1857346/\
# python-optparse-how-to-include-additional-info-in-usage-output
class MysqlEbsOptParser(OptionParser):
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
        ebsmgr = MysqlEbsSnapshotManager(logger, opts)

        if opts.cmd == MYSQLEBS_CMD_SNAP:
            if opts.run:
                ebsmgr.create_snapshot()
            else:
                ebsmgr.snapshots_summary()
        elif opts.cmd == MYSQLEBS_CMD_VOLS:
            ebsmgr.list_volumes()
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
