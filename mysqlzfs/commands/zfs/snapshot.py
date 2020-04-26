#!/bin/env python3

import logging
import os
import re
import shlex
import shutil
import signal
import time
from ... import *
from ... import util as zfs_util
from ... import zfs
from ...constants import *
from ...mysql import MySQLClient
from collections import OrderedDict
from datetime import datetime, timedelta
from mysql.connector.errors import Error as MySQLError
from subprocess import Popen, PIPE

logger = logging.getLogger(__name__)


class MysqlZfsSnapshotManager(object):
    def __init__(self, opts):
        self.sigterm_caught = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.opts = opts
        self.snaps = None
        self.bins = None
        self.bins_dict = OrderedDict()
        self.error_message = ''

        if self.opts.dataset is not None and self.zfs_dataset_info(self.opts.dataset) is None:
            raise Exception('Specified dataset does not exist in any pool')

        self.list_binaries()
        self.list_snapshots()

    def _signal_handler(self, signal, frame):
        self.sigterm_caught = True
        logger.info('Signal caught, cleaning up')

    def list_snapshots(self, refresh=False):
        if self.snaps is not None and refresh is False:
            return len(self.snaps), ''

        cmd_list = ['/sbin/zfs', 'list', '-H', '-t', 'snap', '-d', '1', self.opts.dataset]
        p = Popen(cmd_list, stdout=PIPE, stderr=PIPE)
        logger.debug(' '.join(cmd_list))
        out, err = p.communicate()
        if err.decode('ascii') is not '':
            logger.error(err.decode('ascii'))
            return 0, err.decode('ascii')

        root_list = out.decode('ascii').split('\n')
        self.snaps = []
        for s in root_list:
            if self.opts.root not in s:
                continue
            snap = s.split('\t')
            self.snaps.append(re.sub(self.opts.root, '', snap[0]))

        if len(self.snaps) == 0:
            logger.info('No existing snapshots found')
        else:
            logger.info('Found %d snapshots' % len(self.snaps))

        return len(self.snaps), err

    def zfs_dataset_info(self, full_snapname):
        p = Popen(['/sbin/zfs', 'list', '-H', '-p', full_snapname], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err.decode('ascii') is not '':
            logger.error(err.decode('ascii'))
            return None

        return out.decode('ascii').split('\t')

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
            logger.debug('Found %d snapshot(s) taken today' % len(snapshots_today))

            if len(snapshots_today) == 0:
                logger.error('No existing snapshots from today!')
                return False

            if self.opts.incr or self.opts.full:
                snapname = snapshots_today[-1]
            else:
                snapname = snapshots_today[0]

        for fs in self.opts.fslist:
            full_name = '%s%s' % (fs, snapname)
            snap_info = self.zfs_dataset_info(full_name)
            if snap_info is None:
                logger.error('%s is not found from root dataset')
                return False

            full_names.append(full_name)

        last_export_type, last_export_name, last_set = self.find_last_export()
        execute_full_snapshot = False
        if (snapdt.isoweekday() == 7 or self.bins is None or self.opts.full) and \
                (self.bins is not None and not self.opts.incr):
            logger.info("Forcing full snapshot (i.e. Sunday, no previous snapshot or explicitly requested)")
            execute_full_snapshot = True

        if last_export_type is not MYSQLZFS_EXPTYPE_NONE and int(last_export_name) >= int(snapname):
            logger.info("Target snapshot (%s) has already been exported" % snapname)
            logger.info("Last export was %s, last snapshot is %s" % (last_export_name, snapname))
            logger.info("Try exporting most recent snapshot with --incr/--full")
            return True

        if not execute_full_snapshot:
            snapprop, snaperr = zfs.get('%s@s%s' % (self.opts.dataset, last_export_name), ['origin'])
            if snapprop is None:
                logger.error('Looks like the source for incremental snapshot is missing')
                logger.error(snaperr)
                return False

        for fs in self.opts.fslist:
            full_name = '%s%s' % (fs, snapname)
            if execute_full_snapshot:
                logger.info('Start full snapshot to binary copy for %s' % full_name)
                full_binname = os.path.join(self.opts.bindir, snapday)

                if not os.path.isdir(full_binname):
                    os.mkdir(full_binname)

                binname = '_'.join(re.sub('@', '/', full_name).split('/')[::-1])
                full_binname = os.path.join(full_binname, '%s_full.zfs' % binname)

                if not self.zfs_send_to_bin(full_name, full_binname):
                    has_errors = True
                    break
            else:
                logger.info('Start incremental snapshot to binary copy for %s' % full_name)

                full_binname = os.path.join(self.opts.bindir, last_set)

                binname = '_'.join(re.sub('@', '/', full_name).split('/')[::-1])
                full_binname = os.path.join(full_binname, '%s_incr.zfs' % binname)

                if not self.zfs_send_to_bin(full_name, full_binname, '@s%s' % last_export_name):
                    has_errors = True
                    break

            full_bins.append(full_binname)

        if has_errors:
            for fs in full_bins:
                logger.info('Cleaning up %s' % fs)
                if os.path.isfile(fs):
                    os.unlink(fs)

            return False

        self.list_binaries()
        if len(self.bins) > self.opts.retention_sets:
            logger.debug('Export sets list %s' % str(self.bins))
            # delete only the oldest set when the newest one is full
            # this means if we want to keep two weeks/sets, we make it three here
            purge_list = list(self.bins.keys())[:-self.opts.retention_sets]
            logger.debug('Pruning export sets %s' % str(purge_list))
            for folder in purge_list:
                purge_folder = os.path.join(self.opts.bindir, folder)
                logger.info('Pruning export set %s' % purge_folder)
                shutil.rmtree(purge_folder)

        return True

    def zfs_send_to_bin(self, full_snapname, full_binname, incr_snapname=None):
        p_send = None

        if os.path.isfile(full_binname):
            logger.error('%s exists, aborting' % full_binname)
            return False

        cmd_send = '/sbin/zfs send -P -c -v'
        if incr_snapname is not None:
            cmd_send = '%s -i %s' % (cmd_send, incr_snapname)
        cmd_send = '%s %s' % (cmd_send, full_snapname)
        binfd = os.open(full_binname, os.O_WRONLY|os.O_CREAT|os.O_TRUNC)
        binlog = os.open(re.sub('.zfs$', '.log', full_binname), os.O_WRONLY|os.O_CREAT|os.O_TRUNC)

        logger.info('Running command [%s > %s]' % (cmd_send, full_binname))

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
            logger.error('ZFS send command failed with code: %d' % r)
            return False

        zfs_util.emit_text_metric(
            'mysqlzfs_last_export{dataset="%s"}' % self.opts.dataset,
            int(time.time()), self.opts.metrics_text_dir)

        return True

    def list_binaries(self):
        self.bins = OrderedDict()
        root_list = os.listdir(self.opts.bindir)
        root_list.sort()
        if len(root_list) == 0:
            self.bins = None
            logger.info('No existing set of binary backups found')
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
            logger.info('Binary export set: %s' % s)
            size_h = zfs_util.sizeof_fmt(self.bins_dict[s][self.bins[s]['full']]['size'])
            logger.info('+- full: %s %s' % (self.bins[s]['full'], size_h))
            for i in self.bins[s]['incr']:
                size_h = zfs_util.sizeof_fmt(self.bins_dict[s][i]['size'])
                logger.info('+--- incr: %s %s' % (i, size_h))

    def find_last_export(self):
        if self.bins is None:
            return MYSQLZFS_EXPTYPE_NONE, MYSQLZFS_EXPTYPE_NONE, MYSQLZFS_EXPTYPE_NONE

        last_set = list(self.bins.keys())[-1]
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
                    logger.error('Expected binary backup is missing %s' % bin_fullpath)
                    return False

                bin_dict[b][dataset_name] = { 'file': bin_fullpath, 'dataset': full_dataset_name }

            is_incr = True

        for b in bin_dict:
            for ds in bin_dict[b]:
                bin_fullpath = bin_dict[b][ds]['file']
                bin_dataset = bin_dict[b][ds]['dataset']
                logger.info('Importing %s' % bin_fullpath)

                p_receive = None

                cmd_receive = ['/sbin/zfs', 'receive', bin_dataset]
                binfd = os.open(bin_fullpath, os.O_RDONLY)
                p_receive = Popen(cmd_receive, stdin=binfd, stderr=PIPE)
                logger.debug(cmd_receive)

                r = p_receive.poll()
                poll_count = 0
                poll_size = 0
                while r is None:
                    time.sleep(2)
                    p_receive.poll()

                    # Workaround poll() not catching return code for small snapshots
                    # https://lists.gt.net/python/bugs/633489
                    current_size = int(self.zfs_dataset_info(bin_dataset)[1])
                    logger.debug('%s %d' % (bin_dataset, current_size))
                    if poll_size == current_size:
                        poll_count = poll_count + 1
                    else:
                        poll_count = 0

                    poll_size = current_size
                    if poll_count > 3:
                        r = p_receive.wait()

                os.close(binfd)

                if r != 0:
                    logger.error('ZFS receive command failed with code: %d' % r)
                    return False

        return True

    def zfs_destroy_dataset(self, dataset, recursive=True):
        success, error = zfs.destroy(dataset, recursive)

        if not success:
            logger.error(error)
            return success

        return True

    def zfs_create_dataset(self, dataset, properties={}):
        p = Popen(['/sbin/zfs', 'create', dataset], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err.decode('ascii') is not '':
            logger.error(err.decode('ascii'))
            return False

        if len(properties) > 0:
            for k in properties:
                p = Popen(['/sbin/zfs', 'set', '%s=%s' % (k, properties[k]), dataset],
                          stdout=PIPE, stderr=PIPE)
                out, err = p.communicate()
                if err.decode('ascii') is not '':
                    logger.error(err.decode('ascii'))
                    self.zfs_destroy_dataset(dataset)
                    return False

        return True

    def import_bin(self, binname):
        bin_type, bin_name, bin_set, bin_series = self.find_bin_set(binname)
        if bin_set is None:
            logger.error('Binary backup does not belong to any set')
            return False

        # Create root staging dataset, this will be on top of the main dataset
        # for now.
        root_target = '%s/s%s' % (self.opts.dataset, binname)

        dsprop, zfserr = zfs.get(root_target, ['origin'])
        if dsprop is not None:
            logger.error('Target dataset %s already exists, cannot import' % root_target)
            return False

        if not self.zfs_create_dataset(root_target):
            logger.error('Unable to create staging ZFS dataset %s' % root_target)
            return False

        if not self.zfs_receive_from_bin(bin_series, bin_set, root_target):
            logger.error('An error occurred importing the binary backup set')
            self.zfs_destroy_dataset(root_target, True)
            return False

        return True

    def zfs_snapshot(self):
        mysql_client = None

        try:
            mysql_client = MySQLClient({'option_files': self.opts.dotmycnf,
                                        'option_groups': ['mysqlzfs', 'zfsmysql', 'client']})

            if not self.opts.skip_repl_check and not mysql_client.replication_status():
                logger.error('Replication thread(s) are not running')
                return False

            if self.opts.skip_repl_check:
                logger.debug('Locking tables for backup')
                mysql_client.query('LOCK TABLES FOR BACKUP')
            else:
                logger.debug('Stopping SQL thread')
                if not mysql_client.stop_replication():
                    logger.error('STOP SLAVE failed')
                    return False
                logger.debug('Flushing tables')
                mysql_client.query('FLUSH TABLES')

            snapshot_ts = datetime.today().strftime('%Y%m%d%H%M%S')
            logger.debug('Taking snapshot')
            args = ['/sbin/zfs', 'snap', '-r', '%s@s%s' % (self.opts.dataset, snapshot_ts)]

            p = Popen(args, stdout=PIPE, stderr=PIPE)
            out, err = p.communicate()
            if err.decode('ascii') is not '':
                logger.error(err.decode('ascii'))
                return False

            logger.info('Snapshot %s@s%s complete' % (self.opts.dataset, snapshot_ts))

            zfs_util.emit_text_metric(
                'mysqlzfs_last_snapshot{dataset="%s"}' % self.opts.dataset,
                int(time.time()), self.opts.metrics_text_dir)

            snapshots_prune_list = self.snaps[:-(self.opts.retention_sets-1)]
            logger.info('Pruning %d old snapshots' % len(snapshots_prune_list))
            for s in snapshots_prune_list:
                logger.debug(' - %s@s%s' % (self.opts.dataset, s))
                self.zfs_destroy_dataset('%s@s%s' % (self.opts.dataset, s))

        except MySQLError as err:
            logger.error('A MySQL error has occurred, aborting new snapshot')
            logger.error(str(err))
            return False
        finally:
            if mysql_client is not None:
                if self.opts.skip_repl_check:
                    logger.debug('Unlocking tables')
                    mysql_client.query('UNLOCK TABLES')
                else:
                    logger.debug('Resuming replication')
                    mysql_client.start_replication()
                mysql_client.close()

    def zfs_snapshot_summary(self):
        if len(self.snaps) <= 0:
            return False

        logger.info('Oldest snapshot %s' % self.snaps[0])
        logger.info('Latest snapshot %s' % self.snaps[-1])

        return True

    def clone_snapshot(self):
        root_snapname = '%s@s%s' % (self.opts.dataset, self.opts.snapshot)
        root_target = '%s/s%s' % (self.opts.dataset, self.opts.snapshot)

        dsprop, zfserr = zfs.get(root_target, ['origin'])
        if dsprop is not None:
            logger.error('Target dataset %s already exists, cannot clone' % root_target)
            return False

        logger.info('Cloning %s to %s' % (root_snapname, root_target))
        snapinfo = self.zfs_dataset_info(root_snapname)
        if snapinfo is None:
            logger.error('Missing root snapshot %s' % root_snapname)
            return False

        for fs in self.opts.fslist:
            snapname = '%s%s' % (fs, self.opts.snapshot)
            snapinfo = self.zfs_dataset_info(snapname)
            if snapinfo is None:
                logger.error('Missing child on this dataset %s' % snapname)
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

        logger.info('Clone successfully completed')
        return True

    def zfs_clone_snapshot(self, snapshot, target):
        p = Popen(['/sbin/zfs', 'clone', snapshot, target], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if err.decode('ascii') is not '':
            logger.error(err.decode('ascii'))
            return False

        return True

    def destroy_clone(self, snapshot):
        dataset = '%s/s%s' % (self.opts.dataset, snapshot)
        return self.zfs_destroy_dataset(dataset, True)

