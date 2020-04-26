#!/usr/bin/env python3

import sys
import traceback
from mysqlzfs import *
from mysqlzfs import aws
from mysqlzfs import util as zfs_util
from mysqlzfs import zfs
from mysqlzfs.mysql import MySQLClient
from mysqlzfs.commands.binlog import MysqlBinlogStreamer
from mysqlzfs.commands.dump import MysqlDumper
from mysqlzfs.commands.mysqld import MysqlZfsService
from mysqlzfs.commands.mysqld_group import MysqlZfsServiceList
from mysqlzfs.commands.s3 import MysqlS3Client
from mysqlzfs.commands.zfs.snapshot import MysqlZfsSnapshotManager
from mysqlzfs.constants import *

# TODO items:
# Binlog streaming support
# Auto replication service after import
# How can we make sure exported snapshots are usable?
# Handling disk space issues.
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
# implement control for exported snapshot so that it is actually completed


if __name__ == "__main__":
    logger = None
    opts = None

    try:
        opts = zfs_util.buildopts()
        logger = zfs_util.create_logger(opts)
        zfsmgr = MysqlZfsSnapshotManager(opts)

        if opts.cmd == MYSQLZFS_CMD_EXPORT:
            if opts.run:
                zfsmgr.snapshot_to_bin()
            else:
                zfsmgr.show_binaries()
        elif opts.cmd == MYSQLZFS_CMD_CLONE:
            if opts.cleanup:
                mysqlds = MysqlZfsServiceList(opts)
                mysqlds.cleanup()
            elif opts.run:
                success = zfsmgr.clone_snapshot()
                if success and opts.stage:
                    mysqld = MysqlZfsService(opts)
                    mysqld.start()
            else:
                mysqlds = MysqlZfsServiceList(opts)
                mysqlds.show_sandboxes()
        elif opts.cmd == MYSQLZFS_CMD_IMPORT:
            success = zfsmgr.import_bin(opts.snapshot)
            if success and opts.stage:
                mysqld = MysqlZfsService(opts)
                mysqld.start()
        elif opts.cmd == MYSQLZFS_CMD_DUMP:
            dumper = MysqlDumper(opts, zfsmgr)
            if opts.run:
                dumper.start()
            else:
                dumper.status()
        elif opts.cmd == MYSQLZFS_CMD_MYSQLD:
            if opts.stop:
                mysqld = MysqlZfsService(opts)
                mysqld.stop()
            elif opts.start:
                mysqld = MysqlZfsService(opts)
                mysqld.start()
            elif opts.cleanup:
                mysqlds = MysqlZfsServiceList(opts)
                mysqlds.cleanup()
            else:
                mysqlds = MysqlZfsServiceList(opts)
                mysqlds.show_sandboxes()
        elif opts.cmd == MYSQLZFS_CMD_SNAP:
            if opts.run:
                zfsmgr.zfs_snapshot()
            else:
                zfsmgr.zfs_snapshot_summary()
        elif opts.cmd == MYSQLZFS_CMD_S3:
            s3 = MysqlS3Client(opts)
            if opts.run:
                s3.upload_dumps()
                s3.upload_binlogs()
            else:
                logger.info('S3 command does not have status subcommand yet')
        elif opts.cmd == MYSQLZFS_CMD_BINLOGD:
            if opts.run:
                binlogd = MysqlBinlogStreamer(opts)
                binlogd.start()
            else:
                logger.info('binlogd command does not have status subcommand yet')
        else:
            logger.error('Unknown command "%s"' % opts.cmd)

        logger.info('Done')

    except Exception as e:
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
