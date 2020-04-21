#!/bin/env python3

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