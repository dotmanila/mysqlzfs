#!/bin/env python3

import logging
import os
import re
import signal
from .. import zfs
from .mysqld import MysqlZfsService
from collections import OrderedDict

logger = logging.getLogger(__name__)


class MysqlZfsServiceList(object):
    """
    Manage a group of MysqlZfsService
    """

    def __init__(self, opts):
        self.sigterm_caught = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.opts = opts
        self.rootdir = '/%s' % self.opts.dataset

    def _signal_handler(self, signal, frame):
        self.sigterm_caught = True
        logger.info('Signal caught, cleaning up')

    def cleanup(self):
        sandboxes = self.scan_sandboxes()
        if sandboxes is None:
            logger.info('No sandboxes running on any stage datasets')
            return None

        for s in sandboxes:
            if self.opts.snapshot and self.opts.snapshot != s:
                continue

            mysqld = MysqlZfsService(self.opts, s)
            logger.info('+- %s' % sandboxes[s]['rootdir'])
            if mysqld.is_alive():
                logger.info('+--- MySQL is running, shutting down')
                mysqld.stop()

            logger.info('+--- Cleaning up ZFS dataset %s' % mysqld.dataset)
            success, error = zfs.destroy(mysqld.dataset, recursive=True)
            if success:
                logger.info('+--- Done')
            else:
                logger.error('+-- Unable to delete ZFS clone %s' % mysqld.dataset)

    def show_sandboxes(self):
        sandboxes = self.scan_sandboxes()
        if sandboxes is None:
            logger.info('No sandboxes running on any stage datasets')
            return None

        for s in sandboxes:
            mysqld = MysqlZfsService(self.opts, s)
            logger.info('+- %s' % sandboxes[s]['rootdir'])
            logger.info('+--- mysql --defaults-file=%s --socket=%s' % (
                             self.opts.dotmycnf, sandboxes[s]['socket']))
            if mysqld.is_alive():
                logger.info('+--- Running: Yes')
            else:
                logger.info('+--- Running: No')

            if sandboxes[s]['deployed']:
                logger.info('+--- MySQL deployed: Yes')
            else:
                logger.info('+--- MySQL deployed: No')

            logger.info('+--- Origin: %s' % sandboxes[s]['origin'])

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

            props, err = zfs.get(rootdir.strip('/'), ['origin'])
            if err is not '':
                logger.error('Unable to retrieve dataset property for %s' % rootdir.strip('/'))
                logger.error(err)
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

        props, err = zfs.get(rootdir.strip('/'), ['origin'])
        if err is not '':
            logger.error('Unable to retrieve dataset property for %s' % rootdir.strip('/'))
            logger.error('Returned "%s"' % err)
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
