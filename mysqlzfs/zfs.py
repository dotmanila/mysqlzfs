#!/bin/env python3

from collections import OrderedDict
from subprocess import Popen, PIPE, STDOUT, CalledProcessError


def clone():
    pass


def snapshot():
    pass


def destroy():
    pass


def create():
    pass


def list():
    pass


def get(dataset, properties=[]):
    cmd_get = ['/sbin/zfs', 'get', '-H', '-p']
    if len(properties) == 0:
        cmd_get.append('all')
    else:
        cmd_get.append(','.join(properties))
    cmd_get.append(dataset)

    p = Popen(cmd_get, stdout=PIPE, stderr=PIPE)
    # print(str(cmd_get))
    out, err = p.communicate()

    if err.decode('ascii') is not '':
        return None, err.decode('ascii')

    root_list = out.decode('ascii').split('\n')

    prop_list = OrderedDict()
    for s in root_list:
        if s == '':
            continue

        p = s.split('\t')
        prop_list[p[1]] = p[2]

    return prop_list, ''
