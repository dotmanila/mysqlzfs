#!/bin/env python3

from .constants import *


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
    # 5.6 GTID enabled replication events
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
