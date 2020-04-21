#!/bin/env python3

import mysql.connector


class MySQLClient(object):
    def __init__(self, params, header='mysqlzfs', allow_local_infile=False):
        self.conn = mysql.connector.connect(**params, autocommit=True, use_pure=True,
                                            allow_local_infile=allow_local_infile)
        self.query_header = header
        self.rowcount = None

    def sqlize(self, sql):
        sql = '/* %s */ %s' % (self.query_header, sql)
        return sql

    def affected_rows(self):
        return self.conn.affected_rows

    def cursor(self):
        return mysql.connector.cursor.MySQLCursorDict(self.conn)

    def query(self, sql, args=None):
        results = None
        cursor = mysql.connector.cursor.MySQLCursorDict(self.conn)
        cursor.execute(self.sqlize(sql), args)

        if cursor.with_rows:
            results = cursor.fetchall()

        self.rowcount = cursor.rowcount

        cursor.close()
        return results

    def fetchone(self, sql, column=None):
        results = self.query(sql)
        if len(results) == 0:
            return None

        if column is not None:
            return results[0][column]

        return results[0]

    def start_replication(self, sql_thread=False, io_thread=False):
        sql = 'START SLAVE'
        if sql_thread:
            sql = "%s SQL_THREAD" % sql
        elif io_thread:
            sql = "%s IO_THREAD" % sql

        self.query(sql)
        return self.replication_status(sql_thread=sql_thread, io_thread=io_thread)

    def stop_replication(self, sql_thread=False, io_thread=False):
        sql = 'STOP SLAVE'
        if sql_thread:
            sql = "%s SQL_THREAD" % sql
        elif io_thread:
            sql = "%s IO_THREAD" % sql

        self.query(sql)
        return not self.replication_status(sql_thread=sql_thread, io_thread=io_thread)

    def replication_status(self, sql_thread=False, io_thread=False):
        status = self.show_slave_status()
        sql_running = True if status['Slave_SQL_Running'] == 'Yes' else False
        io_running = True if status['Slave_IO_Running'] == 'Yes' else False

        if sql_thread:
            return sql_running
        elif io_thread:
            return io_running

        return sql_running and io_running

    def show_variables(self, variable_name, session=False):
        if session:
            sql = 'SELECT @@session.%s AS v' % variable_name
        else:
            sql = 'SELECT @@global.%s AS v' % variable_name

        return self.fetchone(sql, 'v')

    def show_master_status(self):
        sql = 'SHOW MASTER STATUS'
        return self.fetchone(sql)

    def show_slave_status(self):
        sql = 'SHOW SLAVE STATUS'
        return self.fetchone(sql)

    def seconds_behind_master(self):
        status = self.show_slave_status()
        if status is None:
            return status

        return status['Seconds_Behind_Master']

    def show_binary_logs(self):
        sql = 'SHOW BINARY LOGS'
        return self.query(sql)

    def query_array(self, sql, args=None):
        results = None
        cursor = mysql.connector.cursor.MySQLCursor(self.conn)
        cursor.execute(self.sqlize(sql), args)

        if cursor.with_rows:
            results = cursor.fetchall()

        self.rowcount = cursor.rowcount

        cursor.close()
        return results

    def close(self):
        self.conn.close()
