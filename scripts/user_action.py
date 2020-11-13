#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Author: nguyenthong
    Date Created: 11/11/20
"""
import os
import re
from datetime import datetime, timedelta

import MySQLdb
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def process():
    # Connect
    mysql_uri = os.getenv('MYSQL_URI')
    user, password, host, port, database = re.match('mysql://(.*?):(.*?)@(.*?):(.*?)/(.*)', mysql_uri).groups()
    db = MySQLdb.connect(host=host,
                         user=user,
                         passwd=password,
                         port=int(port),
                         db=database)
    cursor = db.cursor()

    # Connect Cassandra
    fs_auth_provider = PlainTextAuthProvider(username=os.getenv('CASSANDRA_USER'),
                                             password=os.getenv('CASSANDRA_PWD'))
    fs_cluster = Cluster(os.getenv('CASSANDRA_HOST').strip().split(','),
                         auth_provider=fs_auth_provider,
                         protocol_version=3)
    fs_session = fs_cluster.connect(os.getenv('CASSANDRA_KEYSPACE'))

    # Insert cassandra
    insert_cql = "INSERT INTO aggregate_action(user_id, number_comment, start_time, create_time) " \
                 "VALUES (?, ?, ?, ?);"
    prepared = fs_session.prepare(insert_cql)

    run_time = datetime.utcnow().replace(year=2019, second=0, microsecond=0)
    # start_time = run_time - timedelta(hours=1)
    start_time = run_time - timedelta(minutes=2)

    start_timestamp = start_time.timestamp()
    end_timestamp = run_time.timestamp()

    mysql_query = "SELECT user_id, count(1) as 'number_comment' FROM gapo_api.tbl_comment " \
                  "where create_time >= %s and create_time < %s " \
                  "group by user_id;" % (start_timestamp, end_timestamp)

    cursor.execute(query=mysql_query)
    columns = [column[0] for column in cursor.description]
    results = cursor.fetchall()
    for result in results:
        row = dict(zip(columns, result))
        user_id = row.get('user_id')
        number_comment = row.get('number_comment')
        # insert cassandra
        fs_session.execute(prepared,
                           (user_id, number_comment, int(start_timestamp), datetime.utcnow().timestamp() * 1000))


if __name__ == '__main__':
    process()
