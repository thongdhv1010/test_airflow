#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Author: nguyenthong
    Date Created: 11/11/20
"""
import argparse
import re
import uuid
from datetime import datetime, timedelta

import MySQLdb
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def process(args):
    print("BEGIN")
    # Connect
    mysql_uri = 'mysql://thongnv:2TmI6EskMeI6K7Qe@10.19.96.8:3306/gapo_api'
    user, password, host, port, database = re.match('mysql://(.*?):(.*?)@(.*?):(.*?)/(.*)', mysql_uri).groups()
    db = MySQLdb.connect(host="10.19.96.8",
                         user="thongnv",
                         passwd="2TmI6EskMeI6K7Qe",
                         port=3306,
                         db="gapo_api")
    cursor = db.cursor()

    # Connect Cassandra
    fs_auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    fs_cluster = Cluster(['127.0.0.1'], auth_provider=fs_auth_provider, protocol_version=3)
    fs_session = fs_cluster.connect('tutorialspoint')

    # Insert cassandra
    insert_cql = "INSERT INTO aggregate_action(id, user_id, number_comment, start_time, end_time) " \
                 "VALUES (?, ?, ?, ?, ?);"
    prepared = fs_session.prepare(insert_cql)

    start_time = datetime.utcnow().replace(year=2019, second=0, microsecond=0)
    # end_time = start_time + timedelta(hours=1)
    end_time = start_time + timedelta(minutes=2)

    start_timestamp = start_time.timestamp()
    end_timestamp = end_time.timestamp()

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
                           (uuid.uuid4(), user_id, number_comment, start_timestamp * 1000, end_timestamp * 1000))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # Sql config
    # parser.add_argument("--mysql_host", help="Sql server", required=True)
    # parser.add_argument("--mysql_userid", help="Sql userid", required=True)
    # parser.add_argument("--mysql_password", help="Sql password", required=True)
    # parser.add_argument("--mysql_db", help="Sql relationdb", required=True)
    # parser.add_argument("--mysql_port", help="Mysql port", default=3306)

    # # Cassandra config
    # parser.add_argument("--cassandra_host", help="Cassandra config", required=True)
    # parser.add_argument("--cassandra_userid", help="Cassandra config", required=True)
    # parser.add_argument("--cassandra_password", help="Cassandra config", required=True)
    # parser.add_argument("--cassandra_keyspace", help="Cassandra config", required=True)
    # parser.add_argument("--cassandra_port", help="Cassandra port", default=9042)
    #
    # parser.add_argument("--execution_ts", help="Current execution timestamp, int_format")
    # parser.add_argument("--prev_ts", help="Previous execution timestamp, int_format")

    arguments = parser.parse_args()

    process(arguments)
