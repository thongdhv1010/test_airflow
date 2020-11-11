#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Author: nguyenthong
    Date Created: 11/11/20
"""
import argparse

import MySQLdb


def process(args):
    # Connect
    db = MySQLdb.connect(host="10.19.96.8",
                         user="thongnv",
                         passwd="2TmI6EskMeI6K7Qe",
                         port=3306,
                         db="gapo_api")
    cursor = db.cursor()
    mysql_query = "SELECT * FROM gapo_api.tbl_comment limit 100;"
    cursor.execute(query=mysql_query)
    columns = [column[0] for column in cursor.description]
    results = cursor.fetchall()
    for result in results:
        row = dict(zip(columns, result))
        print("Result_ID: ",row.get('id'))



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
