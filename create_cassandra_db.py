#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
               ..
              ( '`<
               )(
        ( ----'  '.
        (         ;
         (_______,' 
    ~^~^~^~^~^~^~^~^~^~^~
    Author: nvt
    Date Created: 13/11/2020
"""

import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username=os.getenv('CASSANDRA_USER'),
                                      password=os.getenv('CASSANDRA_PWD'))
cluster = Cluster(os.getenv('CASSANDRA_HOST').strip().split(','),
                  auth_provider=auth_provider,
                  protocol_version=3)
session = cluster.connect(os.getenv('CASSANDRA_KEYSPACE'))

session.execute("""
    CREATE TABLE IF NOT EXISTS aggregate_action(
    user_id int,
    number_comment int,
    number_like int,
    start_time int,
    create_time timestamp,
    PRIMARY KEY (user_id, start_time));
   """)
