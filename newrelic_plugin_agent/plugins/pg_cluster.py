"""
PostgreSQL Cluster Plugin

"""
import logging
import psycopg2
from psycopg2 import extras

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)

CLUSTER_ROLE = """
SELECT pg_is_in_recovery();
"""
DATABASE = 'SELECT * FROM pg_stat_database;'


class PostgreSqlCluster(base.HTTPStatsPlugin):

    GUID = 'com.meetme.newrelic_postgresql_cluster_agent'

    def __init__(self):
        super(PostgreSqlCluster, self).__init__()
        self.node_list = self.config.nodes
        self.dbname = self.config.dbname
        self.user = self.config.user
        self.password = self.config.password

    def add_http_status_stats(self):
        status = 0
        status_servers_num = 0
        for node in self.node_list:
            host = self.node.get('host', 'localhost')
            status_port = self.node.get('status_port', '15432')
            url = 'http://%s:%s/' % (host, status_port)
            result = self.http_get(url=url)
            # for governor http status
            # http_status   |   cluster role    |   metrics
            # 200           |   master          |   2
            # 502           |   slave           |   1
            # error         |   offline         |   0
            if result == '':
                status = 0
            elif result is None:
                status = 1
            else:
                status = 2

            if status > 0:
                status_servers_num += 1

            self.add_gauge_value('PG_Cluster/HttpStatus/%s' % host,
                                 None,
                                 status,
                                 count=1)

        self.add_gauge_value('PG_Cluster/HttpStatusServersNum',
                             None,
                             status_servers_num,
                             count=1)

    def add_master_slave_stats(self):
        cluster_roles = {}
        master = ''
        slaves_number = 0
        for node in self.node_list:
            host = self.node.get('host', 'localhost')
            kwargs = {'host': host,
                      'port': self.node.get('port', 5432),
                      'user': self.node.get('user', ''),
                      'password': self.node.get('password', ''),
                      'database': self.node.get('dbname', '')}
            try:
                conn = psycopg2.connect(**kwargs)
            except:
                cluster_roles[host] = 0
                continue

            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
            cursor.execute(CLUSTER_ROLE)
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            cluster_roles[host] = 1
            if not data.get('pg_is_in_recovery', True):
                cluster_roles[host] = 2
                master = host

            slaves_number += 1
            self.add_gauge_value('PG_Cluster/ClusterRole/%s' % host,
                                 None,
                                 cluster_roles[host],
                                 count=1)

        self.add_gauge_value('PG_Cluster/SlavesNum',
                             None,
                             slaves_number,
                             count=1)

        switch_over = 0
        with open('/tmp/last_pg_master', 'w+') as f:
            last_master = f.readline().strip()
            if master != last_master:
                switch_over = 1
                f.write(master)

        self.add_gauge_value('PG_Cluster/SwitchOver',
                             None,
                             switch_over,
                             count=1)

    def add_cluster_stats(self):
        status = 1
        cluster_host = self.node.get('cluster_host', 'localhost')
        cluster_port = self.node.get('cluster_port', 5432)
        kwargs = {'host': cluster_host,
                  'port': cluster_port,
                  'user': self.node.get('user', ''),
                  'password': self.node.get('password', ''),
                  'database': self.node.get('dbname', '')}
        data = {}
        try:
            conn = psycopg2.connect(**kwargs)
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
            cursor.execute(DATABASE)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
        except:
            data = {}

        if not data:
            status = 0

        self.add_gauge_value('PG_Cluster/ClusterStatus',
                             None,
                             status,
                             count=1)

    def poll(self):
        self.initialize()
        self.add_http_status_stats()
        self.add_master_slave_stats()
        self.add_cluster_stats()
        self.finish()