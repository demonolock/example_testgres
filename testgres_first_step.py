import os
import testgres
import unittest

"""
Before test run you should set up env variable PG_CONFIG
"""

# Define the node name and paths for the test environment
# These params will set up pgdata path and log path for the node,
# using in this test
node_name = 'my_pg_node'
current_file_name = os.path.basename(os.path.abspath(__file__))
current_dir_path = os.path.dirname(os.path.realpath(__file__))
base_dir = os.path.join(current_file_name, node_name)
tmp_dir = os.path.join(current_dir_path, 'tmp_dirs', base_dir)

# List to keep track of nodes for cleanup
nodes_to_cleanup = []

# Create a NodeApp instance for managing PostgreSQL nodes
pg_node = testgres.NodeApp(tmp_dir, nodes_to_cleanup)


class TestgresFirstStep(unittest.TestCase):

    def test_create_and_fill_node(self):
        # Initialize and start a PostgreSQL node with specific configuration
        node = pg_node.make_simple(
            base_dir='node',
            set_replication=True,
            ptrack_enable=True,
            initdb_params=['--data-checksums'],
            pg_options={
                'max_wal_senders': '2',
                'shared_buffers': '16MB',
                'log_min_duration_statement': '0',
                'log_min_messages': 'LOG',
                'client_min_messages': 'LOG',
            }
        )

        # Start the node with a delay to ensure it's ready
        node.slow_start()

        # Create a new database and a table within it
        node.safe_psql('postgres', 'CREATE DATABASE test1')
        node.safe_psql('test1', 'CREATE TABLE T1 AS SELECT GENERATE_SERIES(0,100)')
        # The resutl of query can be get and processed
        result = node.safe_psql('test1', 'SELECT * FROM T1')

        # Restart the node, also can be used node.restart()
        node.stop()
        node.slow_start()

        # Create a new table in the 'postgres' database and fill
        node.safe_psql(
            'postgres',
            'CREATE TABLE t_heap AS SELECT 1 AS id, md5(i::text) AS text, '
            'md5(repeat(i::text,10))::tsvector AS tsvector FROM '
            'generate_series(0,100) i'
        )

        # For fast comparing tables  can be used table_checksum function
        before_checksum = node.table_checksum('t_heap')

        # Insert new rows into the table
        node.safe_psql(
            "postgres",
            "INSERT INTO t_heap (id, text, tsvector) VALUES "
            "(101, md5('101'::text), md5(repeat('101'::text, 10))::tsvector), "
            "(102, md5('102'::text), md5(repeat('102'::text, 10))::tsvector)"
        )

        # Calculate and store the checksum of the table after modification
        after_checksum = node.table_checksum('t_heap')

        # Assert that the checksums are different, indicating a change in the table
        assert after_checksum != before_checksum

        # - Testgres has ability to read params from pg_control file
        before_control_data = node.get_control_data()
        time_checkpoint_before = before_control_data['Time of latest checkpoint']

        # Initialize and run pgbench
        node.pgbench_init(scale=100, no_vacuum=True)
        # for 5 sec, 2 clients
        pgbench = node.pgbench(options=['-T', '5', '-c', '2'])
        pgbench.wait()

        # Get control data after running pgbench
        after_control_data = node.get_control_data()
        time_checkpoint_after = after_control_data['Time of latest checkpoint']

        assert time_checkpoint_after != time_checkpoint_before
