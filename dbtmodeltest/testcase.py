import os
import unittest
from contextlib import contextmanager

from unittest.mock import patch

import pandas as pd
import dbt.main as dbt
from sqlalchemy import create_engine
from dbt.adapters.factory import get_adapter, reset_adapters, register_adapter
from dbt.config import RuntimeConfig
from dbt.context import providers


class TestArgs:
    def __init__(self, kwargs):
        self.which = 'run'
        self.single_threaded = False
        self.profiles_dir = None
        self.project_dir = None
        self.__dict__.update(kwargs)


class DBTModelTestCase(unittest.TestCase):
    conn_name = '__test'
    database = None
    schema = None

    @contextmanager
    def get_connection(self, name=None):
        """Create a test connection context where all executed macros, etc will
        get self.adapter as the adapter.
        This allows tests to run normal adapter macros as if reset_adapters()
        were not called by handle_and_check (for asserts, etc)
        """
        if name is None:
            name = '__test'
        with patch.object(providers, 'get_adapter', return_value=self.adapter):
            with self.adapter.connection_named(name):
                conn = self.adapter.connections.get_thread_connection()
                yield conn

    def setUp(self):
        self.database = os.getenv('DBT_MODEL_TEST_DATABASE')
        self.schema = os.getenv('DBT_MODEL_TEST_SCHEMA')
        self.identifier_prefix = os.getenv('DBT_MODEL_TEST_IDENTIFIER_PREFIX')

        reset_adapters()

        kwargs = {
            'profile': 'modeltests',
            'profiles_dir': 'conf/',
            'target': None,
        }

        config = RuntimeConfig.from_args(TestArgs(kwargs))
        register_adapter(config)
        adapter = get_adapter(config)
        adapter.cleanup_connections()
        self.adapter = adapter

    def _adapter_sqlalchemy_conn_string(self):
        # TODO this needs to be dynamic for all supported dbt databases.
        if self.adapter.type() == 'postgres':
            postgres_conn_string_tmpl = 'postgresql://{user}:{password}@{host}:{port}/{database}'
            return postgres_conn_string_tmpl.format(
                user=self.adapter.config.credentials.user,
                password=self.adapter.config.credentials.password,
                port=self.adapter.config.credentials.port,
                host=self.adapter.config.credentials.host,
                schema=self.adapter.config.credentials.schema,
                database=self.adapter.config.credentials.database,
            )
        elif self.adapter.type() == 'snowflake':
            import ipdb; ipdb.set_trace();



        raise NotImplementedError(
            'Adapter type: {} is not supported by dbtmodel tests'.format(
                self.adapter.type(),
            )
        )

    def execute_model_with_refs(self, model, **ref_dfs):
        """
        TODO HOW SHOULD WE MOCK THE REFS?

        I can think of the following possibilities:

        - Rewrite the 'ref' to be a test namespaced table. This framework
            would write the datframe to that file using `to_table(...)`
        - Have some hackery to replace the ref with a "seed". We would
            need to rename the real model ref
        - ?

        :param model:
        :param ref_dfs:
        :return:
        """
        engine = create_engine(self._adapter_sqlalchemy_conn_string())
        with engine.connect() as conn:
            for ref_name, ref_df in ref_dfs.items():
                table_name = self.identifier_prefix + ref_name
                # drop table and cascade if exists...
                conn.execute('DROP TABLE {} CASCADE'.format(table_name))
                print('creating table: {}'.format(table_name))
                ref_df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='replace',
                    index=False,
                )

        return self.execute_model(model)

    def execute_model(self, model):
        dbt_args = [
            'run',
            '-m', model,
            '--profiles-dir', 'conf/',
            '--profile', 'ci'
        ]
        resp, success = dbt.handle_and_check(dbt_args)
        self.assertTrue(success)
        self.assertEqual(1, len(resp.results))
        rs = resp.results[0]
        print(rs.node.relation_name)

        sql = 'SELECT * FROM {}'.format(rs.node.relation_name)

        with self.get_connection(self.conn_name) as conn:
            df = pd.read_sql(sql, conn.handle)
            print(df)
            return df

    def assertDFEqual(self, df1, df2):
        self.assertTrue(
            df1.equals(df2), '\n{} \n not equal to:\n{}'.format(
                df1,
                df2
            )
        )
