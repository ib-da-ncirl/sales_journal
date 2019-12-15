# The MIT License (MIT)
# Copyright (c) 2019 Ian Buttimer

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import psycopg2
from dagster import (
    solid,
    Dict,
    String,
    OutputDefinition, Output, Field, Bool, composite_solid, Optional)
from dagster_pandas import DataFrame
from dagster_toolkit.postgres import (
    query_table,
)
from db_toolkit.postgres import (
    count_sql,
    estimate_count_sql,
)
from psycopg2.extras import execute_values
import numpy
from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

@solid(
    output_defs=[
        OutputDefinition(dagster_type=String, name='create_columns', is_optional=False),
        OutputDefinition(dagster_type=String, name='insert_columns', is_optional=False),
    ],
)
def generate_table_fields_str(context, table_desc: DataFrame):
    """
    Upload a DataFrame to the Postgres server, creating the table if it doesn't exist
    :param context: execution context
    :param table_desc: pandas DataFrame containing details of the database table
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """

    # add fields from the table description
    create_columns = ''
    insert_columns = ''
    idx = 0
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.itertuples.html#pandas.DataFrame.itertuples
    for row in table_desc.itertuples(index=False, name='FieldDef'):
        if row.save.lower() != 'y':
            # don't save this entry to database
            continue

        if idx > 0:
            create_columns += ', '
            insert_columns += ', '

        create_columns += f'{row.field} {row.datatype} '
        insert_columns += f'{row.field} '

        if row.primary_key.lower() == 'y':
            create_columns += 'PRIMARY KEY '
        if row.not_null.lower() == 'y':
            create_columns += 'NOT NULL '
        if row.default != '':
            # integer field default values may be real/str in table_desc
            dtype = row.datatype.lower()
            if dtype == 'integer' or dtype == 'smallint' or dtype == 'bigint' or dtype == 'serial':
                default = f'{int(row.default)}'
            else:
                default = f'{row.default}'
            create_columns += f'DEFAULT {default} '

        idx += 1

    yield Output(create_columns, 'create_columns')
    yield Output(insert_columns, 'insert_columns')


@solid(required_resource_keys={'postgres_warehouse'},
       config={
           'fatal': Field(
               Bool,
               default_value=True,
               is_optional=True,
               description='Controls whether exceptions cause a Failure or not',
           )
       }
       )
def upload_sales_table(context, sets_df: Dict, insert_columns: String, table_name: String) -> Dict:
    """
    Upload a DataFrame to the Postgres server, creating the table if it doesn't exist
    :param context: execution context
    :param sets_df: dict of DataSet with set ids as key
    :param insert_columns: column names for the database table
    :param table_name: name of database table to upload to
    :return: dict of results with set id as the key
             { <set_id>: { 'uploaded': True|False,
                           'value': { 'fileset': <set_id>,
                                      'sj_pk_min': min value of sales journal primary key,
                                      'sj_pk_max': max value of sales journal primary key  }}}
    """

    results = {}

    if len(sets_df.keys()) == 0:
        context.log.info(f"No records to upload to '{table_name}'")
    else:
        client = context.resources.postgres_warehouse.get_connection(context)

        if client is not None:

            insert_query = f'INSERT INTO {table_name} ({insert_columns}) VALUES %s;'

            # insert data sql
            for set_id in sets_df.keys():
                data_set = sets_df[set_id]
                tuples = [tuple(x) for x in data_set.df.values]

                if len(tuples) > 0:
                    cursor = client.cursor()
                    try:
                        context.log.info(f"Uploading {len(tuples)} records for '{set_id}' to '{table_name}'")

                        # psycopg2.extras.execute_values() doesn't return much, so calc existing & post-insert count
                        # estimate using estimate_count_sql

                        # TODO better method of count estimation, estimate_count_sql doesn't work here

                        cursor.execute(estimate_count_sql(table_name))
                        result = cursor.fetchone()
                        pre_len = result[0]

                        execute_values(cursor, insert_query, tuples)
                        client.commit()

                        cursor.execute(estimate_count_sql(table_name))
                        result = cursor.fetchone()
                        post_len = result[0]

                        results[set_id] = {
                            'uploaded': True,
                            'value': {
                                # entries must follow order of tracking_data_columns.names from config
                                # ignoring the id column
                                'fileset': set_id,
                                'sj_pk_min': data_set.df['ID'].min(),
                                'sj_pk_max': data_set.df['ID'].max()
                            }
                        }

                        context.log.info(f"Uploaded estimated {post_len - pre_len} records from '{set_id}'")

                    except psycopg2.Error as e:
                        context.log.error(f'Error: {e}')
                        if context.solid_config['fatal']:
                            raise e

                    finally:
                        # tidy up
                        cursor.close()
                else:
                    context.log.info(f"No records to upload for '{set_id}' to '{table_name}'")
                    results[set_id] = {
                        'uploaded': True,   # so its saved to tracking table and won't get continually loaded
                        'value': {
                            # entries must follow order of tracking_data_columns.names from config
                            # ignoring the id column
                            'fileset': set_id,
                            'sj_pk_min': 0,
                            'sj_pk_max': 0
                        }
                    }

            client.close_connection()

    return results


@composite_solid()
def query_sales_data(sql: String) -> Optional[DataFrame]:
    """
    Query postgres sales_data table
    """
    query_data_table = query_table.alias('query_data_table')

    return query_data_table(sql)
