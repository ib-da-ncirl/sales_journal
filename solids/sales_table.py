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
    Failure,
    Dict,
    String,
    lambda_solid, composite_solid, OutputDefinition, Output)
from dagster_pandas import DataFrame
from db_toolkit.postgres.postgresdb_sql import count_sql
from db_toolkit.postgres.postgresdb_sql import estimate_count_sql
from psycopg2.extras import execute_values


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
        if idx > 0:
            create_columns += ', '
            insert_columns += ', '

        create_columns += f'{row.field} {row.datatype} '
        insert_columns += f'{row.field} '

        if row.not_null != '':
            create_columns += 'NOT NULL '
        if row.default != '':
            # integer field default values may be real/str in table_desc
            dtype = row.datatype.lower()
            if dtype == 'integer' or dtype == 'smallint' or dtype == 'bigint':
                default = f'{int(row.default)}'
            else:
                default = f'{row.default}'
            create_columns += f'DEFAULT {default} '

        idx += 1

    yield Output(create_columns, 'create_columns')
    yield Output(insert_columns, 'insert_columns')


@solid(required_resource_keys={'postgres_warehouse'})
def upload_sales_table(context, sets_df: Dict, insert_columns: String, table_name: String) -> Dict:
    """
    Upload a DataFrame to the Postgres server, creating the table if it doesn't exist
    :param context: execution context
    :param sets_df: dict of DataSet with set ids as key
    :param insert_columns: column names for the database table
    :param table_name: name of database table to upload to
    """

    results = {}

    if len(sets_df.keys()) == 0:
        context.log.info(f'No records to upload to {table_name}')
    else:
        client = context.resources.postgres_warehouse.get_connection(context)

        if client is not None:

            insert_query = f'INSERT INTO {table_name} ({insert_columns}) VALUES %s;'

            cursor = client.cursor()

            # insert data sql
            for set_id in sets_df.keys():
                data_set = sets_df[set_id]
                tuples = [tuple(x) for x in data_set.df.values]

                try:
                    context.log.info(f'Uploading {len(tuples)} records for {set_id} to {table_name}')

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
                            # entries must follow order of tracking_data_columns.names from config ignoring the id columnn
                            'fileset': set_id,
                        }
                    }

                    context.log.info(f'Uploaded {post_len - pre_len} records from {set_id}')

                except psycopg2.Error as e:
                    context.log.warn(f'Error: {e}')

                finally:
                    # tidy up
                    cursor.close()
                    client.close_connection()

    return results