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

from dagster import (
    solid,
    Failure,
    Dict,
    String
)
from dagster_pandas import DataFrame
from db_toolkit.postgres.postgresdb_sql import count_sql
from db_toolkit.postgres.postgresdb_sql import estimate_count_sql
from psycopg2.extras import execute_values


@solid(required_resource_keys={'postgres_warehouse'})
def upload_table(context, sets_df: Dict, table_desc: DataFrame, table_name: String):
    """
    Upload a DataFrame to the Postgres server, creating the table if it doesn't exist
    :param context: execution context
    :param sets_df: dict of pandas DataFrames with set ids as key
    :param table_desc: pandas DataFrame containing details of the database table
    :param table_name: name of database table to upload to
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        # generate create table and insert data sql
        create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ('
        insert_query = f'INSERT INTO {table_name} ('

        fields = []
        idx = 0
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.itertuples.html#pandas.DataFrame.itertuples
        for row in table_desc.itertuples(index=False, name='FieldDef'):
            if idx > 0:
                create_table_query += ', '
                insert_query += ', '

            create_table_query += f'{row.field} {row.datatype} '
            if row.not_null != '':
                create_table_query += 'NOT NULL '
            if row.default != '':
                # integer field default values may be real/str in table_desc
                dtype = row.datatype.lower()
                if dtype == 'integer' or dtype == 'smallint' or dtype == 'bigint':
                    default = f'{int(row.default)}'
                else:
                    default = f'{row.default}'
                create_table_query += f'DEFAULT {default} '

            insert_query += f'{row.field} '
            fields.append(f'{row.field} ')

            idx += 1

        create_table_query += ');'
        insert_query += f') VALUES %s;'

        cursor = client.cursor()

        context.log.info(f'Execute create table query')
        cursor.execute(create_table_query)
        client.commit()


        # insert data sql
        for set_id in sets_df.keys():
            tuples = [tuple(x) for x in sets_df[set_id].values]

            context.log.info(f'Uploading {len(tuples)} records for {set_id}')

            # psycopg2.extras.execute_values() doesn't return much, so calc existing & post-insert count
            # estimate using estimate_count_sql
            cursor.execute(estimate_count_sql(table_name))
            result = cursor.fetchone()
            pre_len = result[0]

            execute_values(cursor, insert_query, tuples)
            client.commit()

            cursor.execute(estimate_count_sql(table_name))
            result = cursor.fetchone()
            post_len = result[0]

            context.log.info(f'Uploaded {post_len - pre_len} records from {set_id}')

        # tidy up
        cursor.close()
        client.close_connection()

