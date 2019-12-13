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
    OutputDefinition, Output, Optional, Field, Bool)
from dagster_pandas import DataFrame
from db_toolkit.postgres import (
    count_sql,
    estimate_count_sql,
)
from psycopg2.extras import execute_values
import pandas as pd


@solid(
    output_defs=[
        OutputDefinition(dagster_type=String, name='create_currency_columns', is_optional=False),
        OutputDefinition(dagster_type=String, name='insert_currency_columns', is_optional=False),
    ],
)
def generate_currency_table_fields_str(context, table_data_columns: Dict):
    """
    Upload a DataFrame to the Postgres server, creating the table if it doesn't exist
    :param context: execution context
    :param table_data_columns: dict containing list of column names and definitions for the database table
    """
    
    # TODO basically same as generate_tracking_table_fields_str, so make a generic solid

    names = table_data_columns['value']['names']
    defs = table_data_columns['value']['defs']
    auto = table_data_columns['value']['auto']
    if len(names) != len(defs) or len(names) != len(auto):
        raise Failure(f'Configuration error: column definition counts do not match: names ({len(names)}), '
                      f'definitions ({len(defs)}), auto ({len(auto)})')

    # add fields from the table description
    create_columns = ''
    insert_columns = ''
    for idx in range(len(names)):
        if len(create_columns) > 0:
            create_columns += ', '

        create_columns += f"{names[idx]} {defs[idx]}"

        if not auto[idx]:
            if len(insert_columns) > 0:
                insert_columns += ', '

            insert_columns += f"{names[idx]}"

    yield Output(create_columns, 'create_currency_columns')
    yield Output(insert_columns, 'insert_currency_columns')


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
def upload_currency_table(context, currency_eq_usd_df: DataFrame, insert_columns: String, table_name: String):
    """
    Upload a DataFrame to the Postgres server, creating the table if it doesn't exist
    :param context: execution context
    :param currency_eq_usd_df: panda Dataframe of currency to USD rates
    :param insert_columns: column names for the database table
    :param table_name: name of database table to upload to
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """

    if len(currency_eq_usd_df) == 0:
        context.log.info(f"No currency records to upload to '{table_name}'")
    else:
        client = context.resources.postgres_warehouse.get_connection(context)

        if client is not None:
            insert_query = f'INSERT INTO {table_name} ({insert_columns}) VALUES %s;'

            # insert data sql
            for column in currency_eq_usd_df.columns:
                if column == 'Date' or column == 'USD':
                    continue

                df = pd.concat([currency_eq_usd_df['Date'], currency_eq_usd_df[column]], axis=1)
                df['currency_code'] = column
                tuples = [tuple(x) for x in df.values]

                if len(tuples) > 0:
                    cursor = client.cursor()
                    try:
                        context.log.info(f"Uploading {len(tuples)} records for '{column}' to '{table_name}'")

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

                        context.log.info(f"Uploaded estimated {post_len - pre_len} records from '{column}'")

                    except psycopg2.Error as e:
                        context.log.error(f'Error: {e}')
                        if context.solid_config['fatal']:
                            raise e

                    finally:
                        # tidy up
                        cursor.close()
                else:
                    context.log.info(f"No records to upload for '{column}' to '{table_name}'")

            client.close_connection()

