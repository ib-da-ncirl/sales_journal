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


@solid(
    output_defs=[
        OutputDefinition(dagster_type=String, name='create_tracking_columns', is_optional=False),
        OutputDefinition(dagster_type=String, name='insert_tracking_columns', is_optional=False),
    ],
)
def generate_tracking_table_fields_str(context, tracking_data_columns: Dict):
    """
    Upload a DataFrame to the Postgres server, creating the table if it doesn't exist
    :param context: execution context
    :param tracking_data_columns: dict containing list of column names and definitions for the database table
    """

    names = tracking_data_columns['value']['names']
    defs = tracking_data_columns['value']['defs']
    auto = tracking_data_columns['value']['auto']
    if len(names) != len(defs):
        raise Failure(f'Configuration error: tracking column name count ({len(names)}) does not match'
                      f'tracking column definition count ({len(defs)})')

    # add fields from the table description
    create_tracking_columns = ''
    insert_tracking_columns = ''
    for idx in range(len(names)):
        if len(create_tracking_columns) > 0:
            create_tracking_columns += ', '

        create_tracking_columns += f"{names[idx]} {defs[idx]}"

        if not auto[idx]:
            if len(insert_tracking_columns) > 0:
                insert_tracking_columns += ', '

            insert_tracking_columns += f"{names[idx]}"

    yield Output(create_tracking_columns, 'create_tracking_columns')
    yield Output(insert_tracking_columns, 'insert_tracking_columns')


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
def upload_tracking_table(context, results: Dict, insert_columns: String, table_name: String):
    """
    Upload a DataFrame to the Postgres server, creating the table if it doesn't exist
    :param context: execution context
    :param results: dict of results dicts with set ids as key
             { <set_id>: { 'uploaded': True|False,
                           'value': { 'fileset': <set_id>,
                                      'sj_pk_min': min value of sales journal primary key,
                                      'sj_pk_max': max value of sales journal primary key  }}}
    :param insert_columns: column names for the database table
    :param table_name: name of database table to upload to
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """

    if len(results.keys()) == 0:
        context.log.info(f"No tracking records to upload to '{table_name}'")
    else:
        client = context.resources.postgres_warehouse.get_connection(context)

        if client is not None:
            cursor = client.cursor()

            # insert data sql
            try:
                for set_id in results.keys():
                    result = results[set_id]

                    if result['uploaded']:

                        value = result['value']
                        insert_query = f'INSERT INTO {table_name} ({insert_columns}) VALUES (' +\
                                       ('%s,' * len(value))[0:-1] + ');'

                        query = cursor.mogrify(insert_query, list(value.values()))

                        context.log.info(f"Uploading result record for '{set_id}'")

                        cursor.execute(query)
                        client.commit()

            except psycopg2.Error as e:
                context.log.error(f'Error: {e}')
                if context.solid_config['fatal']:
                    raise e

            finally:
                # tidy up
                cursor.close()
                client.close_connection()


@solid()
def transform_loaded_records(context, prev_uploaded: Optional[DataFrame], tracking_data_columns: Dict) -> Optional[DataFrame]:
    if prev_uploaded is not None and len(prev_uploaded) > 0:
        names = tracking_data_columns['value']['names']
        prev_uploaded.columns = names
    return prev_uploaded
