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

from dagster import execute_pipeline, pipeline, ModeDefinition
from dagster_toolkit.postgres import (
    postgres_warehouse_resource,
)
from dagster_toolkit.files import (
    load_csv,
)
from dagster_toolkit.environ import (
    EnvironmentDict,
)
from sales_journal.solids import (
    generate_table_fields_str,
    create_tables,
    transform_table_desc_df,
    generate_tracking_table_fields_str,
    generate_currency_table_fields_str,
    create_currency_tables,
)
import pprint


@pipeline(
    mode_defs=[
        ModeDefinition(
            # attach resources to pipeline
            resource_defs={
                'postgres_warehouse': postgres_warehouse_resource,
            }
        )
    ]
)
def create_sales_data_postgres_pipeline():
    """
    Definition of the pipeline to create the sales journal tables in Postgres
    """
    # load and process the postgres table information
    table_desc = transform_table_desc_df(
        load_csv()  # TODO should supply dtypes
    )
    # generate column string for creation and insert queries, for the sales_data and tracking_data tables
    create_data_columns, insert_data_columns = generate_table_fields_str(table_desc)
    create_tracking_columns, insert_tracking_columns = generate_tracking_table_fields_str()

    # create sales_data and tracking_data tables
    create_tables(create_data_columns, create_tracking_columns)


def execute_create_sales_data_postgres_pipeline(sj_config: dict, postgres_warehouse: dict):
    """
    Execute the pipeline to create the sales journal tables in Postgres
    :param sj_config: app configuration
    :param postgres_warehouse: postgres server resource
    """

    # environment dictionary
    env_dict = EnvironmentDict() \
        .add_solid_input('load_csv', 'csv_path', sj_config['sales_data_desc']) \
        .add_solid_input('load_csv', 'kwargs', {}, is_kwargs=True) \
        .add_solid('generate_table_fields_str') \
        .add_solid_input('generate_tracking_table_fields_str',
                         'tracking_data_columns', sj_config['tracking_data_columns']) \
        .add_composite_solid_input('create_tables', 'create_data_table', 'table_name',
                                   sj_config['sales_data_table']) \
        .add_composite_solid_input('create_tables', 'create_tracking_table', 'table_name',
                                   sj_config['tracking_data_table']) \
        .add_resource('postgres_warehouse', postgres_warehouse) \
        .build()

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(env_dict)

    result = execute_pipeline(create_sales_data_postgres_pipeline, environment_dict=env_dict)
    assert result.success

@pipeline(
    mode_defs=[
        ModeDefinition(
            # attach resources to pipeline
            resource_defs={
                'postgres_warehouse': postgres_warehouse_resource,
            }
        )
    ]
)
def create_currency_data_postgres_pipeline():
    """
    Definition of the pipeline to create the currency tables in Postgres
    """
    # generate column string for creation and insert queries, for the sales_data and tracking_data tables
    create_currency_columns, insert_currency_columns = generate_currency_table_fields_str()

    # create currency_data table
    create_currency_tables(create_currency_columns)


def execute_create_currency_data_postgres_pipeline(cur_config: dict, postgres_warehouse: dict):
    """
    Execute the pipeline to create the currency tables in Postgres
    :param cur_config: app configuration
    :param postgres_warehouse: postgres server resource
    """

    # environment dictionary
    env_dict = EnvironmentDict() \
        .add_solid_input('generate_currency_table_fields_str',
                         'table_data_columns', cur_config['currency_data_columns']) \
        .add_composite_solid_input('create_currency_tables', 'create_currency_table', 'table_name',
                                   cur_config['currency_data_table']) \
        .add_resource('postgres_warehouse', postgres_warehouse) \
        .build()

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(env_dict)

    result = execute_pipeline(create_currency_data_postgres_pipeline, environment_dict=env_dict)
    assert result.success

