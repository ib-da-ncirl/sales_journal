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
    query_table,
)
from dagster_toolkit.files import (
    load_csv,
)
from dagster_toolkit.environ import (
    EnvironmentDict,
)
from solids import (
    generate_table_fields_str,
    create_tables,
    upload_sales_table,
    get_table_desc_by_type,
    transform_sets_df,
    transform_table_desc_df,
    load_list_of_csv_files,
    create_csv_file_sets,
    read_sj_csv_file_sets,
    merge_promo_csv_file_sets,
    generate_tracking_table_fields_str,
    upload_tracking_table,
    generate_dtypes)


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
def csv_to_postgres_pipeline():
    """
    Definition of the pipeline to upload the sales journal data to Postgres
    """
    # load and process the postgres table information
    table_desc = transform_table_desc_df(
        load_csv()  # TODO should supply dtypes
    )
    table_desc_by_type = get_table_desc_by_type(table_desc)
    dtypes_by_root = generate_dtypes(table_desc, table_desc_by_type)

    # generate column string for creation and insert queries, for the sales_data and tracking_data tables
    create_data_columns, insert_data_columns = generate_table_fields_str(table_desc)
    create_tracking_columns, insert_tracking_columns = generate_tracking_table_fields_str()

    # create sales_data and tracking_data tables
    create_tables(create_data_columns, create_tracking_columns)

    # get previously uploaded file sets info
    prev_uploaded = query_table()

    # load the csv files in to sets, so that the csv files that relate to a common export are all together and load them
    sets = create_csv_file_sets(
        load_list_of_csv_files(), prev_uploaded
    )

    # read the sales journal
    sets_list, sets_df = read_sj_csv_file_sets(sets, dtypes_by_root)

    # merge the promo info into the sales journal
    sets_list, sets_df = merge_promo_csv_file_sets(sets_list, sets_df, dtypes_by_root)

    sets_df = transform_sets_df(sets_df, table_desc, table_desc_by_type)

    upload_results = upload_sales_table(sets_df, insert_data_columns)

    upload_tracking_table(upload_results, insert_tracking_columns)


def execute_csv_to_postgres_pipeline(sj_config: dict, postgres_warehouse: dict):
    """
    Execute the pipeline to upload the sales journal data to Postgres
    :param sj_config: app configuration
    :param postgres_warehouse: postgres server resource
    """

    # .add_solid_input('does_psql_table_exist', 'name', sj_config['tracking_table_query']) \

    # environment dictionary
    regex_patterns = sj_config['regex_patterns']
    env_dict = EnvironmentDict() \
        .add_solid_input('load_csv', 'csv_path', sj_config['sales_data_desc']) \
        .add_solid_input('load_csv', 'kwargs', {}, is_kwargs=True) \
        .add_solid('transform_table_desc_df') \
        .add_solid('generate_table_fields_str') \
        .add_solid_input('generate_tracking_table_fields_str',
                         'tracking_data_columns', sj_config['tracking_data_columns']) \
        .add_composite_solid_input('create_tables', 'create_data_table', 'table_name',
                                   sj_config['sales_data_table']) \
        .add_composite_solid_input('create_tables', 'create_tracking_table', 'table_name',
                                   sj_config['tracking_data_table']) \
        .add_solid_input('query_table', 'sql', sj_config['tracking_table_query']) \
        .add_solid_input('load_list_of_csv_files', 'db_data_path', sj_config['db_data_path']) \
        .add_solid_input('load_list_of_csv_files', 'date_in_name_pattern', sj_config['date_in_name_pattern']) \
        .add_solid_input('load_list_of_csv_files', 'date_in_name_format', sj_config['date_in_name_format']) \
        .add_solid_input('create_csv_file_sets', 'regex_patterns', regex_patterns) \
        .add_solid_input('read_sj_csv_file_sets', 'regex_patterns', regex_patterns) \
        .add_solid_input('merge_promo_csv_file_sets', 'regex_patterns', regex_patterns) \
        .add_solid('transform_sets_df') \
        .add_solid_input('upload_sales_table', 'table_name', sj_config['sales_data_table']) \
        .add_solid_input('upload_tracking_table', 'table_name', sj_config['tracking_data_table']) \
        .add_resource('postgres_warehouse', postgres_warehouse) \
        .build()

    print(env_dict)

    result = execute_pipeline(csv_to_postgres_pipeline, environment_dict=env_dict)
    assert result.success

