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
from .currency_pipelines import currency_pipeline_environmental_dict
from sales_journal.solids import (
    generate_table_fields_str,
    upload_sales_table,
    get_table_desc_by_type,
    get_table_desc_type_limits,
    transform_sets_df,
    transform_table_desc_df,
    load_list_of_csv_files,
    create_csv_file_sets,
    filter_load_file_sets,
    read_sj_csv_file_sets,
    merge_promo_csv_file_sets,
    merge_segs_csv_file_sets,
    generate_tracking_table_fields_str,
    upload_tracking_table,
    transform_loaded_records,
    currency_transform_sets_df,
    generate_dtypes,
    query_sales_data,

    generate_currency_table_fields_str,
    read_currency_codes,
    read_sdr_per_currency,
    transform_sdr_per_currency,
    read_sdr_per_usd,
    transform_usd_sdr,
    read_sdr_valuation,
    read_ex_rates_per_usd,
    transform_rates,
    transform_ex_rates_per_usd,

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

    table_type_limits = get_table_desc_type_limits(table_desc)

    # generate column string for creation and insert queries, for the sales_data and tracking_data tables
    create_data_columns, insert_data_columns = generate_table_fields_str(table_desc)
    create_tracking_columns, insert_tracking_columns = generate_tracking_table_fields_str()

    # get previously uploaded file sets info
    prev_uploaded, uploaded_ids = transform_loaded_records(
        query_table(),
        query_sales_data()
    )

    # load the csv files in to sets, so that the csv files that relate to a common export are all together and load them
    sets = create_csv_file_sets(
        load_list_of_csv_files(), prev_uploaded
    )

    # read the sales journal
    sets_list, sets_df = read_sj_csv_file_sets(
        filter_load_file_sets(sets), dtypes_by_root, prev_uploaded, uploaded_ids
    )

    # merge the promo info into the sales journal
    sets_list, sets_df = merge_promo_csv_file_sets(sets_list, sets_df, dtypes_by_root)

    # merge the segs info into the sales journal
    sets_list, sets_df = merge_segs_csv_file_sets(sets_list, sets_df, dtypes_by_root)

    sets_df = transform_sets_df(sets_df, table_desc, table_desc_by_type, table_type_limits)

    upload_results = upload_sales_table(sets_df, insert_data_columns)

    upload_tracking_table(upload_results, insert_tracking_columns)


def execute_csv_to_postgres_pipeline(sj_config: dict, postgres_warehouse: dict):
    """
    Execute the pipeline to upload the sales journal data to Postgres
    :param sj_config: app configuration
    :param postgres_warehouse: postgres server resource
    """

    # environment dictionary
    regex_patterns = sj_config['regex_patterns']
    load_file_sets = None
    if 'load_file_sets' in sj_config:
        load_file_sets = sj_config['load_file_sets']

    env_dict = cvs_pipeline_environmental_dict(EnvironmentDict(), sj_config, postgres_warehouse) \
        .build()

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(env_dict)

    run_mode = 'normal'
    if 'csv_pipeline_run_mode' in sj_config.keys():
        run_mode = sj_config['csv_pipeline_run_mode'].lower()
    if run_mode:
        loop = True
        while loop:
            result = execute_pipeline(csv_to_postgres_pipeline, environment_dict=env_dict)
            assert result.success

            upload_results = result.result_for_solid('upload_sales_table').output_value()
            loop = len(upload_results.keys()) > 0
            result = None

    else:
        # normal mode
        result = execute_pipeline(csv_to_postgres_pipeline, environment_dict=env_dict)
        assert result.success


def cvs_pipeline_environmental_dict(env_dict: EnvironmentDict, sj_config: dict,
                                    postgres_warehouse: dict) -> EnvironmentDict:
    """
    Execute the pipeline to upload the sales journal data to Postgres
    :param env_dict:
    :param sj_config: app configuration
    :param postgres_warehouse: postgres server resource
    """

    # environment dictionary
    regex_patterns = sj_config['regex_patterns']
    load_file_sets = None
    if 'load_file_sets' in sj_config:
        load_file_sets = sj_config['load_file_sets']
    if 'exclude_file_sets' in sj_config:
        exclude_file_sets = sj_config['exclude_file_sets']

    env_dict.add_solid_input('load_csv', 'csv_path', sj_config['sales_data_desc']) \
        .add_solid_input('load_csv', 'kwargs', {}, is_kwargs=True) \
        .add_solid('transform_table_desc_df') \
        .add_solid('generate_table_fields_str') \
        .add_solid_input('generate_tracking_table_fields_str',
                         'tracking_data_columns', sj_config['tracking_data_columns']) \
        .add_solid_input('transform_loaded_records', 'tracking_data_columns', sj_config['tracking_data_columns']) \
        .add_solid_input('transform_loaded_records', 'sj_pk_range', sj_config['sj_pk_range']) \
        .add_solid_input('query_table', 'sql', sj_config['tracking_table_query']) \
        .add_solid_input('query_sales_data', 'sql', sj_config['sales_id_query']) \
        .add_solid_input('load_list_of_csv_files', 'db_data_path', sj_config['db_data_path']) \
        .add_solid_input('load_list_of_csv_files', 'date_in_name_pattern', sj_config['date_in_name_pattern']) \
        .add_solid_input('load_list_of_csv_files', 'date_in_name_format', sj_config['date_in_name_format']) \
        .add_solid_input('create_csv_file_sets', 'regex_patterns', regex_patterns) \
        .add_solid_input('filter_load_file_sets', 'load_file_sets', load_file_sets) \
        .add_solid_input('filter_load_file_sets', 'max_file_sets_per_run', sj_config['max_file_sets_per_run']) \
        .add_solid_input('read_sj_csv_file_sets', 'regex_patterns', regex_patterns) \
        .add_solid_input('merge_promo_csv_file_sets', 'regex_patterns', regex_patterns) \
        .add_solid_input('merge_segs_csv_file_sets', 'regex_patterns', regex_patterns) \
        .add_solid('transform_sets_df') \
        .add_solid_input('upload_sales_table', 'table_name', sj_config['sales_data_table']) \
        .add_solid_input('upload_tracking_table', 'table_name', sj_config['tracking_data_table']) \
        .add_resource('postgres_warehouse', postgres_warehouse)

    return env_dict


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
def csv_currency_to_postgres_pipeline():
    """
    Definition of the pipeline to upload the sales journal data to Postgres
    """
    # load and process the postgres table information
    table_desc = transform_table_desc_df(
        load_csv()  # TODO should supply dtypes
    )
    table_desc_by_type = get_table_desc_by_type(table_desc)
    dtypes_by_root = generate_dtypes(table_desc, table_desc_by_type)

    table_type_limits = get_table_desc_type_limits(table_desc)

    # generate column string for creation and insert queries, for the sales_data and tracking_data tables
    create_data_columns, insert_data_columns = generate_table_fields_str(table_desc)
    create_tracking_columns, insert_tracking_columns = generate_tracking_table_fields_str()

    # ----- currency portion --------
    # generate column string for creation and insert queries, for the sales_data and tracking_data tables
    create_columns, insert_columns = generate_currency_table_fields_str()

    # generate IMF SDF per currencies info
    currency_df, currency_names = transform_sdr_per_currency(
        read_sdr_per_currency()  # read IMF SDF per various currencies
    )
    usd_srd_df, usd_name = transform_usd_sdr(
        read_sdr_per_usd()  # read IMF SDF per USD
    )
    # generate currency equivalents in USD
    currency_eq_usd_df, currency_names = transform_rates(usd_srd_df, currency_df, usd_name, currency_names,
                                                         read_sdr_valuation()  # read IMF SDR valuation
                                                         )
    currency_eq_usd_df = transform_ex_rates_per_usd(
        read_ex_rates_per_usd(),
        currency_eq_usd_df,
        read_currency_codes()  # read IS0 4217 currency codes
    )
    # ----- currency portion --------

    # get previously uploaded file sets info
    prev_uploaded, uploaded_ids = transform_loaded_records(
        query_table(),
        query_sales_data()
    )

    # load the csv files in to sets, so that the csv files that relate to a common export are all together and load them
    sets = create_csv_file_sets(
        load_list_of_csv_files(), prev_uploaded
    )

    # read the sales journal
    sets_list, sets_df = read_sj_csv_file_sets(
        filter_load_file_sets(sets), dtypes_by_root, prev_uploaded, uploaded_ids
    )

    # merge the promo info into the sales journal
    sets_list, sets_df = merge_promo_csv_file_sets(sets_list, sets_df, dtypes_by_root)

    # merge the segs info into the sales journal
    sets_list, sets_df = merge_segs_csv_file_sets(sets_list, sets_df, dtypes_by_root)

    sets_df = transform_sets_df(sets_df, table_desc, table_desc_by_type, table_type_limits)

    # ----- currency portion --------
    sets_df = currency_transform_sets_df(sets_df, table_desc, table_desc_by_type, table_type_limits,
                                         currency_eq_usd_df, dtypes_by_root)
    # ----- currency portion --------

    upload_results = upload_sales_table(sets_df, insert_data_columns)

    upload_tracking_table(upload_results, insert_tracking_columns)


def execute_csv_currency_to_postgres_pipeline(sj_config: dict, postgres_warehouse: dict):
    """
    Execute the pipeline to upload the sales journal data to Postgres
    :param sj_config: app configuration
    :param postgres_warehouse: postgres server resource
    """

    # environment dictionary
    regex_patterns = sj_config['regex_patterns']
    currency_cfg = sj_config['currency']
    load_file_sets = None
    if 'load_file_sets' in sj_config:
        load_file_sets = sj_config['load_file_sets']

    env_dict = cvs_pipeline_environmental_dict(EnvironmentDict(), sj_config, postgres_warehouse)
    env_dict = currency_pipeline_environmental_dict(env_dict, sj_config) \
        .add_solid_input('currency_transform_sets_df', 'regex_patterns', regex_patterns) \
        .add_solid_input('currency_transform_sets_df', 'currency_cfg', currency_cfg) \
        .build()

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(env_dict)

    run_mode = 'normal'
    if 'csv_pipeline_run_mode' in sj_config.keys():
        run_mode = sj_config['csv_pipeline_run_mode'].lower()
    if run_mode:
        loop = True
        while loop:
            result = execute_pipeline(csv_currency_to_postgres_pipeline, environment_dict=env_dict)
            assert result.success

            upload_results = result.result_for_solid('upload_sales_table').output_value()
            loop = len(upload_results.keys()) > 0
            result = None

    else:
        # normal mode
        result = execute_pipeline(csv_currency_to_postgres_pipeline, environment_dict=env_dict)
        assert result.success
