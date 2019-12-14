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
    execute_pipeline,
    pipeline,
    ModeDefinition,
    Dict,
)
from dagster_toolkit.postgres import (
    postgres_warehouse_resource,
)
from dagster_toolkit.environ import (
    EnvironmentDict,
)
from sales_journal.solids import (
    read_currency_codes,
    generate_currency_table_fields_str,
    read_sdr_per_currency,
    transform_sdr_per_currency,
    read_sdr_per_usd,
    transform_usd_sdr,
    read_sdr_valuation,
    read_ex_rates_per_usd,
    transform_rates,
    transform_ex_rates_per_usd,
    upload_currency_table,
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
def currency_to_postgres_pipeline():
    """
    Definition of the pipeline to upload currency information
    """
    # generate column string for creation and insert queries, for the sales_data and tracking_data tables
    create_columns, insert_columns = generate_currency_table_fields_str()

    # generate IMF SDF per currencies info
    currency_df, currency_names = transform_sdr_per_currency(
        read_sdr_per_currency()     # read IMF SDF per various currencies
    )
    usd_srd_df, usd_name = transform_usd_sdr(
        read_sdr_per_usd()          # read IMF SDF per USD
    )
    # generate currency equivalents in USD
    currency_eq_usd_df, currency_names = transform_rates(usd_srd_df, currency_df, usd_name, currency_names,
                                                         read_sdr_valuation()   # read IMF SDR valuation
                                                         )
    currency_eq_usd_df = transform_ex_rates_per_usd(
        read_ex_rates_per_usd(),
        currency_eq_usd_df,
        read_currency_codes()   # read IS0 4217 currency codes
    )

    # below this is the upload part

    upload_currency_table(
        currency_eq_usd_df,
        insert_columns
    )


def execute_currency_to_postgres_pipeline(sj_config: Dict, postgres_warehouse: Dict):
    """
    Execute the pipeline to create a plot
    :param sj_config:
    :param sj_config: app configuration
    :param postgres_warehouse: postgres server resource
    :param plot_cfg_path: path to plot configuration file
    :param plot_name: name of plot to produce
    """
    currency_config = sj_config['currency']

    # environment dictionary
    env_dict = currency_pipeline_environmental_dict(EnvironmentDict(), sj_config)\
        .add_solid_input('upload_currency_table', 'table_name', currency_config['currency_data_table']) \
        .add_resource('postgres_warehouse', postgres_warehouse) \
        .build()

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(env_dict)

    result = execute_pipeline(currency_to_postgres_pipeline, environment_dict=env_dict)
    assert result.success


def currency_pipeline_environmental_dict(env_dict: EnvironmentDict, sj_config: Dict) -> EnvironmentDict:
    currency_config = sj_config['currency']

    # environment dictionary
    env_dict.add_solid_input('read_currency_codes', 'cur_config', currency_config) \
        .add_solid_input('read_currency_codes', 'xml_path', currency_config['currency_codes']['cc_xml_path']) \
        .add_solid_input('generate_currency_table_fields_str',
                         'table_data_columns', currency_config['currency_data_columns']) \
        .add_composite_solid_input('read_sdr_per_currency', 'read_sdr_currencies', 'cur_config', currency_config) \
        .add_composite_solid_input('read_sdr_per_currency', 'read_sdr_currencies',
                                   'tsv_path', currency_config['tsv_path']) \
        .add_composite_solid_input('transform_sdr_per_currency', 'transform_sdr_currencies', 'cur_config',
                                   currency_config) \
        .add_composite_solid_input('read_sdr_per_usd', 'read_usd_sdr', 'cur_config', currency_config) \
        .add_composite_solid_input('read_sdr_per_usd', 'read_usd_sdr',
                                   'tsv_path', currency_config['sdr_per_usd_path']) \
        .add_composite_solid_input('transform_usd_sdr', 'transform_usd_sdr_currency', 'cur_config',
                                   currency_config) \
        .add_solid_input('read_sdr_valuation', 'cur_config', currency_config) \
        .add_solid_input('read_sdr_valuation', 'sdrv_path', currency_config['sdrv_path']) \
        .add_solid_input('read_ex_rates_per_usd', 'cur_config', currency_config) \
        .add_solid_input('read_ex_rates_per_usd', 'ex_per_usd_path', currency_config['ex_per_usd_path']) \
        .add_solid_input('transform_rates', 'cur_config', currency_config) \
        .add_solid_input('transform_ex_rates_per_usd', 'cur_config', currency_config) \

    return env_dict
