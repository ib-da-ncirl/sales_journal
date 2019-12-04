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
from dagster_toolkit.environ import (
    EnvironmentDict,
)
from solids import (
    drop_tables
)


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
def clean_sales_data_postgres_pipeline():
    """
    Definition of the pipeline to clear sales journal data from Postgres
    """
    drop_tables()


def execute_clean_sales_data_postgres_pipeline(sj_config: dict, postgres_warehouse: dict):
    """
    Execute the pipeline to clear sales journal data from Postgres
    :param sj_config: app configuration
    :param postgres_warehouse: postgres server resource
    """

    # .add_solid_input('does_psql_table_exist', 'name', sj_config['tracking_table_query']) \

    # environment dictionary
    env_dict = EnvironmentDict() \
        .add_composite_solid_input('drop_tables', 'drop_sales_table', 'table_name',
                                   sj_config['sales_data_table']) \
        .add_composite_solid_input('drop_tables', 'drop_tracking_table', 'table_name',
                                   sj_config['tracking_data_table']) \
        .add_resource('postgres_warehouse', postgres_warehouse) \
        .build()

    print(env_dict)

    result = execute_pipeline(clean_sales_data_postgres_pipeline, environment_dict=env_dict)
    assert result.success

