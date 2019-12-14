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

from .csv_pipelines import (
    execute_csv_to_postgres_pipeline,
    execute_csv_currency_to_postgres_pipeline
)
from .plot_pipelines import (
    execute_postgres_to_plot_pipeline,
    execute_file_ip_postgres_to_plot_pipeline,
    execute_file_ip_sql_to_plot_pipeline,
)
from .create_pipelines import (
    execute_create_sales_data_postgres_pipeline,
    execute_create_currency_data_postgres_pipeline,
)
from .clean_pipelines import (
    execute_clean_sales_data_postgres_pipeline,
    execute_clean_currency_data_postgres_pipeline
)
from .currency_pipelines import execute_currency_to_postgres_pipeline

# if somebody does "from sales_journal.pipelines import *", this is what they will
# be able to access:
__all__ = [
    'execute_csv_to_postgres_pipeline',
    'execute_csv_currency_to_postgres_pipeline',

    'execute_postgres_to_plot_pipeline',
    'execute_file_ip_postgres_to_plot_pipeline',
    'execute_file_ip_sql_to_plot_pipeline',

    'execute_create_sales_data_postgres_pipeline',
    'execute_create_currency_data_postgres_pipeline',

    'execute_clean_sales_data_postgres_pipeline',
    'execute_clean_currency_data_postgres_pipeline',

    'execute_currency_to_postgres_pipeline',
]
