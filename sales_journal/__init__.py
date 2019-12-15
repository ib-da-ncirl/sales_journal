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

from .sales_journal import (
    get_app_config,
    make_call_execute_csv_to_postgres_pipeline,
    make_call_execute_csv_currency_to_postgres_pipeline,
    make_call_execute_interactive_plot_pipeline,
    make_call_execute_currency_to_postgres_pipeline,
    make_call_execute_create_sales_data_postgres_pipeline,
    make_call_execute_create_currency_data_postgres_pipeline,
    make_call_execute_clean_sales_data_postgres_pipeline,
    make_call_execute_clean_currency_data_postgres_pipeline,
)
from .solids.__init__ import __all__ as solids_all
from .plot.__init__ import __all__ as plot_all
from .pipelines.__init__ import __all__ as pipelines_all
from .misc_sj.__init__ import __all__ as misc_sj_all

# if somebody does "from sales_journal import *", this is what they will
# be able to access:
__all__ = ['get_app_config',
           'make_call_execute_csv_to_postgres_pipeline',
           'make_call_execute_csv_currency_to_postgres_pipeline',
           'make_call_execute_interactive_plot_pipeline',
           'make_call_execute_currency_to_postgres_pipeline',
           'make_call_execute_create_sales_data_postgres_pipeline',
           'make_call_execute_create_currency_data_postgres_pipeline',
           'make_call_execute_clean_sales_data_postgres_pipeline',
           'make_call_execute_clean_currency_data_postgres_pipeline'
           ] + solids_all + plot_all + pipelines_all + misc_sj_all
