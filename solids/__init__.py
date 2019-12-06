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

from .create_table import (
    create_tables,
)
from .drop_table import (
    drop_tables,
)
from .sales_table import (
    generate_table_fields_str,
    upload_sales_table,
)
from .process_node import (
    get_table_desc_by_type,
    transform_sets_df,
    transform_table_desc_df,
)
from .read_cvs_node import (
    load_list_of_csv_files,
    create_csv_file_sets,
    generate_dtypes,
    filter_load_file_sets,
    read_sj_csv_file_sets,
    merge_promo_csv_file_sets,
    merge_segs_csv_file_sets,
)
from .tracking_table import (
    generate_tracking_table_fields_str,
    upload_tracking_table,
    transform_loaded_records,
)


# if somebody does "from sales_journal.solids import *", this is what they will
# be able to access:
__all__ = [
    'create_tables',

    'drop_tables',

    'generate_table_fields_str',
    'upload_sales_table',

    'get_table_desc_by_type',
    'transform_sets_df',
    'transform_table_desc_df',

    'load_list_of_csv_files',
    'create_csv_file_sets',
    'generate_dtypes',
    'filter_load_file_sets',
    'read_sj_csv_file_sets',
    'merge_promo_csv_file_sets',
    'merge_segs_csv_file_sets',

    'generate_tracking_table_fields_str',
    'upload_tracking_table',
    'transform_loaded_records',
]
