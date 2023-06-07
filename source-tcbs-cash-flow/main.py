#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_tcbs_cash_flow import SourceTcbsCashFlow

if __name__ == "__main__":
    source = SourceTcbsCashFlow()
    launch(source, sys.argv[1:])
