#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_tcbs_balance_sheet import SourceTcbsBalanceSheet

if __name__ == "__main__":
    source = SourceTcbsBalanceSheet()
    launch(source, sys.argv[1:])
