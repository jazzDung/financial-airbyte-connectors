#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_tcbs_price_history import SourceTcbsPriceHistory

if __name__ == "__main__":
    source = SourceTcbsPriceHistory()
    launch(source, sys.argv[1:])
