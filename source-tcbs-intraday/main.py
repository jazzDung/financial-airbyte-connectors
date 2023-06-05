#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_tcbs_intraday import SourceTcbsIntraday

if __name__ == "__main__":
    source = SourceTcbsIntraday()
    launch(source, sys.argv[1:])
