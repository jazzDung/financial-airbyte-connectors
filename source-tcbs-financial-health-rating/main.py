#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_tcbs_financial_health_rating import SourceTcbsFinancialHealthRating

if __name__ == "__main__":
    source = SourceTcbsFinancialHealthRating()
    launch(source, sys.argv[1:])
