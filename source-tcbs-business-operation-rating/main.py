#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_tcbs_business_operation_rating import SourceTcbsBusinessOperationRating

if __name__ == "__main__":
    source = SourceTcbsBusinessOperationRating()
    launch(source, sys.argv[1:])
