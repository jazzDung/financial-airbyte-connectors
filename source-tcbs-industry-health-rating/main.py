#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_tcbs_industry_health_rating import SourceTcbsIndustryHealthRating

if __name__ == "__main__":
    source = SourceTcbsIndustryHealthRating()
    launch(source, sys.argv[1:])
