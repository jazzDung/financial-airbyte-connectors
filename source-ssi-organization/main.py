#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_ssi_organization import SourceSsiOrganization

if __name__ == "__main__":
    source = SourceSsiOrganization()
    launch(source, sys.argv[1:])
