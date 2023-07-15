#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from datetime import datetime
from typing import Any, Iterable, List, Mapping, Optional, Tuple
from airbyte_cdk.models import SyncMode

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

class Symbol(HttpStream):
    url_base = None
    primary_key = None
    
    @property  
    def use_cache(self) -> bool:  
        "Cache symbol list to disk to prevent calling the URL everytime we get price history"
        return True

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()

        self.fast_mode = config["Fast mode"]
        self.url = config["Symbol URL"]
    
    def next_page_token(self, response: requests.Response):
        "The API does not offer pagination, so we return None to indicate there are no more pages in the response"
        return None

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        """
        Complete the URL to ingest data. Airbyte see concat url_base + path() as URL
        Since the base URL stored in config (Can not be assigned to url_base before init), we will store the full URL in path()
        """
        return self.url

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:    
        """
        The symbol file content will look like this "VVS,XDC,HSV,CST,BVL,SGI,TOS,VTZ,SSH,BCA,GMH,BIG"
        So this function collect the text and transform to a list of symbol
        """
        response = response.text.split(",")
        return response[:5] if self.fast_mode else response

class SymbolSubStream(HttpSubStream, Symbol, ABC):
    raise_on_http_errors = False 
    def __init__(self, config: Mapping[str, Any], parent: Symbol, **kwargs):
        super().__init__(config=config, parent=parent, **kwargs)

class ValuationRating(SymbolSubStream):
    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        "URL example: 'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/TCB/valuation?fType=TICKER'"
        return f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{stream_slice}/valuation?fType=TICKER'
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        "Get the symbol list" 
        for record in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            yield record
    
    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        "Parse json records from URL"
        response = response.json()
        yield response

# Source
class SourceTcbsValuationRating(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        "Test the connector, check for invalid arguments"
        try:
            requests.get(config["Symbol URL"]).text.split(",")
        except:
            return False, "Invalid URL or invalid file content format"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth() 
        return [
            ValuationRating(
                parent=Symbol(config=config, authenticator=auth), 
                config=config, 
                authenticator=auth
            )
        ]