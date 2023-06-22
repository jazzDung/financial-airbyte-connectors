#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import date, timedelta, datetime

import requests, time
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth

class Symbol(HttpStream):
    url_base = None
    primary_key = 'tradingDate'

    def use_cache(self) -> bool:
        return True

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()

        self.sync_all = config['sync_all']
        self.days_before = config['days_before']
        self.url = config["symbol_url"]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
    
    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return self.url

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        
        response = response.text.split(",")
        
        if self.sync_all:
            return response
        else:
            return response[:10]

class SymbolSubStream(HttpSubStream, Symbol, ABC):
    raise_on_http_errors = False
 
    def __init__(self, config: Mapping[str, Any], parent: Symbol, **kwargs):
        super().__init__(config=config, parent=parent, **kwargs)
        
        self.sync_all = config['sync_all']
        self.days_before = config['days_before']

        # Specify some of the timestamp here
        today = date.today()
        start_date = today - timedelta(self.days_before)

        self.today = int(time.mktime(today.timetuple()))
        self.start_date = int(time.mktime(start_date.timetuple()))
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()


class PriceHistory(SymbolSubStream):
 
    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
             next_page_token: Mapping[str, Any] = None) -> str:
        if self.sync_all:
            # Sync data from day 0 (Mean, all of them)
            return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice["ticker"]}&type=stock&resolution=D&from=0&to={self.today}'
        else:
            # Sync only the data of yesterday
            return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice["ticker"]}&type=stock&resolution=D&from={self.start_date}&to={self.today}'
 
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for stream_slices in self.parent.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slices):
                # for data in record["items"]:
                yield {"ticker": record}
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        response = response.json()
        data = response['data']
        ticker = response['ticker']
        for element in data:
            element['ticker'] = ticker
            yield element

# Source
class SourceTcbsPriceHistory(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth() 
        
        return [
            PriceHistory(parent=Symbol(config=config, authenticator=auth), config=config, authenticator=auth),
        ]
