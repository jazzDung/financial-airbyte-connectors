#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
import datetime
from typing import Any, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, timedelta

import requests, time
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

class Symbol(HttpStream, IncrementalMixin):
    url_base = None
    primary_key = cursor_field = "tradingDate"

    def str_to_date(self, string):
        return datetime.strptime(string, '%Y-%m-%d').date()
    
    def use_cache(self) -> bool:
        return True

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()

        self.fast_mode = config["Fast mode"]
        self.url = config["Symbol URL"]
        self.day_offset = config["Day offset"]

        # Fill the _cursor_value with ticker symbol from url
        # Data type: {"TCB": datetime.date(2000, 1, 1), "ABC": datetime.date(2000, 1, 1)}
        # Print format: {"TCB":"2023-06-23", "ABC":"2023-06-23"}
        
        response = requests.get(config["Symbol URL"]).text.split(",")
        response = response[:5] if self.fast_mode else response
        self._cursor_value = dict.fromkeys(response, self.str_to_date("2000-01-01"))

    @property
    def state(self) -> Mapping[str, Any]:
        return self._cursor_value
        
    @state.setter
    def state(self, value: Mapping[str, Any]):
        for key in self._cursor_value:    
            self._cursor_value[key] = self.str_to_date(value[key][:10])

    def next_page_token(self, response: requests.Response):
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
        return response[:5] if self.fast_mode else response

class SymbolSubStream(HttpSubStream, Symbol, ABC):
    raise_on_http_errors = False 

    def __init__(self, config: Mapping[str, Any], parent: Symbol, **kwargs):
        super().__init__(config=config, parent=parent, **kwargs)

class PriceHistory(SymbolSubStream):
 
    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        start_timestamp = int(time.mktime(self._cursor_value[stream_slice].timetuple()))
        end_datetime = datetime.today() - timedelta(self.day_offset) 
        end_timestamp = int(time.mktime(end_datetime.date().timetuple()))
        return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice}&type=stock&resolution=D&from={start_timestamp}&to={end_timestamp}'
 
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for record in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            yield record
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response = response.json()
        for record in response["data"]:
            record["ticker"] = response["ticker"]
            yield record
    
    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):            
            latest_record_date = self.str_to_date(record['tradingDate'][:10])
            if self._cursor_value[record["ticker"]] < latest_record_date:
                self._cursor_value[record["ticker"]] = latest_record_date
                yield record

class SourceTcbsPriceHistory(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            requests.get(config["Symbol URL"]).text.split(",")
        except:
            return False, "Invalid URL or invalid file content format"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth() 
        return [
            PriceHistory(
                parent=Symbol(config=config, authenticator=auth), 
                config=config, 
                authenticator=auth
            ),
        ]
    