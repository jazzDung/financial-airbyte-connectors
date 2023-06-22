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

class Symbol(HttpStream, IncrementalMixin):
    url_base = None
    primary_key = 'tradingDate'
    cursor_field = "tradingDate"

    def use_cache(self) -> bool:
        return True

    def __init__(self, config: Mapping[str, Any], start_date: datetime, **kwargs):
        super().__init__()

        self.fast_mode = config['fast_mode']
        self.url = config["symbol_url"]
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        else:
            return {self.cursor_field: self.start_date.strftime('%Y-%m-%d')}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
       self._cursor_value = datetime.strptime(value[self.cursor_field][:10], '%Y-%m-%d')

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
        
        if self.fast_mode:
            return response[:10]
        else:
            return response


class SymbolSubStream(HttpSubStream, Symbol, ABC):
    raise_on_http_errors = False
    primary_key = 'tradingDate'
    cursor_field = "tradingDate"
 
    def __init__(self, config: Mapping[str, Any], parent: Symbol, **kwargs):
        super().__init__(config=config, parent=parent, **kwargs)
        
        self.sync_all_history = config['sync_all_history']
        self.days_before = config['days_before']
        self.start_date = self.parent.start_date
        self._cursor_value = datetime.strptime('2000-01-01', '%Y-%m-%d')

        # Specify some of the timestamp here
        today = date.today()
        start_date = today - timedelta(self.days_before)

        self.today = int(time.mktime(today.timetuple()))
        self.start_date = int(time.mktime(start_date.timetuple()))
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()


class PriceHistory(SymbolSubStream):
 
    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        if self.sync_all_history:
            # Sync data from day 0 (Mean, all of them)
            return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice["ticker"]}&type=stock&resolution=D&from={int(time.mktime(self._cursor_value.timetuple()))}&to={self.today}'
        else:
            # Sync only the data of yesterday
            return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice["ticker"]}&type=stock&resolution=D&from={self.start_date}&to={self.today}'
 
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for stream_slices in self.parent.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slices):
                yield {"ticker": record, "resume_date": self._cursor_value}
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response = response.json()
        data = response['data']
        ticker = response['ticker']
        for element in data:
            element['ticker'] = ticker
            yield element
    
    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self.parent._cursor_value:
                print("Parent: ", self.parent._cursor_value)
                latest_record_date = datetime.strptime(record[self.cursor_field][:10], '%Y-%m-%d')
                self._cursor_value = max(self._cursor_value, latest_record_date)
            
            if self._cursor_value:
                print("Child: ", self._cursor_value)
                latest_record_date = datetime.strptime(record[self.cursor_field][:10], '%Y-%m-%d')
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record

# Source
class SourceTcbsPriceHistory(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:

        if 
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth() 

        if config['sync_all_history']:
            start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
        else:
            today = datetime.today()
            start_date = today - timedelta(config['days_before'])

        return [
            PriceHistory(
                parent=Symbol(
                    config=config, 
                    authenticator=auth, 
                    start_date=start_date), 
                config=config, 
                authenticator=auth,
                start_date=start_date
            ),
        ]
