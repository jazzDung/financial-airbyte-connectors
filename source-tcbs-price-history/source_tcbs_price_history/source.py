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

    def __init__(self, config: Mapping[str, Any], cursor_start: datetime, **kwargs):
        super().__init__()

        self.fast_mode = config['fast_mode']
        self.url = config["symbol_url"]
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        else:
            return {self.cursor_field: self.cursor_start.strftime('%Y-%m-%d')}
    
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
     
    def __init__(self, config: Mapping[str, Any], parent: Symbol, cursor_start:datetime, **kwargs):
        super().__init__(config=config, parent=parent, cursor_start=cursor_start, **kwargs)
        
        self.sync_all_history = config['sync_all_history']
        self.start_date = int(time.mktime(datetime.strptime(config['start_date'], '%Y-%m-%d').timetuple()))
        self.end_date = int(time.mktime(datetime.strptime(config['end_date'], '%Y-%m-%d').timetuple()))
        self._cursor_value = cursor_start
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()


class PriceHistory(SymbolSubStream):
 
    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        if self.sync_all_history:
            # Sync data from day 0 (Mean, all of them)
            return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice["ticker"]}&type=stock&resolution=D&from={int(time.mktime(self._cursor_value.timetuple()))}&to={int(time.mktime(datetime.today().timetuple()))}'
        else:
            # Sync only the data of yesterday
            return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice["ticker"]}&type=stock&resolution=D&from={self.start_date}&to={self.end_date}'
 
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for stream_slices in self.parent.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slices):
                yield {"ticker": record, "single_cursor": self.sursor_start}
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response = response.json()
        data = response['data']
        ticker = response['ticker']
        
        for element in data:
            element['ticker'] = ticker
            yield element
    
    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):            
            if self._cursor_value:
                print("Child: ", self._cursor_value)
                latest_record_date = datetime.strptime(record[self.cursor_field][:10], '%Y-%m-%d')
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record

# Source
class SourceTcbsPriceHistory(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        if not config['sync_all_history'] and not config['start_date'] and not config['end_date']:
            return False, "You must either enable sync_all_history or specify start_date and end_date"
        elif not config['sync_all_history'] and not config['start_date']:
            return False, "Missing end_date"
        elif not config['sync_all_history'] and not config['end_date']:
            return False, "Missing start_date" 
        elif not config['sync_all_history'] and int(time.mktime(datetime.strptime(config['start_date'], '%Y-%m-%d').timetuple())) > int(time.mktime(datetime.strptime(config['end_date'], '%Y-%m-%d').timetuple())):
            return False, "start_date is after end_date" 
        elif not config['sync_all_history'] and config['start_date'] and config['end_date']:
            return True, "sync_all_history are enabled and will override date range ingest mode"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth() 

        cursor_start = datetime.strptime('2000-01-01', '%Y-%m-%d')

        return [
            PriceHistory(
                parent=Symbol(
                    config=config, 
                    authenticator=auth, 
                    cursor_start=cursor_start), 
                config=config, 
                authenticator=auth,
                cursor_start=cursor_start
            ),
        ]
