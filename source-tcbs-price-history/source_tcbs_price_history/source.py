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
    primary_key = "tradingDate"
    cursor_field = "tradingDate"

    def use_cache(self) -> bool:
        return True

    def __init__(self, config: Mapping[str, Any], cursor_start: datetime, **kwargs):
        super().__init__()

        self.fast_mode = config['Fast mode']
        self.url = config["Symbol URL"]
        self._cursor_value = {}

        """
        Fill the _cursor_value with ticker symbol from url
        Format will be like this : { "TCB":946659600, "ABC":946659600}
        """
        
        response = requests.get(config["Symbol URL"]).text.split(",")

        if self.fast_mode:
            for symbol in response[:5]:
                self._cursor_value[symbol] = cursor_start
        else:
            for symbol in response:
                self._cursor_value[symbol] = cursor_start    


    @property
    def state(self) -> Mapping[str, Any]:
        return self._cursor_value
        
    @state.setter
    def state(self, value: Mapping[str, Any]):
        for key in self._cursor_value:    
            self._cursor_value[key] = datetime.strptime(value[key][:10], '%Y-%m-%d').date()

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
        return response[:5] if self.fast_mode else response


class SymbolSubStream(HttpSubStream, Symbol, ABC):
    raise_on_http_errors = False
    primary_key = 'tradingDate'
    cursor_field = "tradingDate"
     
    def __init__(self, config: Mapping[str, Any], parent: Symbol, cursor_start: datetime, **kwargs):
        super().__init__(config=config, parent=parent, cursor_start=cursor_start, **kwargs)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()


class PriceHistory(SymbolSubStream):
 
    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice}&type=stock&resolution=D&from={int(time.mktime(self._cursor_value[stream_slice].timetuple()))}&to={int(time.mktime(datetime.today().date().timetuple()))}'
 
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for stream_slices in self.parent.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slices):
                yield record
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response = response.json()
        data = response["data"]
        ticker = response["ticker"]
        
        for element in data:
            element["ticker"] = ticker
            yield element
    
    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):            
            latest_record_date = datetime.strptime(record[self.cursor_field][:10], '%Y-%m-%d').date()

            if self._cursor_value[record["ticker"]] < latest_record_date:
                self._cursor_value[record["ticker"]] = latest_record_date
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

        cursor_start = datetime.strptime('2000-01-01', '%Y-%m-%d').date()

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
