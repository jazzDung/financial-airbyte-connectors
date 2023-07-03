#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import requests, time
from abc import ABC
from typing import Any, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, date
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

class Symbol(HttpStream, IncrementalMixin):
    url_base = None
    primary_key = cursor_field = "id"

    def reset_cursor_value(self):

        """
        Fill the _cursor_value with ticker symbol, their corresponding date and total records 
        Data type: 
        {
            'TCB': 
            {
                'date': datetime.date(2000, 1, 1), 
                'records: 0
            }
        }
        """
        
        response = requests.get(self.url).text.split(",")
        response = response[:5] if self.fast_mode else response
        _cursor_value =  dict.fromkeys(response, -1)
        _cursor_date = {"date": date.today()}
        return _cursor_date | _cursor_value
    
    def get_page_list(self, symbol):
        url = f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{symbol}/his/paging?page=0&size=1'
        page_amount = requests.get(url).json()["total"]
        page_num = page_amount//self.page_size
        return [i for i in range (page_num, -1, -1)]        
        
    @property  
    def use_cache(self) -> bool:  
        "Cache symbol list to disk to prevent calling the URL everytime we get price history"
        return True

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()

        self.fast_mode = config["Fast mode"]
        self.url = config["Symbol URL"]
        self.page_size = config["Page size"]
        self._cursor_value = self.reset_cursor_value()

    @property
    def state(self) -> Mapping[str, Any]:
        "Return the _cursor_value to show on UI at Connection > Settings  > Advanced"
        return self._cursor_value
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        "Update _cursor_value with latest timestamp in ingested record"
        self._cursor_value = value
        self._cursor_value["date"] = datetime.strptime(value["date"], '%Y-%m-%d').date()

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


class StockIntraday(SymbolSubStream):
    state_checkpoint_interval = None

    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        "URL example: 'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker=TCB&type=stock&resolution=D&from=1687798800&to=1687798800'"
        if datetime.now().weekday() > 4: #today is weekend
            return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{stream_slice["symbol"]}/his/paging?page={stream_slice["page"]}&size={self.page_size}&headIndex=-1'
        else:
            return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{stream_slice["symbol"]}/his/paging?page={stream_slice["page"]}&size={self.page_size}'

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        "Get the symbol list" 
        for record in self.parent.read_records(sync_mode=SyncMode.full_refresh):
            for page_num in self.get_page_list(record):
                yield {"symbol": record, "page": page_num}
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response = response.json()
        page = response["page"]
        total = response["total"]
        size = self.page_size
        ticker = response["ticker"]

        if page == total//size:
            base_index = 0
        else:
            base_index = total - size * (page + 1)

        if base_index > self._cursor_value[ticker]:
            response = response["data"]
            response.reverse()
            for record in response:
                record["ticker"] = ticker
                record["id"] = base_index + response.index(record)
                yield record

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        "Update symbol's cursor value with highest timestamp in corresponding symbol's record"

        if self._cursor_value["date"] < date.today():
            self._cursor_value["date"] = date.today()
            self._cursor_value = self.reset_cursor_value()

        for record in super().read_records(*args, **kwargs):            
            if self._cursor_value[record["ticker"]] < record["id"]:
                self._cursor_value[record["ticker"]] = record["id"]
                yield record
        
# Source
class SourceTcbsIntraday(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        "Test the connector, check for invalid arguments"
        try:
            requests.get(config["Symbol URL"]).text.split(",")
        except:
            return False, "Invalid URL or invalid file content format"
        
        if config["Page size"] > 100:
            return False, "Page size must be smaller or equal to 100"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth()
        return [
            StockIntraday(
                parent=Symbol(config=config, authenticator=auth), 
                config=config, 
                authenticator=auth
            ),
        ]
