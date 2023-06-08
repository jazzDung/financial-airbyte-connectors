#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import date, timedelta, strptime

import requests, time
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth

class Organization(HttpStream, IncrementalMixin):
    url_base = None
    cursor_field = 'tradingDate'
    primary_key = 'tradingDate'

    def use_cache(self) -> bool:
        return True

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()

        self.sync_all = config['sync_all']
        self.days_before = config['days_before']
        self._cursor_value = None   
        

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%YYYY-%MM-%DD')}
        else:
            return {self.cursor_field: self.start_date.strftime('%YYYY-%MM-%DD')}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
       self._cursor_value = strptime(value[self.cursor_field], '%Y-%m-%d')

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        # Get organization list through https://fiin-core.ssi.com.vn/Master/GetListOrganization?language=vi
        return "https://fiin-core.ssi.com.vn/Master/GetListOrganization?language=vi"  

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        headers = {
            'Connection': 'keep-alive',
            'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
            'DNT': '1',
            'sec-ch-ua-mobile': '?0',
            'X-Fiin-Key': 'KEY',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Fiin-User-ID': 'ID',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'X-Fiin-Seed': 'SEED',
            'sec-ch-ua-platform': 'Windows',
            'Origin': 'https://iboard.ssi.com.vn',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://iboard.ssi.com.vn/',
            'Accept-Language': 'en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7'
        }

        return headers
    
    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        
        response = response.json()
        response = response['items']

        return response[:10]

class OrganizationSubStream(HttpSubStream, Organization, ABC):
    raise_on_http_errors = False
 
    def __init__(self, config: Mapping[str, Any], parent: Organization, **kwargs):
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


class PriceHistory(OrganizationSubStream):
 
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
                yield {"ticker": record["ticker"]}

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_field: start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], '%Y-%m-%d') if stream_state and self.cursor_field in stream_state else self.start_date
        return self._chunk_date_range(start_date)
 
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
                latest_record_date = strptime(record[self.cursor_field], '%Y-%m-%d')
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record

# Source
class SourceTcbsPriceHistory(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth() 
        
        return [
            # Organization(config=config, authenticator=auth), 
            PriceHistory(parent=Organization(config=config, authenticator=auth), config=config, authenticator=auth),
        ]
