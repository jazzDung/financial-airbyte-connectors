#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import date, timedelta

import requests, time
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth

class Organization(HttpStream):
    url_base = None

    # Set this as a noop.
    primary_key = 'ticker'

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()

        try:
            self.sync_all = config['sync_all']
        except:
            self.sync_all = False
        self.days_before = config['days_before']
        
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
        
        try:
            self.sync_all = config['sync_all']
        except:
            self.sync_all = False

        self.days_before = config['days_before']

        # Specify some of the timestamp here
        today = date.today()
        start_date = today - timedelta(self.days_before)

        self.today = int(time.mktime(today.timetuple()))
        self.start_date = int(time.mktime(start_date.timetuple()))
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()

class PriceHistory(OrganizationSubStream):
    # primary_key = None
    primary_key = ['ticker','tradingDate']
 
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
            # Organization(config=config, authenticator=auth), 
            PriceHistory(parent=Organization(config=config, authenticator=auth), config=config, authenticator=auth),
        ]
