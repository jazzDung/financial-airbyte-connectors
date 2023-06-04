from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

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
        self.start_date = int(time.mktime(time.strptime(config['start_date'], "%Y-%m-%d"))) 
        self.end_date = int(time.mktime(time.strptime(config['end_date'], "%Y-%m-%d"))) 
        
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, so we return None to indicate there are no more pages in the response
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
        self.start_date = int(time.mktime(time.strptime(config['start_date'], "%Y-%m-%d"))) 
        self.end_date = int(time.mktime(time.strptime(config['end_date'], "%Y-%m-%d"))) 
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()
    
class OrganizationOverview(OrganizationSubStream):
    primary_key = 'ticker'

    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
             next_page_token: Mapping[str, Any] = None) -> str:
        return f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/ticker/{stream_slice["ticker"]}/overview'
 
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for stream_slices in self.parent.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slices):
                yield {"ticker": record["ticker"]}
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()

class PriceHistory(OrganizationSubStream):
    primary_key = None
    # primary_key = ['ticker','tradingDate']
 
    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
             next_page_token: Mapping[str, Any] = None) -> str:
        return f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={stream_slice["ticker"]}&type=stock&resolution=D&from={self.start_date}&to={self.end_date}'
 
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
class SourceTcbs(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth() 
        return [
            Organization(config=config, authenticator=auth), 
            PriceHistory(parent=Organization(config=config, authenticator=auth), config=config, authenticator=auth),
            OrganizationOverview(parent=Organization(config=config, authenticator=auth), config=config, authenticator=auth)
        ]