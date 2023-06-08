#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.models import SyncMode

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth

HEADERS = {
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

# Basic full refresh stream
class Organization(HttpStream, ABC):
    url_base = None
    primary_key = ["ticker", "quarter", "year"]

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.fast_mode = config['fast_mode']
        self.all_data = config['all_data']
        self.frequency = config['frequency']

    def path(self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "https://fiin-core.ssi.com.vn/Master/GetListOrganization?language=vi"   

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        
        response = response.json()
        response = response['items']

        if self.fast_mode:
            return response[:10]
        else:
            return response

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return HEADERS


class OrganizationSubStream(HttpSubStream, Organization, ABC):

    raise_on_http_errors = False
 
    def __init__(self, config: Mapping[str, Any], parent: Organization, **kwargs):
        super().__init__(config=config, parent=parent, **kwargs)
        self.frequency = config['frequency']
        self.all_data = config['all_data']
 
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()

class IncomeStatement(OrganizationSubStream):

    primary_key = ["ticker", "quarter", "year"]
    
    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{stream_slice["ticker"]}/incomestatement?yearly=0&isAll={self.all_data}'
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for stream_slices in self.parent.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slices):
                yield {"ticker": record["ticker"]}
    
    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:

        url = f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{stream_slice["ticker"]}/incomestatement'

        if self.frequency == "Yearly":
            response = requests.get(url, params={'yearly': 1, 'isAll': self.all_data}).json()

        if self.frequency == "Quarterly" or self.frequency == "Quarterly":
            response = requests.get(url, params={'yearly': 0, 'isAll': self.all_data}).json()

        if self.frequency == "Both":
            yearly_data = requests.get(url, params={'yearly': 0, 'isAll': self.all_data}).json() 
            quarterly_data = requests.get(url, params={'yearly': 0, 'isAll': self.all_data}).json()
            response = yearly_data + quarterly_data

        for element in response:
            yield element

# Source
class SourceTcbsIncomeStatement(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:

        if config["frequency"] not in ["Yearly", "Quarterly", "Both"]:
            return False, f'Frequency must be one of these values: Yearly, Quarterly, Both. Got {config["frequency"]} instead'
        
        else:
            try:
                url = f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/TCB/incomestatement'
                requests.get(url, params={'yearly': 1, 'isAll': config["all_data"]})
                return True, None
        
            except requests.exceptions.RequestException as e:
                return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth() 
        return [
            IncomeStatement(
                parent=Organization(config=config, authenticator=auth), 
                config=config, 
                authenticator=auth
            )
        ]