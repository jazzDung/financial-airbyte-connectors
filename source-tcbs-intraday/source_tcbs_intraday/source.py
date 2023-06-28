#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import requests, time
from abc import ABC
from typing import Any, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, timedelta
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

class Symbol(HttpStream, IncrementalMixin):
    url_base = None
    primary_key = cursor_field = "tradingDate"

    def str_to_date(self, string):  
        "'2000-01-01' -> datetime.date(2000, 1, 1)"
        return datetime.strptime(string, '%Y-%m-%d').date()
    
    @property  
    def use_cache(self) -> bool:  
        "Cache symbol list to disk to prevent calling the URL everytime we get price history"
        return True

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()

        self.fast_mode = config["Fast mode"]
        self.url = config["Symbol URL"]
        self.day_offset = config["Day offset"]

        """
        Fill the _cursor_value with ticker symbol from url
        Data type: {"TCB": datetime.date(2000, 1, 1), "ABC": datetime.date(2000, 1, 1)}
        Print format: {"TCB":"2023-06-23", "ABC":"2023-06-23"}
        """

        response = requests.get(config["Symbol URL"]).text.split(",")
        response = response[:5] if self.fast_mode else response
        self._cursor_value = dict.fromkeys(response, self.str_to_date("2000-01-01"))

    @property
    def state(self) -> Mapping[str, Any]:
        "Return the _cursor_value to show on UI at Connection > Settings  > Advanced"
        return self._cursor_value
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        "Update _cursor_value with latest timestamp in ingested record"
        for key in self._cursor_value:    
            self._cursor_value[key] = self.str_to_date(value[key][:10])
    
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

class IncrementalTcbsIntradayStream(HttpSubStream):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


# Source
class SourceTcbsIntraday(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = TokenAuthenticator(token="api_key")  # Oauth2Authenticator is also available if you need oauth support
        return [Customers(authenticator=auth), Employees(authenticator=auth)]
