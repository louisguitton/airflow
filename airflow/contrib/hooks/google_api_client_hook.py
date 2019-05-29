# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from airflow.hooks.base_hook import BaseHook


class GoogleApiClientHook(BaseHook):
    """
    A hook to use the Google API Client library.

    :param google_api_client_conn_id: Holds the connection information needed for
        an google api client to be authenticated.
    :type google_api_client_conn_id: str
    :param api_service_name: The name of the api service that is needed to get the data
        for example 'youtube'.
    :type api_service_name: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    """

    def __init__(self, google_api_client_conn_id, api_service_name, api_version):
        super(GoogleApiClientHook, self).__init__(google_api_client_conn_id)
        self.google_api_client_conn_id = google_api_client_conn_id
        self.api_service_name = api_service_name
        self.api_version = api_version

    def get_conn(self):
        """
        Creates an authenticated api client for the given api service name and credentials.

        :return: the authenticated api service.
        :rtype: Resource
        """
        self.log.info("Authenticating Google API Client")

        credentials = self._get_credentials_from_connection()

        api_service = build(
            serviceName=self.api_service_name,
            version=self.api_version,
            credentials=credentials,
            cache_discovery=False
        )

        return api_service

    def query(self, endpoint, data, paginate=False):
        """
        Creates a dynamic API call to any Google API registered in Google's API Client Library
        and queries it.

        :param endpoint: The client libraries path to the api call's executing method.
            For example: 'analyticsreporting.reports.batchGet'

            .. seealso:: https://developers.google.com/apis-explorer
                for more information on what methods are available.
        :type endpoint: str
        :param data: The data (endpoint params) needed for the specific request to given endpoint.
        :type data: dict
        :param paginate: If set to True, it will collect all pa
        :type paginate: bool
        :return: the API response from the passed endpoint.
        :rtype: dict
        """
        google_api_conn_client = self.get_conn()

        api_response = self._call_api_request(google_api_conn_client, endpoint, data, paginate)
        return api_response

    def _get_credentials_from_connection(self):
        conn = self.get_connection(self.google_api_client_conn_id)

        info = {
            'refresh_token': conn.schema,
            'client_id': conn.login,
            'client_secret': conn.password
        }

        scopes = conn.host.split(',')

        return Credentials.from_authorized_user_info(info=info, scopes=scopes)

    def _call_api_request(self, google_api_conn_client, endpoint, data, paginate):
        api_endpoint_parts = endpoint.split('.')

        google_api_endpoint_instance = self._build_api_request(
            google_api_conn_client,
            api_sub_functions=api_endpoint_parts[1:],
            api_endpoint_params=data
        )

        if paginate:
            return self._paginate_api(
                google_api_endpoint_instance,
                google_api_conn_client,
                api_endpoint_parts
            )

        return google_api_endpoint_instance.execute()

    def _build_api_request(self, google_api_conn_client, api_sub_functions, api_endpoint_params):
        for sub_function in api_sub_functions:
            google_api_conn_client = getattr(google_api_conn_client, sub_function)
            if sub_function != api_sub_functions[-1]:
                google_api_conn_client = google_api_conn_client()
            else:
                google_api_conn_client = google_api_conn_client(**api_endpoint_params)

        return google_api_conn_client

    def _paginate_api(self, google_api_endpoint_instance, google_api_conn_client, api_endpoint_parts):
        api_responses = []

        while google_api_endpoint_instance:
            api_response = google_api_endpoint_instance.execute()
            api_responses.append(api_response)

            google_api_endpoint_instance = self._build_next_api_request(
                google_api_conn_client,
                api_endpoint_parts[1:],
                google_api_endpoint_instance,
                api_response
            )

        return api_responses

    def _build_next_api_request(self,
                                google_api_conn_client,
                                api_sub_functions,
                                api_endpoint_instance,
                                api_response):
        for sub_function in api_sub_functions:
            if sub_function != api_sub_functions[-1]:
                google_api_conn_client = getattr(google_api_conn_client, sub_function)
                google_api_conn_client = google_api_conn_client()
            else:
                google_api_conn_client = getattr(google_api_conn_client, sub_function + '_next')
                google_api_conn_client = google_api_conn_client(api_endpoint_instance, api_response)

        return google_api_conn_client
