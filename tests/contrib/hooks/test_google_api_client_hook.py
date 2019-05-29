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
import unittest
from unittest.mock import patch, call

from airflow import configuration, models
from airflow.contrib.hooks.google_api_client_hook import GoogleApiClientHook
from airflow.hooks.base_hook import BaseHook
from airflow.utils import db


class TestGoogleApiClientHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

        db.merge_conn(
            models.Connection(
                conn_id='google_test',
                host='google',
                schema='refresh_token',
                login='client_id',
                password='client_secret'
            )
        )

    @patch('airflow.contrib.hooks.google_api_client_hook.build')
    @patch('airflow.contrib.hooks.google_api_client_hook.Credentials.from_authorized_user_info')
    def test_get_conn(self, mock_google_service_credentials, mock_google_service_build):
        google_api_client_hook = GoogleApiClientHook(
            google_api_client_conn_id='google_test',
            api_service_name='youtube',
            api_version='v2'
        )
        google_api_client_hook_connection = BaseHook.get_connection(
            google_api_client_hook.google_api_client_conn_id
        )

        google_api_client_hook.get_conn()

        mock_google_service_credentials.assert_called_once_with(
            info={
                'refresh_token': google_api_client_hook_connection.schema,
                'client_id': google_api_client_hook_connection.login,
                'client_secret': google_api_client_hook_connection.password
            },
            scopes=google_api_client_hook_connection.host.split(',')
        )
        mock_google_service_build.assert_called_once_with(
            serviceName=google_api_client_hook.api_service_name,
            version=google_api_client_hook.api_version,
            credentials=mock_google_service_credentials(),
            cache_discovery=False
        )

    @patch('airflow.contrib.hooks.google_api_client_hook.getattr')
    @patch('airflow.contrib.hooks.google_api_client_hook.GoogleApiClientHook.get_conn')
    def test_query(self, mock_get_conn, mock_getattr):
        google_api_client_hook = GoogleApiClientHook(
            google_api_client_conn_id='google_test',
            api_service_name='analytics',
            api_version='v4'
        )
        endpoint = 'analyticsreporting.reports.batchGet'
        data = {
            'body': {
                'reportRequests': [{
                    'viewId': '180628393',
                    'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
                    'metrics': [{'expression': 'ga:sessions'}],
                    'dimensions': [{'name': 'ga:country'}]
                }]
            }
        }

        google_api_client_hook.query(endpoint, data)

        google_api_endpoint_name_parts = endpoint.split('.')
        mock_getattr.assert_has_calls([
            call(mock_get_conn.return_value, google_api_endpoint_name_parts[1]),
            call()(),
            call(mock_getattr.return_value.return_value, google_api_endpoint_name_parts[2]),
            call()(**data),
            call()().execute()
        ])

    @patch('airflow.contrib.hooks.google_api_client_hook.getattr')
    @patch('airflow.contrib.hooks.google_api_client_hook.GoogleApiClientHook.get_conn')
    def test_query_with_pagination(self, mock_get_conn, mock_getattr):
        google_api_conn_client_sub_call = mock_getattr.return_value.return_value
        mock_getattr.return_value.side_effect = [
            google_api_conn_client_sub_call,
            google_api_conn_client_sub_call,
            google_api_conn_client_sub_call,
            google_api_conn_client_sub_call,
            google_api_conn_client_sub_call,
            None
        ]
        google_api_client_hook = GoogleApiClientHook(
            google_api_client_conn_id='google_test',
            api_service_name='analytics',
            api_version='v4'
        )
        endpoint = 'analyticsreporting.reports.batchGet'
        data = {
            'body': {
                'reportRequests': [{
                    'viewId': '180628393',
                    'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
                    'metrics': [{'expression': 'ga:sessions'}],
                    'dimensions': [{'name': 'ga:country'}]
                }]
            }
        }

        google_api_client_hook.query(endpoint, data, paginate=True)

        api_endpoint_name_parts = endpoint.split('.')
        google_api_conn_client = mock_get_conn.return_value
        mock_getattr.assert_has_calls([
            call(google_api_conn_client, api_endpoint_name_parts[1]),
            call()(),
            call(google_api_conn_client_sub_call, api_endpoint_name_parts[2]),
            call()(**data),
            call()().__bool__(),
            call()().execute(),
            call(google_api_conn_client, api_endpoint_name_parts[1]),
            call()(),
            call(google_api_conn_client_sub_call, api_endpoint_name_parts[2] + '_next'),
            call()(google_api_conn_client_sub_call, google_api_conn_client_sub_call.execute.return_value),
            call()().__bool__(),
            call()().execute(),
            call(google_api_conn_client, api_endpoint_name_parts[1]),
            call()(),
            call(google_api_conn_client_sub_call, api_endpoint_name_parts[2] + '_next'),
            call()(google_api_conn_client_sub_call, google_api_conn_client_sub_call.execute.return_value)
        ])


if __name__ == '__main__':
    unittest.main()
