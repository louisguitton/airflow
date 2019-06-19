# -*- coding: utf-8 -*-
import json

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.google_discovery_api_hook import GoogleDiscoveryApiHook
from airflow.hooks.S3_hook import S3Hook


class GoogleApiToS3Transfer(BaseOperator):
    """
    Basic class for transferring data from a Google API endpoint into a S3 Bucket.

    :param google_api_service_name: The specific API service that is being requested.
    :type google_api_service_name: str
    :param google_api_service_version: The version of the API that is being requested.
    :type google_api_service_version: str
    :param google_api_endpoint_path: The client libraries path to the api call's executing method.
                                     For example: 'analyticsreporting.reports.batchGet'
                                     NOTE: See https://developers.google.com/apis-explorer
                                     for more information on what methods are available.
    :type google_api_endpoint_path: str
    :param google_api_endpoint_params: The params to control the corresponding endpoint result.
    :type google_api_endpoint_params: dict
    :param s3_destination_key: The url where to put the data retrieved
                               from the endpoint in S3.
    :type s3_destination_key: str
    :param google_api_response_via_xcom: Can be set to expose the google api response
                                         to xcom.
                                         The default value is None.
    :type google_api_response_via_xcom: str
    :param google_api_endpoint_params_via_xcom: If set to a value this value will be used as a key
                                                for pulling from xcom and updating the google api
                                                endpoint params.
                                                The default value is None.
    :type google_api_endpoint_params_via_xcom: str
    :param google_api_endpoint_params_via_xcom_task_ids: Task ids to filter xcom by.
                                                         The default value is None.
    :type google_api_endpoint_params_via_xcom_task_ids: str or list of str
    :param google_api_pagination: If set to True Pagination will be enabled for this request
                                  to retrieve all data.
                                  NOTE: This means the response will be a list of responses.
    :type google_api_pagination: bool
    :param s3_overwrite: Specifies whether the s3 file will be overwritten if exists.
                         The default value is True.
    :type s3_overwrite: bool
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string
    :param s3_conn_id: The connection id specifying the authentication information
                       for the S3 Bucket.
                       The default value is 's3_default'.
    :type s3_conn_id: str
    """

    template_fields = (
        'google_api_endpoint_params',
        's3_destination_key',
    )
    template_ext = ()
    ui_color = '#cc181e'

    @apply_defaults
    def __init__(
        self,
        google_api_service_name,
        google_api_service_version,
        google_api_endpoint_path,
        google_api_endpoint_params,
        s3_destination_key,
        *args,
        google_api_response_via_xcom=None,
        google_api_endpoint_params_via_xcom=None,
        google_api_endpoint_params_via_xcom_task_ids=None,
        google_api_pagination=False,
        s3_overwrite=True,
        gcp_conn_id='google_cloud_default',
        delegate_to=None,
        s3_conn_id='s3_default',
        **kwargs
    ):
        super(GoogleApiToS3Transfer, self).__init__(*args, **kwargs)
        self.google_api_service_name = google_api_service_name
        self.google_api_service_version = google_api_service_version
        self.google_api_endpoint_path = google_api_endpoint_path
        self.google_api_endpoint_params = google_api_endpoint_params
        self.s3_destination_key = s3_destination_key
        self.google_api_response_via_xcom = google_api_response_via_xcom
        self.google_api_endpoint_params_via_xcom = google_api_endpoint_params_via_xcom
        self.google_api_endpoint_params_via_xcom_task_ids = \
            google_api_endpoint_params_via_xcom_task_ids
        self.google_api_pagination = google_api_pagination
        self.s3_overwrite = s3_overwrite
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        """
        Transfers Google APIs json data to S3.

        :param context: The context that is being provided when executing.
        :type context: dict
        """
        self.log.info(
            'Transferring data from %s to s3', self.google_api_service_name
        )

        if self.google_api_endpoint_params_via_xcom:
            self._update_google_api_endpoint_params_via_xcom(
                context['task_instance']
            )

        data = self._retrieve_data_from_google_api()

        if self.google_api_response_via_xcom:
            self._expose_google_api_response_via_xcom(
                context['task_instance'], data
            )

        self._load_data_to_s3(data)

    def _retrieve_data_from_google_api(self):
        google_api_client = GoogleDiscoveryApiHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_service_name=self.google_api_service_name,
            api_version=self.google_api_service_version
        )
        google_api_conn_client = google_api_client.get_conn()

        google_api_response = self._call_google_api_request(
            google_api_conn_client
        )
        return google_api_response

    def _load_data_to_s3(self, data):
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        s3_hook.load_string(
            string_data=json.dumps(data),
            key=self.s3_destination_key,
            replace=self.s3_overwrite
        )

    def _build_google_api_request(
        self, google_api_conn_client, google_api_sub_functions
    ):
        for sub_function in google_api_sub_functions:
            google_api_conn_client = getattr(
                google_api_conn_client, sub_function
            )
            if sub_function != google_api_sub_functions[-1]:
                google_api_conn_client = google_api_conn_client()
            else:
                google_api_conn_client = google_api_conn_client(
                    **self.google_api_endpoint_params
                )

        return google_api_conn_client

    def _call_google_api_request(self, google_api_conn_client):
        google_api_endpoint_parts = self.google_api_endpoint_path.split('.')

        google_api_endpoint_instance = self._build_google_api_request(
            google_api_conn_client, google_api_endpoint_parts[1:]
        )

        if self.google_api_pagination:
            return self._paginate_google_api(
                google_api_endpoint_instance, google_api_conn_client,
                google_api_endpoint_parts
            )

        return google_api_endpoint_instance.execute()

    def _update_google_api_endpoint_params_via_xcom(self, task_instance):
        google_api_endpoint_params = task_instance.xcom_pull(
            task_ids=self.google_api_endpoint_params_via_xcom_task_ids,
            key=self.google_api_endpoint_params_via_xcom
        )
        self.google_api_endpoint_params.update(google_api_endpoint_params)

    def _expose_google_api_response_via_xcom(self, task_instance, data):
        task_instance.xcom_push(
            key=self.google_api_response_via_xcom, value=data
        )

    def _paginate_google_api(
        self, google_api_endpoint_instance, google_api_conn_client,
        google_api_endpoint_parts
    ):
        google_api_responses = []

        while google_api_endpoint_instance:
            google_api_response = google_api_endpoint_instance.execute()

            google_api_responses.append(google_api_response)

            google_api_endpoint_instance = self._build_next_google_api_request(
                google_api_conn_client, google_api_endpoint_parts[1:],
                google_api_endpoint_instance, google_api_response
            )

        return google_api_responses

    def _build_next_google_api_request(
        self, google_api_conn_client, google_api_sub_functions,
        google_api_endpoint_instance, google_api_response
    ):
        for sub_function in google_api_sub_functions:
            if sub_function != google_api_sub_functions[-1]:
                google_api_conn_client = getattr(
                    google_api_conn_client, sub_function
                )
                google_api_conn_client = google_api_conn_client()
            else:
                google_api_conn_client = getattr(
                    google_api_conn_client, sub_function + '_next'
                )
                google_api_conn_client = google_api_conn_client(
                    google_api_endpoint_instance, google_api_response
                )

        return google_api_conn_client
