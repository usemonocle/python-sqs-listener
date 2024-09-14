"""
class for running sqs message launcher

Created December 22nd, 2016
@author: Yaakov Gesher
@version: 0.9.0
@license: Apache
"""
# ================
# start imports
# ================

import asyncio
import json
import logging
import os

import aioboto3
import botocore.exceptions
from botocore.config import Config
from botocore.exceptions import SSOTokenLoadError

# ================
# start class
# ================

sqs_logger = logging.getLogger('async_sqs_launcher')


class AsyncSqsLauncher(object):

    def __init__(self, queue=None, queue_url=None, create_queue=False, visibility_timeout='600', serializer=json.dumps,
                 region_name=None):
        """
        :param queue: (str) name of queue to listen to
        :param queue_url: (str) url of queue to listen to
        :param create_queue (boolean) determines whether to create the queue if it doesn't exist.  If False, an
                                    Exception will be raised if the queue doesn't already exist
        :param visibility_timeout: (str) Relevant to queue creation.  Indicates the number of seconds for which the SQS will hide the message.
                                    Typically this should reflect the maximum amount of time your handler method will take
                                    to finish execution. See http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
                                    for more information
        :param: region_name: manually set region name
        """
        if not any([queue, queue_url]):
            raise ValueError('Either `queue` or `queue_url` should be provided.')

        # new session for each instantiation
        self._session = aioboto3.session.Session()
        self._region_name = region_name or self._session.region_name
        self._queue_name = queue
        self._queue_url = queue_url
        self._serializer = serializer
        self._create_queue = create_queue
        self._visibility_timeout = visibility_timeout
        self._is_init = False

        if self._region_name is None:
            raise ValueError('Region name should be provided or inferred from boto3 session')

    async def _init(self):
        creds = await self._session.get_credentials()
        if creds is None:
            raise botocore.exceptions.NoCredentialsError()

        if (
                not os.environ.get('AWS_ACCOUNT_ID') and
                creds.method not in ['sso','iam-role', 'assume-role', 'assume-role-with-web-identity']
        ):
            raise EnvironmentError('Environment variable `AWS_ACCOUNT_ID` not set and no role found.')

        if not self._queue_url:
            async with self._session.client('sqs', region_name=self._region_name) as sqs:
                try:
                    queues = await sqs.list_queues(QueueNamePrefix=self._queue_name)
                except SSOTokenLoadError:
                    raise EnvironmentError('Error loading SSO Token. Reauthenticate via aws sso login.')
                exists = False
                for q in queues.get('QueueUrls', []):
                    qname = q.split('/')[-1]
                    if qname == self._queue_name:
                        exists = True
                        self._queue_url = q

                if not exists:
                    if self._create_queue:
                        q = await sqs.create_queue(
                            QueueName=self._queue_name,
                            Attributes={
                                'VisibilityTimeout': self._visibility_timeout
                            }
                        )
                        self._queue_url = q['QueueUrl']
                    else:
                        raise ValueError('No queue found with name ' + self._queue_name)
        else:
            self._queue_name = self._get_queue_name_from_url(self._queue_url)
        self._is_init = True

    async def launch_message(self, message, **kwargs):
        """
        sends a message to the queue specified in the constructor
        :param message: (dict)
        :param kwargs: additional optional keyword arguments (DelaySeconds, MessageAttributes, MessageDeduplicationId, or MessageGroupId)
                        See http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Client.send_message for more information
        :return: (dict) the message response from SQS
        """
        sqs_logger.info("Sending message to queue " + self._queue_name)
        tries = 0
        # When under high load sometimes there is "NoCredentialsError"
        # The assumption is the credentials are about to expire so multiple requests tries
        # to refresh them, and then the EC2 metadata service is rate limited.
        while tries < 3:
            try:
                return await self._launch_message_single_try(message, **kwargs)
            except botocore.exceptions.NoCredentialsError:
                tries += 1
                if tries > 3:
                    raise
                await asyncio.sleep(0)
                sqs_logger.info("No credentials. Trying again")

    async def _launch_message_single_try(self, message, **kwargs):
        if not self._is_init:
            await self._init()
        async with self._session.client('sqs', region_name=self._region_name,
                                        config=Config(retries=dict(max_attempts=400))) as sqs:
            return await sqs.send_message(
                QueueUrl=self._queue_url,
                MessageBody=self._serializer(message),
                **kwargs
            )

    def _get_queue_name_from_url(self, url):
        return url.split('/')[-1]
