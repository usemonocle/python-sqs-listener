# ================
# start imports
# ================
import asyncio
import json
import logging
import os
import sys
from abc import ABCMeta, abstractmethod
from concurrent.futures import wait
from contextlib import asynccontextmanager

import aioboto3

from sqs_launcher.asyncio import AsyncSqsLauncher

# ================
# start class
# ================

sqs_logger = logging.getLogger('async_sqs_listener')


class AsyncSqsListener(object):
    __metaclass__ = ABCMeta

    def __init__(self, queue, **kwargs):
        """
        :param queue: (str) name of queue to listen to
        :param kwargs: options for fine tuning. see below
        """
        self._aws_access_key = kwargs.get('aws_access_key', '')
        self._aws_secret_key = kwargs.get('aws_secret_key', '')
        self._queue_name = queue
        self._poll_interval = kwargs.get("interval", 5)
        self._queue_visibility_timeout = kwargs.get('visibility_timeout', '600')
        self._error_queue_name = kwargs.get('error_queue', None)
        self._error_queue_visibility_timeout = kwargs.get('error_visibility_timeout', '600')
        self._queue_url = kwargs.get('queue_url', None)
        self._message_attribute_names = kwargs.get('message_attribute_names', [])
        self._attribute_names = kwargs.get('attribute_names', [])
        self._force_delete = kwargs.get('force_delete', False)
        self._endpoint_name = kwargs.get('endpoint_name', None)
        self._wait_time = kwargs.get('wait_time', 0)
        self._max_number_of_messages = kwargs.get('max_number_of_messages', 1)
        self._deserializer = kwargs.get("deserializer", json.loads)
        self._max_parallel_semaphore = asyncio.Semaphore(kwargs.get('max_messages_parallelism', 200))
        self._region_name = kwargs.get('region_name')
        self._error_queue_launcher = None
        self._tasks = set()
        self._run = True

    @asynccontextmanager
    async def _initialize_client(self):
        # First initialize boto3 session
        if len(self._aws_access_key) != 0 and len(self._aws_secret_key) != 0:
            self._session = aioboto3.Session(
                aws_access_key_id=self._aws_access_key,
                aws_secret_access_key=self._aws_secret_key
            )
        else:
            self._session = aioboto3.session.Session()
            if (
                    not os.environ.get('AWS_ACCOUNT_ID', None) and
                    not ((await self._session.get_credentials()).method in
                         ['iam-role', 'assume-role', 'assume-role-with-web-identity'])
            ):
                raise EnvironmentError('Environment variable `AWS_ACCOUNT_ID` not set and no role found.')

        if self._region_name is None:
            self._region_name = self._session.region_name

        # new session for each instantiation
        ssl = True
        if self._region_name == 'elasticmq':
            ssl = False

        async with self._session.client('sqs', region_name=self._region_name,
                                        endpoint_url=self._endpoint_name, use_ssl=ssl) as sqs:
            queues = await sqs.list_queues(QueueNamePrefix=self._queue_name)
            main_queue_exists = False
            error_queue_exists = False
            if 'QueueUrls' in queues:
                for q in queues['QueueUrls']:
                    qname = q.split('/')[-1]
                    if qname == self._queue_name:
                        main_queue_exists = True
                    if self._error_queue_name and qname == self._error_queue_name:
                        error_queue_exists = True

            # create queue if necessary.
            # creation is idempotent, no harm in calling on a queue if it already exists.
            if self._queue_url is None:
                if not main_queue_exists:
                    sqs_logger.warning("main queue not found, creating now")

                    # is this a fifo queue?
                    if self._queue_name.endswith(".fifo"):
                        fifo_queue = "true"
                        q = await sqs.create_queue(
                            QueueName=self._queue_name,
                            Attributes={
                                'VisibilityTimeout': self._queue_visibility_timeout,  # 10 minutes
                                'FifoQueue': fifo_queue
                            }
                        )
                    else:
                        # need to avoid FifoQueue property for normal non-fifo queues
                        q = await sqs.create_queue(
                            QueueName=self._queue_name,
                            Attributes={
                                'VisibilityTimeout': self._queue_visibility_timeout,  # 10 minutes
                            }
                        )
                    self._queue_url = q['QueueUrl']

            if self._error_queue_name and not error_queue_exists:
                sqs_logger.warning("error queue not found, creating now")
                await sqs.create_queue(
                    QueueName=self._error_queue_name,
                    Attributes={
                        'VisibilityTimeout': self._queue_visibility_timeout  # 10 minutes
                    }
                )

            if self._queue_url is None:
                if os.environ.get('AWS_ACCOUNT_ID', None):
                    qs = await sqs.get_queue_url(QueueName=self._queue_name,
                                                 QueueOwnerAWSAccountId=os.environ.get('AWS_ACCOUNT_ID', None))
                else:
                    qs = await sqs.get_queue_url(QueueName=self._queue_name)
                self._queue_url = qs['QueueUrl']
            yield sqs

    async def _start_listening(self, client):
        while self._run:
            # Using this structure instead of "await sema.acquire()" in order to continuously check on the _run flag
            if self._max_parallel_semaphore.locked():
                await asyncio.sleep(0)
                continue

            # calling with WaitTimeSeconds of zero show the same behavior as
            # not specifying a wait time, ie: short polling
            messages = await client.receive_message(
                QueueUrl=self._queue_url,
                MessageAttributeNames=self._message_attribute_names,
                AttributeNames=self._attribute_names,
                WaitTimeSeconds=self._wait_time,
                MaxNumberOfMessages=self._max_number_of_messages
            )
            if 'Messages' in messages:
                sqs_logger.debug(messages)
                sqs_logger.info("{} messages received".format(len(messages['Messages'])))
                for m in messages['Messages']:
                    task = asyncio.create_task(self.process_message(m, client))
                    # Asyncio tasks can be garbage collected if they don't have
                    # a reference, so adds the task to a set.
                    self._tasks.add(task)
                    task.add_done_callback(self._handle_task_finish)
            else:
                await asyncio.sleep(self._poll_interval)

        # If we get here, we're shutting down. Wait for all tasks to finish
        wait(self._tasks)

    async def process_message(self, m, client):
        async with self._max_parallel_semaphore:
            receipt_handle = m['ReceiptHandle']
            m_body = m['Body']
            message_attribs = None
            attribs = None

            try:
                deserialized = self._deserializer(m_body)
            except:
                sqs_logger.exception("Unable to parse message")
                return

            if 'MessageAttributes' in m:
                message_attribs = m['MessageAttributes']
            if 'Attributes' in m:
                attribs = m['Attributes']
            try:
                if self._force_delete:
                    await client.delete_message(
                        QueueUrl=self._queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    await self.handle_message(deserialized, message_attribs, attribs)
                else:
                    await self.handle_message(deserialized, message_attribs, attribs)
                    await client.delete_message(
                        QueueUrl=self._queue_url,
                        ReceiptHandle=receipt_handle
                    )
            except Exception as ex:
                sqs_logger.exception(ex)
                if self._error_queue_name:
                    if self._error_queue_launcher is None:
                        # Note: initializing the launcher only after region name was set in _initialize_client
                        self._error_queue_launcher = AsyncSqsLauncher(queue=self._error_queue_name,
                                                                      create_queue=True,
                                                                      region_name=self._region_name)
                    exc_type, exc_obj, exc_tb = sys.exc_info()

                    sqs_logger.info("Pushing exception to error queue")
                    await self._error_queue_launcher.launch_message(
                        {
                            'exception_type': str(exc_type),
                            'error_message': str(ex.args)
                        }
                    )

    async def listen(self):
        async with self._initialize_client() as client:
            sqs_logger.info("Listening to queue " + self._queue_name)
            if self._error_queue_name:
                sqs_logger.info("Using error queue " + self._error_queue_name)

            await self._start_listening(client)

    def stop(self):
        self._run = False

    def _prepare_logger(self):
        logger = logging.getLogger('eg_daemon')
        logger.setLevel(logging.INFO)

        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)

        formatstr = '[%(asctime)s - %(name)s - %(levelname)s]  %(message)s'
        formatter = logging.Formatter(formatstr)

        sh.setFormatter(formatter)
        logger.addHandler(sh)

    def _handle_task_finish(self, task):
        try:
            # Prevents silent failure in case of uncaught exceptions in tasks
            task.result()
        except Exception as e:
            sqs_logger.exception("Task failed with exception: %s", e)
        finally:
            self._tasks.remove(task)

    @abstractmethod
    async def handle_message(self, body, attributes, messages_attributes):
        """
        Implement this method to do something with the SQS message contents
        :param body: dict
        :param attributes: dict
        :param messages_attributes: dict
        :return:
        """
        return
