"""Failure records must stay groupable by an aggregator that keys on the log template.

Sentry's logging integration groups a message event by the record's unformatted template
(``record.msg``), so a template that is identical for every failure collapses every failure
class from a listener into one issue. The failure class belongs in the template; the message
id and frames ride as arguments, and nothing from the body or the exception value appears.
"""

import asyncio
import json
import logging

import pytest

from sqs_listener.asyncio import AsyncSqsListener
from sqs_listener.asyncio import sqs_logger

SECRET = 'shopper@grouping-probe.test'
QUEUE = 'probe-queue'
BODY = json.dumps({'email': SECRET})


class VendorError(Exception):
    pass


class RecordCollector(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


class Listener(AsyncSqsListener):
    def __init__(self, error):
        self._queue_name = QUEUE
        self._deserializer = json.loads
        self._force_delete = False
        self._error_queue_name = None
        self._max_parallel_semaphore = asyncio.Semaphore(1)
        self._error = error

    async def handle_message(self, body, attributes, messages_attributes):
        raise self._error


@pytest.fixture
def collector():
    handler = RecordCollector()
    sqs_logger.addHandler(handler)
    previous = sqs_logger.level
    sqs_logger.setLevel(logging.DEBUG)
    yield handler
    sqs_logger.removeHandler(handler)
    sqs_logger.setLevel(previous)


def _fail(collector, error, message_id):
    message = {'ReceiptHandle': 'rh', 'Body': BODY, 'MessageId': message_id}
    asyncio.run(Listener(error).process_message(message, client=None))
    errors = [r for r in collector.records if r.levelno >= logging.WARNING]
    assert len(errors) == 1
    collector.records.clear()
    return errors[0]


def _rendered(record):
    return logging.Formatter().format(record)


def test_two_failure_types_produce_different_templates(collector):
    vendor = _fail(collector, VendorError(f'GET ...?email={SECRET}'), 'mid-1')
    key = _fail(collector, KeyError(SECRET), 'mid-2')

    assert vendor.msg != key.msg
    assert 'VendorError' in vendor.msg
    assert 'KeyError' in key.msg


def test_one_failure_type_keeps_one_template_across_messages(collector):
    first = _fail(collector, VendorError(f'first {SECRET}'), 'mid-1')
    second = _fail(collector, VendorError(f'second {SECRET}'), 'mid-2')

    assert first.msg == second.msg
    assert 'mid-1' not in first.msg


def test_no_body_or_exception_text_is_rendered(collector):
    for error in (VendorError(f'GET ...?email={SECRET}'), KeyError(SECRET)):
        record = _fail(collector, error, 'mid-1')
        rendered = _rendered(record)

        assert SECRET not in rendered
        assert SECRET not in record.msg
        assert record.exc_info is None
        assert '[MESSAGE_ID=mid-1]' in rendered
        assert f'[QUEUE={QUEUE}]' in rendered


def test_a_parse_failure_is_grouped_by_its_type_without_the_body(collector):
    message = {'ReceiptHandle': 'rh', 'Body': f'not json {SECRET}', 'MessageId': 'mid-3'}
    asyncio.run(Listener(VendorError()).process_message(message, client=None))
    [record] = [r for r in collector.records if r.levelno >= logging.WARNING]

    assert 'json.decoder.JSONDecodeError' in record.msg
    assert SECRET not in _rendered(record)
    assert record.exc_info is None
