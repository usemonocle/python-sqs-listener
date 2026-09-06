"""The redaction boundary: what `format_failure` is allowed to put in a log line.

Stdlib only, so this runs on a bare checkout with nothing but pytest installed.
"""

import pytest

from sqs_listener.log_redaction import format_failure
from sqs_listener.log_redaction import UNKNOWN_MESSAGE_ID

SECRET = 'shopper@redaction-probe.test'
QUEUE = 'probe-queue'
MESSAGE_ID = 'probe-message-id'


class VendorError(Exception):
    """Stands in for a client library whose message is built from the request it made."""


def _raised(exc):
    try:
        raise exc
    except type(exc) as caught:
        return caught


def _wrapped():
    try:
        raise RuntimeError('vendor lookup failed') from _raised(VendorError(f'GET ...?email={SECRET}'))
    except RuntimeError as caught:
        return caught


def test_the_exception_message_never_reaches_the_line():
    rendered = format_failure(QUEUE, MESSAGE_ID, _raised(VendorError(f'GET ...?email={SECRET}')))

    assert SECRET not in rendered
    assert 'VendorError' in rendered


def test_the_operational_handles_are_kept():
    rendered = format_failure(QUEUE, MESSAGE_ID, _raised(VendorError('boom')))

    assert f'[QUEUE={QUEUE}]' in rendered
    assert f'[MESSAGE_ID={MESSAGE_ID}]' in rendered
    assert 'in _raised' in rendered


def test_a_missing_message_id_renders_the_sentinel():
    assert f'[MESSAGE_ID={UNKNOWN_MESSAGE_ID}]' in format_failure(QUEUE, None, _raised(VendorError('boom')))


def test_a_wrapper_does_not_hide_the_cause():
    rendered = format_failure(QUEUE, MESSAGE_ID, _wrapped())

    assert SECRET not in rendered
    assert 'RuntimeError' in rendered
    assert 'VendorError' in rendered
    # The wrapper's own frames name the wrapper, not the call that actually failed.
    assert 'in _raised' in rendered


def test_a_context_chain_is_walked_and_a_suppressed_one_is_not():
    try:
        try:
            raise VendorError(f'GET ...?email={SECRET}')
        except VendorError:
            raise RuntimeError('while handling')
    except RuntimeError as caught:
        implicit = caught

    assert 'VendorError' in format_failure(QUEUE, MESSAGE_ID, implicit)
    assert 'VendorError' not in format_failure(QUEUE, MESSAGE_ID, _raised(RuntimeError('alone')))


def test_a_cycle_in_the_chain_terminates():
    first = _raised(VendorError('first'))
    second = _raised(RuntimeError('second'))
    first.__context__ = second
    second.__context__ = first

    assert 'RuntimeError' in format_failure(QUEUE, MESSAGE_ID, first)


@pytest.mark.parametrize('passed', [VendorError, VendorError(f'GET ...?email={SECRET}')])
def test_a_class_or_an_instance_both_render_a_class_name(passed):
    rendered = format_failure(QUEUE, MESSAGE_ID, passed)

    assert SECRET not in rendered
    assert 'VendorError' in rendered
