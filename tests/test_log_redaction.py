"""The redaction boundary: what `format_failure` is allowed to put in a log line.

The module under test imports nothing but `traceback`; importing it through the package
pulls the package's own dependencies, so a bare checkout needs those installed plus pytest.
"""

import pytest

from sqs_listener.log_redaction import format_failure
from sqs_listener.log_redaction import MAX_CHAIN_LINKS
from sqs_listener.log_redaction import UNKNOWN_MESSAGE_ID

SECRET = 'shopper@redaction-probe.test'
QUEUE = 'probe-queue'
MESSAGE_ID = 'probe-message-id'
# Passed as a variable, never written as a literal at a `raise`: rendered frames carry each
# frame's SOURCE text, so a literal in the raising line would appear whatever the renderer does.
OUTERMOST_MESSAGE = 'consumer-raised-value'


class VendorError(Exception):
    """Stands in for a client library whose message is built from the request it made."""


def _raised(exc):
    try:
        raise exc
    except type(exc) as caught:
        return caught


def _outermost_wrap(cause, message):
    """The last wrap, in a frame of its own, so truncation can be checked from which end."""
    try:
        raise RuntimeError(message) from cause
    except RuntimeError as caught:
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
    # `in _raised` is the CAUSE's own frame: the wrapper's frames alone would not name it.
    assert 'in _raised' in rendered


def test_an_implicit_context_is_walked():
    try:
        try:
            raise VendorError(f'GET ...?email={SECRET}')
        except VendorError:
            raise RuntimeError('while handling')
    except RuntimeError as caught:
        implicit = caught

    rendered = format_failure(QUEUE, MESSAGE_ID, implicit)

    assert SECRET not in rendered
    assert 'VendorError' in rendered


def test_a_suppressed_context_is_not_walked():
    try:
        try:
            raise VendorError(f'GET ...?email={SECRET}')
        except VendorError:
            raise RuntimeError('handled, context suppressed') from None
    except RuntimeError as caught:
        suppressed = caught

    # `from None` leaves __context__ set and only flips __suppress_context__, so a walk that
    # ignored the flag would still reach VendorError here.
    assert suppressed.__context__ is not None
    rendered = format_failure(QUEUE, MESSAGE_ID, suppressed)

    assert 'RuntimeError' in rendered
    assert 'VendorError' not in rendered
    assert SECRET not in rendered


def test_a_deep_chain_is_truncated_rather_than_replayed_in_full():
    deep = _raised(VendorError(f'GET ...?email={SECRET}'))
    for step in range(20):
        try:
            raise RuntimeError(f'layer {step}') from deep
        except RuntimeError as caught:
            deep = caught
    deep = _outermost_wrap(deep, OUTERMOST_MESSAGE)

    rendered = format_failure(QUEUE, MESSAGE_ID, deep)

    assert SECRET not in rendered
    assert rendered.count('caused by ') == MAX_CHAIN_LINKS - 1
    assert f'... chain truncated at {MAX_CHAIN_LINKS} links' in rendered
    # Truncation drops the far end, so the link the consumer raised survives it...
    assert 'in _outermost_wrap' in rendered
    # ...while its message, like every other link's, is not rendered.
    assert OUTERMOST_MESSAGE not in rendered
    assert 'layer 19' not in rendered


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
