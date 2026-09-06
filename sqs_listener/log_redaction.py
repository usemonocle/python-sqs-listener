"""Failure rendering for listener logs that cannot replay the message payload.

The SQS body is customer data, and an exception's own text is not safe either: a client
library builds its message out of the request it made, so a failed vendor call carries the
request URL and its query string. A failure is therefore described by exception CLASS, SQS
message id, queue name and traceback FRAMES only -- never the body, never an exception
value, never ``exc_info``.
"""

import traceback

UNKNOWN_MESSAGE_ID = 'unknown'

# A wrapper, an adapter, a client and a transport around the root failure is the realistic
# depth; beyond that a chain is re-inflating the log surface this renderer exists to shrink.
MAX_CHAIN_LINKS = 5


def format_failure(queue_name, message_id, exc):
    """Render an operational failure description that carries no message content."""
    lines = [
        f'[QUEUE={queue_name}] '
        f'[MESSAGE_ID={message_id or UNKNOWN_MESSAGE_ID}] '
        f'[ERROR_TYPE={_type_name(exc)}]',
    ]
    links = []
    for link in _chain(exc):
        links.append(link)
        if len(links) > MAX_CHAIN_LINKS:
            break
    for depth, link in enumerate(links[:MAX_CHAIN_LINKS]):
        if depth:
            lines.append(f'caused by {_type_name(link)}')
        frames = ''.join(traceback.format_tb(link.__traceback__)).rstrip('\n')
        if frames:
            lines.append(frames)
    if len(links) > MAX_CHAIN_LINKS:
        lines.append(f'... chain truncated at {MAX_CHAIN_LINKS} links')
    return '\n'.join(lines)


def _type_name(exc):
    cls = exc if isinstance(exc, type) else type(exc)
    module = getattr(cls, '__module__', None)
    return cls.__name__ if module in (None, 'builtins') else f'{module}.{cls.__name__}'


def _chain(exc):
    """Walk __cause__/__context__ as the interpreter does; a wrapper alone names no frames."""
    seen = set()
    while isinstance(exc, BaseException) and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        if exc.__cause__ is not None:
            exc = exc.__cause__
        elif exc.__suppress_context__:
            exc = None
        else:
            exc = exc.__context__
