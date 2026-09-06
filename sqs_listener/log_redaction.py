"""Failure rendering for listener logs that cannot replay the message payload.

The SQS body is customer data and the exception's own text is not safe either:
``httpx.HTTPStatusError.__str__`` embeds the request URL, and vendor profile lookups
filter by the shopper's email or phone. So a failure is described by exception class,
SQS message id, queue name and traceback FRAMES only -- never the body, never the
exception value, never ``exc_info``.
"""

import traceback

UNKNOWN_MESSAGE_ID = 'unknown'


def format_failure(queue_name, message_id, exc_type, exc_tb):
    """Render an operational failure description that carries no message content."""
    error_type = getattr(exc_type, '__name__', None) or str(exc_type)
    frames = ''.join(traceback.format_tb(exc_tb)) if exc_tb is not None else ''
    return (
        f'[QUEUE={queue_name}] '
        f'[MESSAGE_ID={message_id or UNKNOWN_MESSAGE_ID}] '
        f'[ERROR_TYPE={error_type}]\n{frames}'
    )
