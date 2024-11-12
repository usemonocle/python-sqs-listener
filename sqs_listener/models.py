from pydantic import BaseModel, model_validator


class SQSHandlerResponse(BaseModel):
    requeue_delay_sec: int | None = None

