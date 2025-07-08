from pydantic import BaseModel, model_validator

ERROR_STATUS = 'ERROR_SQS_MESSAGE'
OK_STATUS = 'SUCCESS_SQS_MESSAGE'
class SQSHandlerResponse(BaseModel):
    requeue_delay_sec: int | None = None

