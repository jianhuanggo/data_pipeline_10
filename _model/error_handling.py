from pydantic import BaseModel
from typing import Optional

class ErrorHandlingModel(BaseModel):
    process_name: str
    error_type: str
    error_msg: str
    recovery_type: str
    recovery_method: str
    comment: Optional[str] = None

