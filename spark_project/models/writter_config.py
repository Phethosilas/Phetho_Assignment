from typing import Dict
from enum import Enum
import pydantic
from pydantic import BaseModel, Field

class SparkWriteMode(str,Enum):
    Overwrite = "overwrite"
    Append = "append"
    ErrorIfExists = "error"
    Ignore = "ignore"


class WritterConfig(BaseModel):

    """
    The spark reader type
    """
    type: str

    """
    The file or database table we want to read
    """
    target: str

    """
    This is key value pair of the spark properties that the reader required
    """
    options: Dict[str, str] = Field(default_factory=dict)

    """
    This is for appending
    
    """
    mode: SparkWriteMode = SparkWriteMode.ErrorIfExists
