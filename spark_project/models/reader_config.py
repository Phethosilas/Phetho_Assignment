from typing import Dict
from pydantic import BaseModel, Field

class ReaderConfig(BaseModel):

    """
    The spark reader type
    """
    type: str

    """
    The file or database table we want to read
    """
    source: str

    """
    This is key value pair of the spark properties that the reader required
    """
    options: Dict[str, str] = Field(default_factory=dict)


