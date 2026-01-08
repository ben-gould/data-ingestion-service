from datetime import datetime
from pydantic import BaseModel, Field

class Transaction(BaseModel):
    transaction_id: str
    timestamp: datetime
    user_id: str
    amount: float = Field(..., ge=0)
    currency: str

