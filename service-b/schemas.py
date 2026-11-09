"""Serialisasi: kontrak pesan JSON yang divalidasi dengan Pydantic (versi skema eksplisit)."""
from typing import Literal
from pydantic import BaseModel, Field

class CheckoutRequest(BaseModel):
    sku: str = Field(min_length=1)
    qty: int = Field(gt=0)
    amount: float = Field(gt=0)
    card_token: str = Field(min_length=1)
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    simulate_delay_ms: int = Field(default=0, ge=0)   # untuk demo timeout

class OrderEvent(BaseModel):
    """Pesan asinkron: dikirim A -> B lewat broker."""
    schema_version: Literal[1] = 1
    message_id: str            # kunci idempotensi
    event_type: Literal["order.confirmed"] = "order.confirmed"
    order_id: str
    sku: str
    qty: int
    amount: float
    email: str
    created_at: float
