from pydantic import BaseModel, Field
from enum import Enum

class Actions(Enum):
    """Actions to take based on the analysis"""
    WEAK_SELL = 'Weak Sell'
    SELL = 'Sell'
    STRONG_SELL = 'Strong Sell'
    WEAK_BUY = 'Weak Buy'
    BUY = 'Buy'
    STRONG_BUY = 'Strong Buy'

class Analysis(BaseModel):
    """Help in structuring LLM output"""
    action: Actions = Field(description="Provide actions to take based on the technical analysis")
    reasons: dict[str, str] = Field(description="Provide the technical indicators used as a key and the analysis based on technical indicators as a value")
