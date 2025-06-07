from pydantic import BaseModel

class DailyOHLC(BaseModel):
    time: int 
    open: str
    high: str
    low: str
    close: str
    vwap: str
    volume: str
    count: int