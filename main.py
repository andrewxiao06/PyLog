from fastapi import FastAPI
from pydantic import BaseModel #defines data structures

app = FastAPI()


class FetchRequest(BaseModel):
    topic: str
    partition: str
    offset: int
    max_bytes: int

class FetchResponse(BaseModel):
    records = list[dict]

@app.post("/fetch", responseModel = FetchResponse)
def fetch(request: FetchRequest) -> FetchResponse:
    """Read up to max_bytes of records starting at offset from the given"""
    raise NotImplementedError("This endpoint is not yet implemented.")
