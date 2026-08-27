from fastapi import FastAPI
from pydantic import BaseModel #defines data structures

app = FastAPI()


class FetchRequest(BaseModel):
    topic: str
    partition: int
    offset: int
    max_bytes: int

class FetchResponse(BaseModel):
    records: list[dict]

@app.post("/fetch", response_model=FetchResponse)
def fetch(request: FetchRequest) -> FetchResponse:
    """Read up to max_bytes of records starting at offset from the given"""
    raise NotImplementedError("This endpoint is not yet implemented.")


class ProduceRequest(BaseModel):
    topic: str
    key: str | None
    value: bytes

class ProduceResponse(BaseModel):
    partition: int
    offset: int

@app.post("/produce", response_model=ProduceResponse)
def produce(request: ProduceRequest) -> ProduceResponse:
    """Route by key (or round-robin if key is None), append to the target
    partition's log, and return where it landed."""
    raise NotImplementedError("This endpoint is not yet implemented.")


class CommitRequest(BaseModel):
    group: str
    topic: str
    partition: int
    offset: int

class CommitResponse(BaseModel):
    ack: bool

@app.post("/commit", response_model=CommitResponse)
def commit(request: CommitRequest) -> CommitResponse:
    """Durably record that `group` has processed up to `offset` on `partition`."""
    raise NotImplementedError("This endpoint is not yet implemented.")


class JoinGroupRequest(BaseModel):
    group: str
    consumer_id: str
    topic: str

class JoinGroupResponse(BaseModel):
    partitions: list[int]

@app.post("/join_group", response_model=JoinGroupResponse)
def join_group(request: JoinGroupRequest) -> JoinGroupResponse:
    """Add consumer_id to group, trigger partition (re)assignment, and
    return the partitions assigned to this consumer."""
    raise NotImplementedError("This endpoint is not yet implemented.")


class MetadataResponse(BaseModel):
    partition_count: int

@app.get("/metadata/{topic}", response_model=MetadataResponse)
def metadata(topic: str) -> MetadataResponse:
    """Return the partition count for a topic."""
    raise NotImplementedError("This endpoint is not yet implemented.")
