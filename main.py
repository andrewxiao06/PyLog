from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel #defines data structures

from pylog.broker.server import Broker

app = FastAPI()
broker = Broker(Path("./data"))


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
    topic = broker.get_or_create_topic(request.topic)
    records = topic.read_from(request.partition, request.offset, request.max_bytes)
    return FetchResponse(
        records=[
            {
                "offset": r.offset,
                "timestamp": r.timestamp,
                "key": r.key.decode("utf-8", errors="replace") if r.key else None,
                "value": r.value.decode("utf-8", errors="replace"),
            }
            for r in records
        ]
    )


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
    topic = broker.get_or_create_topic(request.topic)
    key_bytes = request.key.encode("utf-8") if request.key is not None else None
    partition, offset = topic.append(key_bytes, request.value)
    return ProduceResponse(partition=partition, offset=offset)


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
    broker.offsets.commit(request.group, request.topic, request.partition, request.offset)
    return CommitResponse(ack=True)


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
    topic = broker.get_or_create_topic(request.topic)
    group = broker.get_or_create_group(request.group, request.topic, topic.partition_count)
    partitions = group.join(request.consumer_id)
    return JoinGroupResponse(partitions=partitions)


class MetadataResponse(BaseModel):
    partition_count: int

@app.get("/metadata/{topic}", response_model=MetadataResponse)
def metadata(topic: str) -> MetadataResponse:
    """Return the partition count for a topic."""
    t = broker.get_or_create_topic(topic)
    return MetadataResponse(partition_count=t.partition_count)
