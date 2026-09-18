"""Length-prefixed binary wire protocol.

Frame on the wire: [4-byte big-endian length][1-byte opcode][payload].
`length` covers everything after itself (opcode + payload), so a reader
always knows exactly how many more bytes to pull off the socket before
it has a complete message — no delimiters, no parsing ambiguity.

Every request/response type below packs itself the same way: fixed-size
fields via struct, variable-length fields (strings/bytes) as a 4-byte
length prefix followed by the raw bytes.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum

LENGTH_PREFIX_FORMAT = ">I"
LENGTH_PREFIX_SIZE = struct.calcsize(LENGTH_PREFIX_FORMAT)


class OpCode(IntEnum):
    PRODUCE = 1
    PRODUCE_RESPONSE = 2
    FETCH = 3
    FETCH_RESPONSE = 4
    COMMIT = 5
    COMMIT_RESPONSE = 6
    JOIN_GROUP = 7
    JOIN_GROUP_RESPONSE = 8
    METADATA = 9
    METADATA_RESPONSE = 10
    ERROR = 255


def encode_frame(opcode: "OpCode", payload: bytes) -> bytes:
    body = bytes([opcode]) + payload
    return struct.pack(LENGTH_PREFIX_FORMAT, len(body)) + body


async def read_frame(reader) -> tuple["OpCode", bytes]:
    """Read one complete frame from an asyncio StreamReader.
    `readexactly` already loops internally until it has all the bytes it
    asked for (or raises), so this is safe against a single recv()
    returning fewer bytes than requested."""
    length_bytes = await reader.readexactly(LENGTH_PREFIX_SIZE)
    (length,) = struct.unpack(LENGTH_PREFIX_FORMAT, length_bytes)
    body = await reader.readexactly(length)
    return OpCode(body[0]), body[1:]


def _pack_bytes(data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + data


def _unpack_bytes(buf: bytes, offset: int) -> tuple[bytes, int]:
    (length,) = struct.unpack_from(">I", buf, offset)
    offset += 4
    return bytes(buf[offset : offset + length]), offset + length


def _pack_str(s: str) -> bytes:
    return _pack_bytes(s.encode("utf-8"))


def _unpack_str(buf: bytes, offset: int) -> tuple[str, int]:
    raw, offset = _unpack_bytes(buf, offset)
    return raw.decode("utf-8"), offset


@dataclass
class ProduceRequest:
    topic: str
    key: bytes | None
    value: bytes

    def encode(self) -> bytes:
        has_key = self.key is not None
        return (
            _pack_str(self.topic)
            + struct.pack(">B", int(has_key))
            + _pack_bytes(self.key or b"")
            + _pack_bytes(self.value)
        )

    @classmethod
    def decode(cls, buf: bytes) -> "ProduceRequest":
        topic, offset = _unpack_str(buf, 0)
        (has_key,) = struct.unpack_from(">B", buf, offset)
        offset += 1
        key, offset = _unpack_bytes(buf, offset)
        value, offset = _unpack_bytes(buf, offset)
        return cls(topic=topic, key=(key if has_key else None), value=value)


@dataclass
class ProduceResponse:
    partition: int
    offset: int

    def encode(self) -> bytes:
        return struct.pack(">IQ", self.partition, self.offset)

    @classmethod
    def decode(cls, buf: bytes) -> "ProduceResponse":
        partition, offset = struct.unpack(">IQ", buf)
        return cls(partition=partition, offset=offset)


@dataclass
class FetchRequest:
    topic: str
    partition: int
    offset: int
    max_bytes: int

    def encode(self) -> bytes:
        return _pack_str(self.topic) + struct.pack(
            ">IQI", self.partition, self.offset, self.max_bytes
        )

    @classmethod
    def decode(cls, buf: bytes) -> "FetchRequest":
        topic, offset = _unpack_str(buf, 0)
        partition, record_offset, max_bytes = struct.unpack_from(">IQI", buf, offset)
        return cls(topic=topic, partition=partition, offset=record_offset, max_bytes=max_bytes)


@dataclass
class RecordPayload:
    offset: int
    timestamp: int
    key: bytes | None
    value: bytes

    def encode(self) -> bytes:
        has_key = self.key is not None
        return (
            struct.pack(">QQB", self.offset, self.timestamp, int(has_key))
            + _pack_bytes(self.key or b"")
            + _pack_bytes(self.value)
        )

    @classmethod
    def decode(cls, buf: bytes, offset: int) -> tuple["RecordPayload", int]:
        rec_offset, timestamp, has_key = struct.unpack_from(">QQB", buf, offset)
        offset += struct.calcsize(">QQB")
        key, offset = _unpack_bytes(buf, offset)
        value, offset = _unpack_bytes(buf, offset)
        return cls(
            offset=rec_offset,
            timestamp=timestamp,
            key=(key if has_key else None),
            value=value,
        ), offset


@dataclass
class FetchResponse:
    records: list[RecordPayload]
    high_watermark: int

    def encode(self) -> bytes:
        body = struct.pack(">IQ", len(self.records), self.high_watermark)
        for record in self.records:
            body += record.encode()
        return body

    @classmethod
    def decode(cls, buf: bytes) -> "FetchResponse":
        count, high_watermark = struct.unpack_from(">IQ", buf, 0)
        offset = struct.calcsize(">IQ")
        records = []
        for _ in range(count):
            record, offset = RecordPayload.decode(buf, offset)
            records.append(record)
        return cls(records=records, high_watermark=high_watermark)


@dataclass
class CommitRequest:
    group: str
    topic: str
    partition: int
    offset: int

    def encode(self) -> bytes:
        return (
            _pack_str(self.group)
            + _pack_str(self.topic)
            + struct.pack(">IQ", self.partition, self.offset)
        )

    @classmethod
    def decode(cls, buf: bytes) -> "CommitRequest":
        group, offset = _unpack_str(buf, 0)
        topic, offset = _unpack_str(buf, offset)
        partition, record_offset = struct.unpack_from(">IQ", buf, offset)
        return cls(group=group, topic=topic, partition=partition, offset=record_offset)


@dataclass
class CommitResponse:
    ack: bool

    def encode(self) -> bytes:
        return struct.pack(">B", int(self.ack))

    @classmethod
    def decode(cls, buf: bytes) -> "CommitResponse":
        (ack,) = struct.unpack(">B", buf)
        return cls(ack=bool(ack))


@dataclass
class JoinGroupRequest:
    group: str
    consumer_id: str
    topic: str

    def encode(self) -> bytes:
        return _pack_str(self.group) + _pack_str(self.consumer_id) + _pack_str(self.topic)

    @classmethod
    def decode(cls, buf: bytes) -> "JoinGroupRequest":
        group, offset = _unpack_str(buf, 0)
        consumer_id, offset = _unpack_str(buf, offset)
        topic, offset = _unpack_str(buf, offset)
        return cls(group=group, consumer_id=consumer_id, topic=topic)


@dataclass
class JoinGroupResponse:
    partitions: list[int]

    def encode(self) -> bytes:
        body = struct.pack(">I", len(self.partitions))
        for p in self.partitions:
            body += struct.pack(">I", p)
        return body

    @classmethod
    def decode(cls, buf: bytes) -> "JoinGroupResponse":
        (count,) = struct.unpack_from(">I", buf, 0)
        offset = 4
        partitions = []
        for _ in range(count):
            (p,) = struct.unpack_from(">I", buf, offset)
            partitions.append(p)
            offset += 4
        return cls(partitions=partitions)


@dataclass
class MetadataRequest:
    topic: str

    def encode(self) -> bytes:
        return _pack_str(self.topic)

    @classmethod
    def decode(cls, buf: bytes) -> "MetadataRequest":
        topic, _ = _unpack_str(buf, 0)
        return cls(topic=topic)


@dataclass
class MetadataResponse:
    partition_count: int

    def encode(self) -> bytes:
        return struct.pack(">I", self.partition_count)

    @classmethod
    def decode(cls, buf: bytes) -> "MetadataResponse":
        (count,) = struct.unpack(">I", buf)
        return cls(partition_count=count)


@dataclass
class ErrorResponse:
    message: str

    def encode(self) -> bytes:
        return _pack_str(self.message)

    @classmethod
    def decode(cls, buf: bytes) -> "ErrorResponse":
        message, _ = _unpack_str(buf, 0)
        return cls(message=message)
