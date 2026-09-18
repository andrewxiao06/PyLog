"""Thin async TCP client for JOIN_GROUP/FETCH/COMMIT.

At-least-once by construction: call commit() only *after* the caller has
finished processing a fetched batch, never before. If the process dies
between fetch and commit, the next run re-fetches from the last
committed offset and reprocesses those records — duplicates are
possible, loss is not. Consumers must be idempotent to cope with that,
which is the price at-least-once charges the consumer side."""

import asyncio

from pylog.broker.protocol import (
    CommitRequest,
    CommitResponse,
    ErrorResponse,
    FetchRequest,
    FetchResponse,
    JoinGroupRequest,
    JoinGroupResponse,
    OpCode,
    encode_frame,
    read_frame,
)


class Consumer:
    def __init__(
        self,
        group: str,
        topic: str,
        consumer_id: str,
        host: str = "127.0.0.1",
        port: int = 9092,
    ):
        self.group = group
        self.topic = topic
        self.consumer_id = consumer_id
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self.assigned_partitions: list[int] = []

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()

    async def _request(self, opcode: OpCode, request) -> bytes:
        self._writer.write(encode_frame(opcode, request.encode()))
        await self._writer.drain()
        response_opcode, payload = await read_frame(self._reader)
        if response_opcode == OpCode.ERROR:
            raise RuntimeError(ErrorResponse.decode(payload).message)
        return payload

    async def join_group(self) -> list[int]:
        payload = await self._request(
            OpCode.JOIN_GROUP, JoinGroupRequest(self.group, self.consumer_id, self.topic)
        )
        response = JoinGroupResponse.decode(payload)
        self.assigned_partitions = response.partitions
        return response.partitions

    async def fetch(self, partition: int, offset: int, max_bytes: int = 1 << 20) -> FetchResponse:
        payload = await self._request(
            OpCode.FETCH, FetchRequest(self.topic, partition, offset, max_bytes)
        )
        return FetchResponse.decode(payload)

    async def commit(self, partition: int, offset: int) -> None:
        payload = await self._request(
            OpCode.COMMIT, CommitRequest(self.group, self.topic, partition, offset)
        )
        CommitResponse.decode(payload)
