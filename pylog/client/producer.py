"""Thin async TCP client for PRODUCE. One persistent connection per
Producer instance — open it once, reuse it across calls."""

import asyncio

from pylog.broker.protocol import (
    ErrorResponse,
    OpCode,
    ProduceRequest,
    ProduceResponse,
    encode_frame,
    read_frame,
)


class Producer:
    def __init__(self, host: str = "127.0.0.1", port: int = 9092):
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()

    async def produce(self, topic: str, key: bytes | None, value: bytes) -> ProduceResponse:
        request = ProduceRequest(topic=topic, key=key, value=value)
        self._writer.write(encode_frame(OpCode.PRODUCE, request.encode()))
        await self._writer.drain()
        opcode, payload = await read_frame(self._reader)
        if opcode == OpCode.ERROR:
            raise RuntimeError(ErrorResponse.decode(payload).message)
        return ProduceResponse.decode(payload)
