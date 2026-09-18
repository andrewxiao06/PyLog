"""asyncio TCP server. One connection per client; requests on a
connection are handled sequentially (no pipelining), which is enough
for concurrent *producers* since each gets its own connection and the
event loop interleaves them."""

import asyncio
from pathlib import Path

from pylog.broker.protocol import (
    CommitRequest,
    CommitResponse,
    ErrorResponse,
    FetchRequest,
    FetchResponse,
    JoinGroupRequest,
    JoinGroupResponse,
    MetadataRequest,
    MetadataResponse,
    OpCode,
    ProduceRequest,
    ProduceResponse,
    RecordPayload,
    encode_frame,
    read_frame,
)
from pylog.broker.topic import Topic
from pylog.consumer.group import ConsumerGroup
from pylog.consumer.offsets import OffsetStore


class Broker:
    def __init__(self, data_dir: Path, default_partitions: int = 4):
        self.data_dir = Path(data_dir)
        self.default_partitions = default_partitions
        self.topics: dict[str, Topic] = {}
        self.offsets = OffsetStore(self.data_dir / "_offsets")
        self.groups: dict[tuple[str, str], ConsumerGroup] = {}

    def get_or_create_topic(self, name: str) -> Topic:
        if name not in self.topics:
            self.topics[name] = Topic(
                name, self.default_partitions, self.data_dir / "topics" / name
            )
        return self.topics[name]

    def get_or_create_group(self, group: str, topic: str, partition_count: int) -> ConsumerGroup:
        key = (group, topic)
        if key not in self.groups:
            self.groups[key] = ConsumerGroup(group, topic, partition_count)
        return self.groups[key]

    def dispatch(self, opcode: OpCode, payload: bytes) -> tuple[OpCode, bytes]:
        if opcode == OpCode.PRODUCE:
            request = ProduceRequest.decode(payload)
            topic = self.get_or_create_topic(request.topic)
            partition, offset = topic.append(request.key, request.value)
            return OpCode.PRODUCE_RESPONSE, ProduceResponse(partition, offset).encode()

        if opcode == OpCode.FETCH:
            request = FetchRequest.decode(payload)
            topic = self.get_or_create_topic(request.topic)
            records = topic.read_from(request.partition, request.offset, request.max_bytes)
            high_watermark = topic.partitions[request.partition].high_watermark
            payload_records = [
                RecordPayload(r.offset, r.timestamp, r.key, r.value) for r in records
            ]
            return OpCode.FETCH_RESPONSE, FetchResponse(payload_records, high_watermark).encode()

        if opcode == OpCode.COMMIT:
            request = CommitRequest.decode(payload)
            self.offsets.commit(request.group, request.topic, request.partition, request.offset)
            return OpCode.COMMIT_RESPONSE, CommitResponse(True).encode()

        if opcode == OpCode.JOIN_GROUP:
            request = JoinGroupRequest.decode(payload)
            topic = self.get_or_create_topic(request.topic)
            group = self.get_or_create_group(request.group, request.topic, topic.partition_count)
            partitions = group.join(request.consumer_id)
            return OpCode.JOIN_GROUP_RESPONSE, JoinGroupResponse(partitions).encode()

        if opcode == OpCode.METADATA:
            request = MetadataRequest.decode(payload)
            topic = self.get_or_create_topic(request.topic)
            return OpCode.METADATA_RESPONSE, MetadataResponse(topic.partition_count).encode()

        raise ValueError(f"unknown opcode: {opcode}")

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                try:
                    opcode, payload = await read_frame(reader)
                except asyncio.IncompleteReadError:
                    break  # client closed the connection
                try:
                    response_opcode, response_payload = self.dispatch(opcode, payload)
                except Exception as exc:  # noqa: BLE001 - report to client, don't crash the loop
                    response_opcode = OpCode.ERROR
                    response_payload = ErrorResponse(str(exc)).encode()
                writer.write(encode_frame(response_opcode, response_payload))
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


async def serve(host: str = "127.0.0.1", port: int = 9092, data_dir: Path = Path("./data")):
    broker = Broker(data_dir)
    server = await asyncio.start_server(broker.handle_connection, host, port)
    addr = server.sockets[0].getsockname()
    print(f"pylog broker listening on {addr}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(serve())
