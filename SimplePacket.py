import struct
from typing import ClassVar


class SimplePacket:
    _HEADER_FORMAT: ClassVar[str] = "!IIdI"
    _HEADER_SIZE: ClassVar[int] = struct.calcsize(_HEADER_FORMAT)

    def __init__(self, stream_id: int, frame_num: int, timestamp: float, payload: bytes):
        self._stream_id = stream_id
        self._frame_num = frame_num
        self._timestamp = timestamp
        self._payload = payload or b""

    @classmethod
    def encode(cls, stream_id: int, frame_num: int, timestamp: float, frame_data: bytes) -> bytes:
        payload = frame_data or b""
        header = struct.pack(cls._HEADER_FORMAT, stream_id, frame_num, timestamp, len(payload))
        return header + payload

    @classmethod
    def decode(cls, packet_bytes: bytes) -> "SimplePacket":
        if len(packet_bytes) < cls._HEADER_SIZE:
            raise ValueError("Packet too short to contain header")
        header = packet_bytes[:cls._HEADER_SIZE]
        stream_id, frame_num, timestamp, payload_len = struct.unpack(cls._HEADER_FORMAT, header)

        expected_size = cls._HEADER_SIZE + payload_len
        if len(packet_bytes) != expected_size:
            raise ValueError("Packet size mismatch")

        payload = packet_bytes[cls._HEADER_SIZE:expected_size]
        return cls(stream_id, frame_num, timestamp, payload)

    def get_stream_id(self) -> int:
        return self._stream_id

    def get_frame_num(self) -> int:
        return self._frame_num

    def get_timestamp(self) -> float:
        return self._timestamp

    def get_payload(self) -> bytes:
        return self._payload