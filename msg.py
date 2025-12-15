import datetime as dt
import json
from typing import Optional

class Message:
    STREAM_REQUEST = 1
    STREAM_STOP = 2
    STREAMS_AVAILABLE = 3
    RESP_STREAMS_AVAILABLE = 4
    VIDEO_METRIC_REQUEST = 5
    VIDEO_METRIC_RESPONSE = 6
    VIDEO_METRIC_UPDATE = 7
    ADD_NEIGHBOUR = 8
    REMOVE_NEIGHBOUR = 9
    RESP_NEIGHBOUR = 10
    MM = 11
    PING = 12

    def __init__(self, type,src,data:str=""):
        self.mytype = type    
        self.timestamp = dt.datetime.now()
        self.src = src
        self.data = data

    def stringfy(self, streams_id:list, temp:float):
        return ",".join(streams_id) + f";{temp}"
    
    def parseStringfy(self, data_str: str):
        streams_part, temp_part = data_str.split(";")
        streams_id = streams_part.split(",") if streams_part else []
        temp = float(temp_part)
        return streams_id, temp

    def getType(self):
        return self.mytype

    def getTimestamp(self):
        return self.timestamp
    
    def getData(self):
        return self.data
    
    def getSrc(self):
        return self.src
    
    def setSrc(self,newSrc):
        self.src = newSrc

    def encode(self) -> bytes:
        return self.serialize()

    def metrics_encode(self, streams_id: list, request_id: Optional[str] = None, accumulated_delay_ms: float = 0) -> bytes:
        payload = {
            "request_id": request_id or f"req-{int(dt.datetime.now().timestamp()*1000)}",
            "streams": streams_id,
            "accumulated_delay_ms": accumulated_delay_ms
        }
        self.data = json.dumps(payload)
        return self.serialize()
    
    def metrics_decode(self):
        payload = json.loads(self.data or "{}")
        payload.setdefault("streams", [])
        return payload

    @staticmethod
    def decode(data: bytes):
        return Message.deserialize(data)
    
    def serialize(self) -> bytes:
        return f"{self.mytype}|{self.timestamp.isoformat()}|{self.src}|{self.data}".encode('utf-8')

    @staticmethod
    def deserialize(data:bytes):
        decoded = data.decode('utf-8')
        parts = decoded.split('|', 3)
        msg_type = int(parts[0])
        timestamp = dt.datetime.fromisoformat(parts[1])
        src = parts[2]
        data_str = parts[3] if len(parts) > 3 else ""
        msg = Message(msg_type, src, data_str)
        msg.timestamp = timestamp
        return msg