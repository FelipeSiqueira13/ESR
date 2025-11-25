import datetime as dt
import pickle

class Message:
    STREAM_REQUEST = 1
    STREAM_STOP = 2
    STREAMS_AVAILABLE = 3
    RESP_WHAT_VIDEO = 4
    VIDEO_METRIC = 5
    ADD_NEIGHBOUR = 6
    REMOVE_NEIGHBOUR = 7
    RESP_NEIGHBOUR = 8

    def __init__(self, type,src,data:str=""):
        self.mytype = type    
        self.timestamp = dt.datetime.now()
        self.src = src
        self.data = data

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