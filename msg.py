import datetime as dt

class Message:
    STREAM_PLS = 1
    VIDEO_NO = 2
    WHAT_VIDEO = 3
    VIDEO_METRIC = 4
    ADD_NEIGHBOUR = 5
    REMOVE_NEIGHBOUR = 6
    RESP_NEIGHBOUR = 7

    def __init__(self, type,src,data:str=""):
        self.mytype = type    
        self.timestamp = dt.datetime.now()
        self.src = src

    def getType(self):
        return self.mytype

    def getTimestamp(self):
        return self.timestamp
    
    def getStream(self):
        return self.stream
    
    def getSrc(self):
        return self.src
    
    def setSrc(self,newSrc):
        self.src = newSrc

    
    def serialize(self) -> bytes:
        return f"{self.mytype}|{self.timestamp.isoformat()}|{self.src}|{self.data}".encode('utf-8')
    
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