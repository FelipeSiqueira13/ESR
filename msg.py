import datetime as dt

class Message:
    STREAM_PLS = 1
    VIDEO_NO = 2
    WHAT_VIDEO = 3
    VIDEO_METRIC = 4
    ADD_NEIGHBOUR = 5
    REMOVE_NEIGHBOUR = 6
    RESP_NEIGHBOUR = 7
    REQUEST_STREAM = 8

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

    
