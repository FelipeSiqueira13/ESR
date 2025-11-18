import datetime as dt


class Message:
    def __init__(self, type,stream,src):
        self.mytype = type    
        self.timestamp = dt.datetime.now()
        self.stream = stream
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

    