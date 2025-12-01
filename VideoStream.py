class VideoStream:
    
    def __init__(self, filename):
        self.filename = filename
        try:
            self.file = open(filename, 'rb')
        except Exception as e:
            pass
        self.frameNum = 0

    def nexFrame(self):
        """Get next frame."""
        data = self.file.read(5) # Get the framelength from the first 5 bits
        if data:
            framelength = int(data)

            # Read the current frame
            data = self.file.read(framelength)
            self.frameNum += 1
        else:
            # If video is over
            self.restart_stream()
            return self.nexFrame()
        return data
    
    def restart_stream(self):
        """Video restarts when it's over"""
        self.file.seek(0,0)

    def frameNbr(self):
        """Get frame number."""
        return self.frameNum
	