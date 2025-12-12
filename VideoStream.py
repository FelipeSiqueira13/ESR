class VideoStream:
    
    def __init__(self, filename):
        self.filename = filename
        self.file = None
        try:
            self.file = open(filename, 'rb')
        except Exception as e:
            raise FileNotFoundError(f"Cannot open video file '{filename}': {e}")
        self.frameNum = 0

    def nextFrame(self):
        """Get next frame; reinicia ao fim do ficheiro."""
        if not self.file:
            return None

        start_pos = self.file.tell()
        header = self.file.read(5)
        
        if not header:
            self.restart_stream()
            return self.nextFrame()

        # Tenta detectar formato customizado (5 digitos ASCII)
        is_custom = False
        try:
            s_header = header.decode('ascii')
            if len(s_header) == 5 and s_header.isdigit():
                is_custom = True
                framelength = int(s_header)
        except:
            is_custom = False

        if is_custom:
            data = self.file.read(framelength)
            if not data or len(data) < framelength:
                self.restart_stream()
                return self.nextFrame()
            self.frameNum += 1
            return data
        
        # Fallback: MJPEG padrão (procura marcadores JPEG)
        self.file.seek(start_pos)
        
        # 1. Procura Start of Image (FF D8)
        while True:
            b = self.file.read(1)
            if not b:
                self.restart_stream()
                return self.nextFrame()
            if b == b'\xff':
                b2 = self.file.read(1)
                if b2 == b'\xd8':
                    frame_data = bytearray(b'\xff\xd8')
                    break
        
        # 2. Lê até End of Image (FF D9)
        while True:
            b = self.file.read(1)
            if not b:
                self.restart_stream()
                return self.nextFrame()
            frame_data.append(ord(b))
            if b == b'\xff':
                b2 = self.file.read(1)
                if not b2:
                    self.restart_stream()
                    return self.nextFrame()
                frame_data.append(ord(b2))
                if b2 == b'\xd9':
                    self.frameNum += 1
                    return bytes(frame_data)

    def get_frames(self, n=5):
        """Get next n frames."""
        frames = []
        for _ in range(n):
            data = self.nextFrame()
            if data:
                frames.append(data)
            else:
                break
        return frames

    def restart_stream(self):
        """Video restarts when it's over"""
        if self.file:
            self.file.seek(0, 0)

    def frameNbr(self):
        """Get frame number."""
        return self.frameNum
