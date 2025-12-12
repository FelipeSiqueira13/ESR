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

        while True:
            header = self.file.read(5)  # 5 bytes ASCII com o tamanho
            if not header or len(header) < 5:
                # EOF ou header incompleto: reinicia e tenta de novo
                self.restart_stream()
                continue

            try:
                framelength = int(header.decode('ascii'))
            except ValueError:
                # Header inválido: tenta próximo
                self.restart_stream()
                continue

            if framelength <= 0:
                self.restart_stream()
                continue

            data = self.file.read(framelength)
            if not data or len(data) < framelength:
                # Frame incompleta, reinicia
                self.restart_stream()
                continue

            self.frameNum += 1
            return data

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
