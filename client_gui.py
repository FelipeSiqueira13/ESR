import io
import json
import os
import socket
import struct
import sys
import threading
from typing import Callable, List, Optional

try:
    import tkinter as tk
    from tkinter import ttk, messagebox
except ImportError:
    tk = None
    ttk = None
    messagebox = None

try:
    from PIL import Image, ImageTk
except ImportError:
    Image = None
    ImageTk = None

from msg import Message
from database import DataBase
from SimplePacket import SimplePacket

try:
    import client as client_core
except ImportError:
    client_core = None

SENDER_PORT = getattr(client_core, "SENDER_PORT", 40332)
RECEIVER_PORT = 40331


def _tk_gui_available() -> bool:
    if tk is None:
        return False
    platform = sys.platform
    if platform.startswith("win") or platform == "darwin":
        return True
    env_vars = ("DISPLAY", "WAYLAND_DISPLAY", "MIR_SOCKET", "XAUTHORITY")
    return any(os.environ.get(var) for var in env_vars)


def _send_buffer(sock: socket.socket, payload: bytes) -> None:
    view = memoryview(payload)
    total_sent = 0
    while total_sent < len(view):
        sent = sock.send(view[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection broken")
        total_sent += sent


def get_client_ip(client_name: str) -> Optional[str]:
    with open("config.json", "r") as file:
        ip_config = json.load(file)
    client_data = ip_config.get(client_name, {})
    if client_data:
        return list(client_data.keys())[0]
    return None


def send_tcp_request(node_host: str, node_port: int, msg: Message) -> Optional[Message]:
    try:
        with socket.create_connection((node_host, node_port), timeout=5) as s:
            _send_buffer(s, msg.serialize() + b"\n")
            buffer = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                buffer += chunk
                if b"\n" in buffer:
                    data, _ = buffer.split(b"\n", 1)
                    return Message.deserialize(data)
        return None
    except Exception as exc:
        print(f"[GUI][TCP] error: {exc}")
        return None


def get_node_info(client_name: str):
    db = DataBase(client_name)
    if db.vizinhos:
        return db.vizinhos[0], RECEIVER_PORT
    return None, None


def get_available_streams(node_host: str, node_port: int, client_name: str) -> List[str]:
    source = get_client_ip(client_name)
    if not source:
        return []
    msg = Message(Message.STREAMS_AVAILABLE, source, "")
    response = send_tcp_request(node_host, node_port, msg)
    if not response:
        return []
    streams_raw = response.data.strip()
    if not streams_raw or streams_raw == "No streams available":
        return []
    return [s.strip() for s in streams_raw.split(",") if s.strip()]


def request_stream(node_host: str, node_port: int, client_name: str, stream_id: str) -> str:
    source = get_client_ip(client_name)
    if not source:
        return "Client IP not found."
    msg = Message(Message.STREAM_REQUEST, source, stream_id)
    response = send_tcp_request(node_host, node_port, msg)
    if not response:
        return "No response from node."
    return response.data or ""


def send_stream_stop(node_host: str, node_port: int, client_name: str, stream_id: str) -> None:
    source = get_client_ip(client_name)
    if not source:
        return
    msg = Message(Message.STREAM_STOP, source, stream_id)
    send_tcp_request(node_host, node_port, msg)


class FrameBuffer:
    def __init__(self):
        self._frames = {}
        self._expected: Optional[int] = None
        self._lock = threading.Lock()

    def clear(self):
        with self._lock:
            self._frames.clear()
            self._expected = None

    def add_frames(self, base: int, frames: List[bytes]):
        with self._lock:
            for idx, frame in enumerate(frames):
                self._frames[base + idx] = frame
            if self._expected is None and self._frames:
                self._expected = min(self._frames)
            if len(self._frames) > 300:
                for key in sorted(self._frames)[:-300]:
                    self._frames.pop(key, None)

    def pop(self) -> Optional[bytes]:
        with self._lock:
            if not self._frames:
                return None
            if self._expected is None:
                self._expected = min(self._frames)
            if self._expected in self._frames:
                data = self._frames.pop(self._expected)
                self._expected += 1
                return data
            higher = [k for k in self._frames.keys() if k > (self._expected or 0)]
            if higher:
                next_key = min(higher)
                data = self._frames.pop(next_key)
                self._expected = next_key + 1
                return data
            return None


class UDPReceiver(threading.Thread):
    def __init__(self, frame_buffer: Optional[FrameBuffer], bits_sink: Callable[[str], None],
                 current_stream: Callable[[], Optional[str]], store_frames: bool = True):
        super().__init__(daemon=True)
        self._buffer = frame_buffer
        self._bits_sink = bits_sink
        self._current_stream = current_stream
        self._stop_event = threading.Event()
        self._sock: Optional[socket.socket] = None
        self._store_frames = store_frames

    def stop(self):
        self._stop_event.set()
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass

    def run(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("", SENDER_PORT))
        while not self._stop_event.is_set():
            try:
                raw, _ = self._sock.recvfrom(65535)
            except OSError:
                break
            except Exception as exc:
                print(f"[GUI][UDP] {exc}")
                continue
            try:
                pkt = SimplePacket.decode(raw)
            except Exception as exc:
                print(f"[GUI][UDP][DROP] decode: {exc}")
                continue
            payload = pkt.get_payload()
            if b"\0" not in payload:
                continue
            stream_id_b, batch_data = payload.split(b"\0", 1)
            try:
                stream_id = stream_id_b.decode("utf-8")
            except UnicodeDecodeError:
                continue
            if stream_id != self._current_stream():
                continue
            frames = []
            try:
                if len(batch_data) < 1:
                    raise ValueError("batch too small")
                count = struct.unpack("!B", batch_data[:1])[0]
                offset = 1
                for _ in range(count):
                    if offset + 4 > len(batch_data):
                        break
                    f_len = struct.unpack("!I", batch_data[offset:offset + 4])[0]
                    offset += 4
                    if offset + f_len > len(batch_data):
                        break
                    frame_b = batch_data[offset:offset + f_len]
                    offset += f_len
                    frames.append(frame_b)
                    preview_bits = " ".join(f"{byte:08b}" for byte in frame_b[:8])
                    self._bits_sink(f"frame={pkt.get_frame_num()} bits={preview_bits}...")
            except Exception as exc:
                print(f"[GUI][UDP][DROP] batch decode: {exc}")
                continue
            if frames and self._store_frames and self._buffer is not None:
                self._buffer.add_frames(pkt.get_frame_num(), frames)


def run_headless_cli(client_name: str) -> None:
    print("[CLIENT][GUI] Tk indisponível; executando modo texto.")
    node_host, node_port = get_node_info(client_name)
    if not node_host:
        print(f"[CLIENT][GUI] Sem node configurado para {client_name}.")
        return

    current_stream = {"value": None}
    receiver = UDPReceiver(
        frame_buffer=None,
        bits_sink=lambda line: print(f"[BITS] {line}"),
        current_stream=lambda: current_stream["value"],
        store_frames=False,
    )
    receiver.start()

    try:
        while True:
            streams = get_available_streams(node_host, node_port, client_name)
            if not streams:
                print("[CLIENT][GUI] Nenhuma stream disponível no momento.")
                break
            print("\nStreams disponíveis:")
            for sid in streams:
                print(f" - {sid}")
            choice = input("Selecione uma stream (ou 'quit' para sair): ").strip()
            if not choice:
                continue
            if choice.lower() in {"quit", "exit"}:
                break
            if choice not in streams:
                print("Stream inválida.")
                continue

            resp = request_stream(node_host, node_port, client_name, choice)
            if resp != "OK":
                print(f"Falha ao iniciar stream: {resp}")
                continue

            current_stream["value"] = choice
            print(f"Streaming {choice}. Digite 'stop' para parar ou 'quit' para sair.")
            while current_stream["value"]:
                try:
                    cmd = input("cmd> ").strip().lower()
                except KeyboardInterrupt:
                    cmd = "stop"
                if cmd in {"stop", "quit", "exit"}:
                    send_stream_stop(node_host, node_port, client_name, current_stream["value"])
                    print("Stream parada.")
                    current_stream["value"] = None
                    if cmd in {"quit", "exit"}:
                        return
                else:
                    print("Comandos válidos: stop, quit")
    except KeyboardInterrupt:
        pass
    finally:
        if current_stream["value"]:
            send_stream_stop(node_host, node_port, client_name, current_stream["value"])
        receiver.stop()


class ClientGUI:
    def __init__(self, root: tk.Tk, client_name: str):
        self.root = root
        self.root.title(f"Cliente GUI - {client_name}")
        self.client_name = client_name

        self.node_host, self.node_port = get_node_info(client_name)
        if not self.node_host:
            raise RuntimeError(f"No node info for {client_name}")

        self.frame_buffer = FrameBuffer()
        self.bits_queue = []
        self.bits_lock = threading.Lock()
        self.current_stream: Optional[str] = None
        self.photo = None

        self._build_layout()
        self._populate_streams()

        self.receiver = UDPReceiver(
            frame_buffer=self.frame_buffer,
            bits_sink=self._enqueue_bits,
            current_stream=lambda: self.current_stream,
        )
        self.receiver.start()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self._schedule_updates()

    def _build_layout(self):
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        controls = ttk.Frame(main)
        controls.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(controls, text="Streams:").pack(side=tk.LEFT)
        self.stream_var = tk.StringVar()
        self.stream_combo = ttk.Combobox(controls, textvariable=self.stream_var, state="readonly")
        self.stream_combo.pack(side=tk.LEFT, padx=6)

        ttk.Button(controls, text="Atualizar", command=self._populate_streams).pack(side=tk.LEFT)
        ttk.Button(controls, text="Iniciar", command=self.start_stream).pack(side=tk.LEFT, padx=4)
        ttk.Button(controls, text="Parar", command=self.stop_stream).pack(side=tk.LEFT)

        self.status_var = tk.StringVar(value="Selecione uma stream e clique em Iniciar.")
        ttk.Label(main, textvariable=self.status_var).pack(fill=tk.X, pady=(0, 10))

        self.video_label = ttk.Label(main)
        self.video_label.pack(fill=tk.BOTH, expand=True)

        bits_frame = ttk.LabelFrame(main, text="Bits recebidos (primeiros 8 bytes)")
        bits_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        self.bits_text = tk.Text(bits_frame, height=6, state=tk.DISABLED)
        self.bits_text.pack(fill=tk.BOTH, expand=True)

    def _populate_streams(self):
        streams = get_available_streams(self.node_host, self.node_port, self.client_name)
        if not streams:
            self.status_var.set("Sem streams disponíveis.")
        self.stream_combo["values"] = streams
        if streams:
            self.stream_combo.current(0)

    def _enqueue_bits(self, line: str):
        with self.bits_lock:
            self.bits_queue.append(line)

    def _flush_bits(self):
        with self.bits_lock:
            lines = list(self.bits_queue)
            self.bits_queue.clear()
        if not lines:
            return
        self.bits_text.configure(state=tk.NORMAL)
        for line in lines:
            self.bits_text.insert(tk.END, line + "\n")
        self.bits_text.see(tk.END)
        self.bits_text.configure(state=tk.DISABLED)

    def start_stream(self):
        stream_id = self.stream_var.get()
        if not stream_id:
            messagebox.showwarning("Stream", "Selecione uma stream.")
            return
        resp = request_stream(self.node_host, self.node_port, self.client_name, stream_id)
        if resp != "OK":
            messagebox.showerror("Stream", f"Falha ao iniciar: {resp}")
            return
        self.current_stream = stream_id
        self.frame_buffer.clear()
        self.status_var.set(f"Streaming {stream_id}...")

    def stop_stream(self):
        if not self.current_stream:
            return
        send_stream_stop(self.node_host, self.node_port, self.client_name, self.current_stream)
        self.status_var.set("Stream parada.")
        self.current_stream = None
        self.frame_buffer.clear()

    def _schedule_updates(self):
        self.root.after(40, self._update_frame)
        self.root.after(120, self._update_bits)

    def _update_frame(self):
        frame = self.frame_buffer.pop()
        if frame and Image is not None and ImageTk is not None:
            try:
                img = Image.open(io.BytesIO(frame))
                img = img.resize((480, 320))
                self.photo = ImageTk.PhotoImage(img)
                self.video_label.configure(image=self.photo)
            except Exception as exc:
                self.status_var.set(f"Erro exibindo frame: {exc}")
        self.root.after(40, self._update_frame)

    def _update_bits(self):
        self._flush_bits()
        self.root.after(120, self._update_bits)

    def on_close(self):
        self.stop_stream()
        self.receiver.stop()
        self.root.destroy()


def main():
    if len(sys.argv) < 2:
        print("Usage: python client_gui.py <clientName>")
        sys.exit(1)
    client_name = sys.argv[1]
    if _tk_gui_available():
        root = tk.Tk()
        try:
            ClientGUI(root, client_name)
            root.mainloop()
        except Exception as exc:
            if messagebox:
                messagebox.showerror("Erro", str(exc))
            else:
                print(f"Erro: {exc}")
            root.destroy()
    else:
        run_headless_cli(client_name)


if __name__ == "__main__":
    main()
