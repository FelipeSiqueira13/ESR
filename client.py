import socket
import sys, json
from msg import Message
from database import DataBase
from onode import send_message
import threading, time
from SimplePacket import SimplePacket

try:
    import tkinter as tk
    from PIL import Image, ImageTk
    import io
    HAS_GUI = True
except ImportError:
    HAS_GUI = False
    print("Warning: PIL or tkinter not found. Video playback disabled.")

SENDER_PORT = 40332  # porta de receção UDP de dados (MM)

_frame_buffer = {}
_frame_lock = threading.Lock()
_running = True
current_stream = None  # Global variable for the active stream
_stats = {
    "stream_id": "None",
    "fps": 0,
    "last_frame": 0
}

def get_node_info(clientName):
    db = DataBase(clientName)
    if db.vizinhos:
        node_host = db.vizinhos[0]
        node_port = 40331 # Porta Receiver dos Routers
        return node_host, node_port
    else:
        print("No neighbors found in the database.")
        return None, None
    
def get_client_ip(clientName):
    with open('config.json', 'r') as file:
        ip_config = json.load(file)
    
    client_data = ip_config.get(clientName, {})
    
    if client_data:
        return list(client_data.keys())[0]
    else:
        return None


def _send_buffer(sock: socket.socket, payload: bytes):
    view = memoryview(payload)
    total_sent = 0
    while total_sent < len(view):
        sent = sock.send(view[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection broken")
        total_sent += sent

def send_tcp_request(node_host, node_port, msg: Message):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)

        s.connect((node_host, node_port))

        _send_buffer(s, msg.serialize() + b'\n')

        buffer = b''
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            buffer += chunk

            if b'\n' in buffer:
                data, _ = buffer.split(b'\n', 1)
                s.close()
                return Message.deserialize(data)
            
        s.close()
        print("Connection closed without receiving complete response")
        return None
    
    except socket.timeout:
        print("Timeout waiting for response from node.")
        return None
    except Exception as e:
        print(f"Error in TCP request: {e}")
        return None


def get_available_streams(node_host, node_port, clientName):
    try:
        source = get_client_ip(clientName)
        if not source:
            print("Client IP not found in config.")
            return []

        msg = Message(Message.STREAMS_AVAILABLE, source, "")
        response_message = send_tcp_request(node_host, node_port, msg)
        if not response_message:
            return []

        streams_raw = response_message.data.strip()
        if not streams_raw or streams_raw == "No streams available":
            return []
        return [stream.strip() for stream in streams_raw.split(",") if stream.strip()]
    except Exception as e:
        print("Error retrieving streams:", e)
        return []


def requestStream(node_host, node_port, client_name, stream_number):
    try:
        source = get_client_ip(client_name)
        if not source:
            return "Client IP not found."

        msg = Message(Message.STREAM_REQUEST, source, stream_number)
        response_message = send_tcp_request(node_host, node_port, msg)
        if not response_message:
            return "No response from node."
        return response_message.data
    except Exception as e:
        print("Error requesting stream:", e)
        return "Request failed."

def udp_listener():
    """Escuta frames via UDP (SimplePacket) e armazena por frame_num."""
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.bind(('', SENDER_PORT))
    print(f"[CLIENT][UDP] listening on 0.0.0.0:{SENDER_PORT}")

    last_log_time = time.time()
    frames_in_interval = 0

    while _running:
        try:
            raw, addr = udp_sock.recvfrom(65535)
            try:
                pkt = SimplePacket.decode(raw)
            except Exception as e:
                # print(f"[CLIENT][UDP][DROP] decode: {e}")
                continue

            payload = pkt.get_payload()
            if b'\0' not in payload:
                # print("[CLIENT][UDP][DROP] malformed payload (no NUL)")
                continue
            stream_id_b, frame_b = payload.split(b'\0', 1)
            try:
                stream_id = stream_id_b.decode('utf-8')
            except Exception:
                continue

            frame_num = pkt.get_frame_num()
            with _frame_lock:
                buf = _frame_buffer.setdefault(stream_id, {})
                buf[frame_num] = frame_b
                # Limita buffer a 200 frames
                if len(buf) > 200:
                    oldest = min(buf.keys())
                    buf.pop(oldest, None)

            # Atualiza estatísticas globais a cada 1.0 segundo
            frames_in_interval += 1
            now = time.time()
            if now - last_log_time > 1.0:
                _stats["stream_id"] = stream_id
                _stats["fps"] = frames_in_interval
                _stats["last_frame"] = frame_num
                
                frames_in_interval = 0
                last_log_time = now

        except OSError:
            break
        except Exception as e:
            print(f"[CLIENT][UDP][ERR] {e}")
    udp_sock.close()

def send_stream_stop(node_host, node_port, client_name, stream_number):
    try:
        source = get_client_ip(client_name)
        if not source:
            return
        msg = Message(Message.STREAM_STOP, source, stream_number)
        send_tcp_request(node_host, node_port, msg)
        print(f"[CLIENT] STREAM_STOP sent for {stream_number}")
    except Exception as e:
        print(f"[CLIENT] STREAM_STOP error: {e}")

def send_ping(node_host, node_port, client_name, stream_id, ttl=8):
    source = get_client_ip(client_name)
    if not source:
        return "Client IP not found."
    payload = json.dumps({"stream_id": stream_id, "ttl": ttl, "path": []})
    msg = Message(Message.PING, source, payload)
    send_tcp_request(node_host, node_port, msg)
    return "PING sent."

class VideoPlayer:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("ESR Video Player")
        self.label = tk.Label(self.root, text="Waiting for stream...")
        self.label.pack()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.current_frame_num = -1
        self.update_loop()
    
    def update_loop(self):
        global current_stream, _frame_buffer, _frame_lock, _running
        
        if not _running:
            self.root.destroy()
            return

        if current_stream:
            with _frame_lock:
                if current_stream in _frame_buffer:
                    buf = _frame_buffer[current_stream]
                    if buf:
                        # Simple strategy: get the latest frame available
                        # This ensures low latency for live streaming
                        latest_frame_num = max(buf.keys())
                        
                        if latest_frame_num > self.current_frame_num:
                            frame_data = buf[latest_frame_num]
                            self.current_frame_num = latest_frame_num
                            
                            try:
                                image = Image.open(io.BytesIO(frame_data))
                                photo = ImageTk.PhotoImage(image)
                                self.label.configure(image=photo, text="")
                                self.label.image = photo
                            except Exception as e:
                                print(f"Error decoding frame: {e}")
                            
                            # Cleanup old frames to save memory
                            # Keep only the last 20 frames
                            to_remove = [k for k in buf.keys() if k < latest_frame_num - 20]
                            for k in to_remove:
                                del buf[k]
        
        self.root.after(30, self.update_loop) # ~33 FPS check

    def start(self):
        self.root.mainloop()
        
    def on_close(self):
        global _running
        _running = False
        self.root.destroy()
        # Force exit to kill console thread
        import os
        os._exit(0)

def main():
    global current_stream, _running
    if len(sys.argv) < 2:
        print("Usage: python3 client.py <clientName>")
        sys.exit(1)

    clientName = sys.argv[1]
    print(f"Client {clientName} initialized")

    node_host, node_port = get_node_info(clientName)
    if not node_host:
        print(f"No node information found for client {clientName}")
        sys.exit(1)

    # Inicia listener UDP de frames
    t_udp = threading.Thread(target=udp_listener, daemon=True)
    t_udp.start()

    streams = get_available_streams(node_host, node_port, clientName)
    if not streams:
        print("No streams received from node.")
        # return # Don't return, allow running to see logs or wait

    print("\nAvailable streams:")
    for stream in streams:
        print(f"Stream {stream}")

    def console_loop():
        global current_stream, _running
        try:
            while _running:
                stream_choice = input("Select stream, 'monitor' for stats, 'quit' to exit, 'ping <stream>': ")
                if stream_choice.lower() == 'quit':
                    _running = False
                    break
                
                if stream_choice.lower() == 'monitor':
                    print("Monitoring stats... Press Ctrl+C to stop.")
                    try:
                        while _running:
                            print(f"Stream: {_stats['stream_id']} | FPS: {_stats['fps']} | LastFrame: {_stats['last_frame']}")
                            time.sleep(1)
                    except KeyboardInterrupt:
                        print("\nStopped monitoring.")
                    continue

                if stream_choice.lower().startswith("ping "):
                    sid = stream_choice.split(None, 1)[1].strip()
                    print(send_ping(node_host, node_port, clientName, sid))
                    continue
                
                print(f"Requesting stream: {stream_choice}")
                response = requestStream(node_host, node_port, clientName, stream_choice)
                print("Response from node:", response)
                if response == "OK":
                    current_stream = stream_choice
        except EOFError:
            pass
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Console error: {e}")
        finally:
            _running = False
            # Envia STREAM_STOP se havia uma stream selecionada
            if current_stream:
                send_stream_stop(node_host, node_port, clientName, current_stream)
            # dá tempo para fechar o UDP
            time.sleep(0.5)
            if not HAS_GUI:
                sys.exit(0)

    if HAS_GUI:
        print("Starting GUI... Console commands are still active.")
        t_console = threading.Thread(target=console_loop, daemon=True)
        t_console.start()
        
        player = VideoPlayer()
        player.start()
    else:
        console_loop()

if __name__ == '__main__':
    main()


